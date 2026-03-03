/// End-to-end sync/merge integration tests against a live S3 bucket.
///
/// Each test exercises one 3-way merge scenario through the full pipeline:
///   scan_local_dir → list_all_objects → generate_sync_plan → execute → SyncDb update
///
/// Mirrors the unit tests in src/core/merger.rs, but against real S3.
///
/// Run with real credentials after sourcing .env:
///   source .env && cargo test --test s3_sync -- --ignored --nocapture
use cobblestone::aws::s3_client::S3Client;
use cobblestone::core::merger::generate_sync_plan;
use cobblestone::core::models::SyncAction;
use cobblestone::core::scanner::scan_local_dir;
use cobblestone::db::sync_db::SyncDb;

// ── Helpers ──────────────────────────────────────────────────────────────────

fn test_prefix(label: &str) -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("cobblestone-test/{}/{}", ts, label)
}

async fn make_client(prefix: &str) -> S3Client {
    let bucket =
        std::env::var("S3_BUCKET_NAME").expect("S3_BUCKET_NAME must be set (source .env first)");
    S3Client::new(&bucket, prefix)
        .await
        .expect("failed to create S3Client")
}

/// Returns (SyncDb, NamedTempFile). Keep the NamedTempFile alive for the test duration.
fn make_db() -> (SyncDb, tempfile::NamedTempFile) {
    let tmp = tempfile::Builder::new()
        .suffix(".db")
        .tempfile()
        .expect("failed to create temp db file");
    let db = SyncDb::new(tmp.path()).expect("failed to create SyncDb");
    (db, tmp)
}

/// Writes `content` to `dir/key`, creating parent directories as needed.
fn write_local(dir: &std::path::Path, key: &str, content: &[u8]) {
    let path = dir.join(key);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, content).unwrap();
}

/// Gathers the three states and returns the sync plan without executing it.
async fn gather_and_plan(
    s3: &S3Client,
    local_dir: &std::path::Path,
    db: &SyncDb,
) -> Vec<SyncAction> {
    let local = scan_local_dir(local_dir).expect("scan_local_dir failed");
    let remote = s3
        .list_all_objects()
        .await
        .expect("list_all_objects failed");
    let base = db.get_all_files().unwrap_or_default();
    generate_sync_plan(local, remote, base)
}

/// Executes Upload/Download/DeleteLocal/DeleteRemote actions; skips Conflict/SkipConflict.
async fn execute_plan(
    plan: Vec<SyncAction>,
    s3: &S3Client,
    local_dir: &std::path::Path,
    db: &mut SyncDb,
) {
    for action in plan {
        match action {
            SyncAction::Upload(meta) => {
                let path = local_dir.join(&meta.key);
                s3.upload_object(&meta.key, &path, &meta.checksum)
                    .await
                    .expect("upload_object failed");
                db.upsert_file(&meta).expect("upsert_file failed");
            }
            SyncAction::Download(meta) => {
                let path = local_dir.join(&meta.key);
                s3.download_object(&meta.key, &path)
                    .await
                    .expect("download_object failed");
                db.upsert_file(&meta).expect("upsert_file failed");
            }
            SyncAction::DeleteLocal(key) => {
                let path = local_dir.join(&key);
                if path.exists() {
                    std::fs::remove_file(&path).expect("remove_file (local) failed");
                }
                db.remove_file(&key).expect("remove_file (db) failed");
            }
            SyncAction::DeleteRemote(key) => {
                s3.delete_object(&key).await.expect("delete_object failed");
                db.remove_file(&key).expect("remove_file (db) failed");
            }
            SyncAction::Conflict { .. } | SyncAction::SkipConflict(_) | SyncAction::UpToDate(_) => {
            }
        }
    }
}

/// Deletes all objects under the client's prefix. Call at end of each test.
async fn cleanup(s3: &S3Client) {
    let objects = s3.list_all_objects().await.unwrap_or_default();
    for obj in objects {
        s3.delete_object(&obj.key).await.ok();
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Local has a new file, S3 is empty, base is empty → Upload.
#[tokio::test]
#[ignore = "requires live AWS credentials (source .env first)"]
async fn s3_sync_local_new_file_uploads() {
    let local_dir = tempfile::tempdir().unwrap();
    let client = make_client(&test_prefix("local-new")).await;
    let (mut db, _tmp_db) = make_db();

    write_local(local_dir.path(), "a.txt", b"hello from local");

    let plan = gather_and_plan(&client, local_dir.path(), &db).await;
    assert_eq!(plan.len(), 1, "expected exactly 1 action, got: {:?}", plan);
    assert!(
        matches!(&plan[0], SyncAction::Upload(m) if m.key == "a.txt"),
        "expected Upload(a.txt), got: {:?}",
        plan[0]
    );

    execute_plan(plan, &client, local_dir.path(), &mut db).await;

    let objects = client.list_all_objects().await.unwrap();
    assert!(
        objects.iter().any(|o| o.key == "a.txt"),
        "a.txt not found in S3 after upload"
    );

    cleanup(&client).await;
}

/// S3 has a new file, local is empty, base is empty → Download.
#[tokio::test]
#[ignore = "requires live AWS credentials (source .env first)"]
async fn s3_sync_remote_new_file_downloads() {
    let local_dir = tempfile::tempdir().unwrap();
    let client = make_client(&test_prefix("remote-new")).await;
    let (mut db, _tmp_db) = make_db();

    // Pre-populate S3
    let src = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(src.path(), b"hello from remote").unwrap();
    client.upload_object("b.txt", src.path(), "").await.unwrap();

    let plan = gather_and_plan(&client, local_dir.path(), &db).await;
    assert!(
        plan.iter()
            .any(|a| matches!(a, SyncAction::Download(m) if m.key == "b.txt")),
        "expected Download(b.txt), got: {:?}",
        plan
    );

    execute_plan(plan, &client, local_dir.path(), &mut db).await;

    let content = std::fs::read(local_dir.path().join("b.txt")).unwrap();
    assert_eq!(content, b"hello from remote", "downloaded content mismatch");

    cleanup(&client).await;
}

/// File is identical in local, S3, and base → UpToDate (no upload/download).
/// Uses two sync rounds: first to establish base, second to verify no churn.
#[tokio::test]
#[ignore = "requires live AWS credentials (source .env first)"]
async fn s3_sync_unchanged_is_uptodate() {
    let local_dir = tempfile::tempdir().unwrap();
    let client = make_client(&test_prefix("unchanged")).await;
    let (mut db, _tmp_db) = make_db();

    write_local(local_dir.path(), "c.txt", b"stable content");

    // Sync 1: uploads c.txt, records in db
    let plan1 = gather_and_plan(&client, local_dir.path(), &db).await;
    execute_plan(plan1, &client, local_dir.path(), &mut db).await;

    // Sync 2: nothing changed — all should be UpToDate
    let plan2 = gather_and_plan(&client, local_dir.path(), &db).await;
    assert!(
        plan2.iter().all(|a| matches!(a, SyncAction::UpToDate(_))),
        "expected all UpToDate on second sync, got: {:?}",
        plan2
    );

    cleanup(&client).await;
}

/// Local file is modified since last sync; remote is unchanged → Upload.
#[tokio::test]
#[ignore = "requires live AWS credentials (source .env first)"]
async fn s3_sync_local_modification_uploads() {
    let local_dir = tempfile::tempdir().unwrap();
    let client = make_client(&test_prefix("local-mod")).await;
    let (mut db, _tmp_db) = make_db();

    // Sync 1: establish base with v1
    write_local(local_dir.path(), "d.txt", b"version 1");
    let plan1 = gather_and_plan(&client, local_dir.path(), &db).await;
    execute_plan(plan1, &client, local_dir.path(), &mut db).await;

    // Modify local to v2
    write_local(local_dir.path(), "d.txt", b"version 2");

    let plan2 = gather_and_plan(&client, local_dir.path(), &db).await;
    assert!(
        plan2
            .iter()
            .any(|a| matches!(a, SyncAction::Upload(m) if m.key == "d.txt")),
        "expected Upload(d.txt), got: {:?}",
        plan2
    );

    execute_plan(plan2, &client, local_dir.path(), &mut db).await;

    // Verify S3 now has v2
    let dest_dir = tempfile::tempdir().unwrap();
    let dest = dest_dir.path().join("d.txt");
    client.download_object("d.txt", &dest).await.unwrap();
    assert_eq!(
        std::fs::read(dest).unwrap(),
        b"version 2",
        "S3 should have v2 content"
    );

    cleanup(&client).await;
}

/// Remote file is modified since last sync; local is unchanged → Download.
#[tokio::test]
#[ignore = "requires live AWS credentials (source .env first)"]
async fn s3_sync_remote_modification_downloads() {
    let local_dir = tempfile::tempdir().unwrap();
    let client = make_client(&test_prefix("remote-mod")).await;
    let (mut db, _tmp_db) = make_db();

    // Sync 1: establish base with v1
    write_local(local_dir.path(), "e.txt", b"version 1");
    let plan1 = gather_and_plan(&client, local_dir.path(), &db).await;
    execute_plan(plan1, &client, local_dir.path(), &mut db).await;

    // Modify S3 to v2 (simulate external upload)
    let tmp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(tmp.path(), b"version 2").unwrap();
    client.upload_object("e.txt", tmp.path(), "").await.unwrap();

    let plan2 = gather_and_plan(&client, local_dir.path(), &db).await;
    assert!(
        plan2
            .iter()
            .any(|a| matches!(a, SyncAction::Download(m) if m.key == "e.txt")),
        "expected Download(e.txt), got: {:?}",
        plan2
    );

    execute_plan(plan2, &client, local_dir.path(), &mut db).await;

    assert_eq!(
        std::fs::read(local_dir.path().join("e.txt")).unwrap(),
        b"version 2",
        "local should have v2 content after download"
    );

    cleanup(&client).await;
}

/// Local file is deleted since last sync; remote still at base → DeleteRemote.
#[tokio::test]
#[ignore = "requires live AWS credentials (source .env first)"]
async fn s3_sync_local_deletion_deletes_remote() {
    let local_dir = tempfile::tempdir().unwrap();
    let client = make_client(&test_prefix("local-del")).await;
    let (mut db, _tmp_db) = make_db();

    // Sync 1: establish base
    write_local(local_dir.path(), "f.txt", b"will be deleted locally");
    let plan1 = gather_and_plan(&client, local_dir.path(), &db).await;
    execute_plan(plan1, &client, local_dir.path(), &mut db).await;

    // Delete f.txt locally
    std::fs::remove_file(local_dir.path().join("f.txt")).unwrap();

    let plan2 = gather_and_plan(&client, local_dir.path(), &db).await;
    assert!(
        plan2
            .iter()
            .any(|a| matches!(a, SyncAction::DeleteRemote(k) if k == "f.txt")),
        "expected DeleteRemote(f.txt), got: {:?}",
        plan2
    );

    execute_plan(plan2, &client, local_dir.path(), &mut db).await;

    let objects = client.list_all_objects().await.unwrap();
    assert!(
        !objects.iter().any(|o| o.key == "f.txt"),
        "f.txt should be gone from S3"
    );
}

/// Remote file is deleted since last sync; local still at base → DeleteLocal.
#[tokio::test]
#[ignore = "requires live AWS credentials (source .env first)"]
async fn s3_sync_remote_deletion_deletes_local() {
    let local_dir = tempfile::tempdir().unwrap();
    let client = make_client(&test_prefix("remote-del")).await;
    let (mut db, _tmp_db) = make_db();

    // Sync 1: establish base
    write_local(local_dir.path(), "g.txt", b"will be deleted remotely");
    let plan1 = gather_and_plan(&client, local_dir.path(), &db).await;
    execute_plan(plan1, &client, local_dir.path(), &mut db).await;

    // Delete g.txt from S3
    client.delete_object("g.txt").await.unwrap();

    let plan2 = gather_and_plan(&client, local_dir.path(), &db).await;
    assert!(
        plan2
            .iter()
            .any(|a| matches!(a, SyncAction::DeleteLocal(k) if k == "g.txt")),
        "expected DeleteLocal(g.txt), got: {:?}",
        plan2
    );

    execute_plan(plan2, &client, local_dir.path(), &mut db).await;

    assert!(
        !local_dir.path().join("g.txt").exists(),
        "g.txt should be deleted locally"
    );
}

/// Both sides independently modified to the same content → UpToDate (convergent).
#[tokio::test]
#[ignore = "requires live AWS credentials (source .env first)"]
async fn s3_sync_both_modified_same_content_uptodate() {
    let local_dir = tempfile::tempdir().unwrap();
    let client = make_client(&test_prefix("both-mod-same")).await;
    let (mut db, _tmp_db) = make_db();

    // Sync 1: establish base with v1
    write_local(local_dir.path(), "h.txt", b"version 1");
    let plan1 = gather_and_plan(&client, local_dir.path(), &db).await;
    execute_plan(plan1, &client, local_dir.path(), &mut db).await;

    // Both sides update to the identical v2 content
    write_local(local_dir.path(), "h.txt", b"version 2 - same everywhere");
    let tmp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(tmp.path(), b"version 2 - same everywhere").unwrap();
    client.upload_object("h.txt", tmp.path(), "").await.unwrap();

    let plan2 = gather_and_plan(&client, local_dir.path(), &db).await;
    assert!(
        plan2.iter().all(|a| matches!(a, SyncAction::UpToDate(_))),
        "expected UpToDate when both sides converge to same content, got: {:?}",
        plan2
    );

    cleanup(&client).await;
}

/// Both sides modified to different content → Conflict (no auto-resolution).
///
/// Prints a summary report showing local/remote content, base state, and conflict details.
#[tokio::test]
#[ignore = "requires live AWS credentials (source .env first)"]
async fn s3_sync_both_modified_conflict() {
    let local_dir = tempfile::tempdir().unwrap();
    let client = make_client(&test_prefix("both-mod-conflict")).await;
    let (mut db, _tmp_db) = make_db();

    // Sync 1: establish base with v1
    let base_content = b"version 1 (shared base)";
    write_local(local_dir.path(), "i.txt", base_content);
    let plan1 = gather_and_plan(&client, local_dir.path(), &db).await;
    execute_plan(plan1, &client, local_dir.path(), &mut db).await;

    // Record base state before divergence
    let base_state = db.get_all_files().unwrap_or_default();
    let base_meta = base_state.iter().find(|m| m.key == "i.txt").unwrap();

    // Local → content-A, S3 → content-B (genuine conflict)
    let local_content = b"local diverged content";
    let remote_content = b"remote diverged content";

    write_local(local_dir.path(), "i.txt", local_content);
    let tmp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(tmp.path(), remote_content).unwrap();
    client.upload_object("i.txt", tmp.path(), "").await.unwrap();

    // Only plan, do not execute (conflict requires interactive resolution)
    let plan2 = gather_and_plan(&client, local_dir.path(), &db).await;

    // Extract conflict details for the report
    let conflict = plan2.iter().find_map(|a| {
        if let SyncAction::Conflict { key, local, remote } = a {
            if key == "i.txt" { Some((key.clone(), local.clone(), remote.clone())) } else { None }
        } else {
            None
        }
    });

    // ── Conflict Summary Report ───────────────────────────────────────────────
    println!();
    println!("╔═══════════════════════════════════════════════════════╗");
    println!("║              CONFLICT SUMMARY REPORT                 ║");
    println!("╠═══════════════════════════════════════════════════════╣");
    println!("║ File : i.txt                                         ║");
    println!("╠═══════════════════════════════════════════════════════╣");
    println!("║ BASE STATE (last known good)                         ║");
    println!("║   content  : {:?}", std::str::from_utf8(base_content).unwrap());
    println!("║   checksum : {}", base_meta.checksum);
    println!("║   size     : {} bytes", base_meta.size);
    println!("║   modified : {} (unix s)", base_meta.modified_ts);
    println!("╠═══════════════════════════════════════════════════════╣");
    println!("║ LOCAL (diverged from base)                           ║");
    println!("║   content  : {:?}", std::str::from_utf8(local_content).unwrap());
    if let Some((_, ref local_meta, _)) = conflict {
        println!("║   checksum : {}", local_meta.checksum);
        println!("║   size     : {} bytes", local_meta.size);
        println!("║   modified : {} (unix s)", local_meta.modified_ts);
    }
    println!("╠═══════════════════════════════════════════════════════╣");
    println!("║ REMOTE (diverged from base)                          ║");
    println!("║   content  : {:?}", std::str::from_utf8(remote_content).unwrap());
    if let Some((_, _, ref remote_meta)) = conflict {
        println!("║   checksum : {}", remote_meta.checksum);
        println!("║   size     : {} bytes", remote_meta.size);
        println!("║   modified : {} (unix s)", remote_meta.modified_ts);
    }
    println!("╠═══════════════════════════════════════════════════════╣");
    println!("║ BEHAVIOR                                             ║");
    println!("║   local change  : Modified (checksum differs from base)");
    println!("║   remote change : Modified (checksum differs from base)");
    println!("║   checksums match: {}", conflict.as_ref().map_or(false, |(_, l, r)| l.checksum == r.checksum));
    println!("╠═══════════════════════════════════════════════════════╣");
    println!("║ RESULT                                               ║");
    println!("║   action : {:?}", plan2.iter().find(|a| matches!(a, SyncAction::Conflict { .. })).unwrap());
    println!("║   resolution : requires interactive user input        ║");
    println!("║     [L]ocal  — upload local version to S3            ║");
    println!("║     [R]emote — download remote version locally        ║");
    println!("║     [S]kip   — defer resolution to next sync         ║");
    println!("╚═══════════════════════════════════════════════════════╝");
    println!();
    // ─────────────────────────────────────────────────────────────────────────

    assert!(
        conflict.is_some(),
        "expected Conflict for i.txt, got: {:?}",
        plan2
    );

    cleanup(&client).await;
}

/// Full two-sync roundtrip: first sync uploads new files, second sync shows everything UpToDate.
/// Validates that SyncDb tracking prevents spurious re-uploads.
#[tokio::test]
#[ignore = "requires live AWS credentials (source .env first)"]
async fn s3_sync_full_roundtrip() {
    let local_dir = tempfile::tempdir().unwrap();
    let client = make_client(&test_prefix("full-roundtrip")).await;
    let (mut db, _tmp_db) = make_db();

    // Sync 1: two new local files, S3 empty
    write_local(local_dir.path(), "x.txt", b"file x content");
    write_local(local_dir.path(), "y.txt", b"file y content");

    let plan1 = gather_and_plan(&client, local_dir.path(), &db).await;
    let upload_count = plan1
        .iter()
        .filter(|a| matches!(a, SyncAction::Upload(_)))
        .count();
    assert_eq!(
        upload_count, 2,
        "expected 2 uploads on first sync, got: {:?}",
        plan1
    );

    execute_plan(plan1, &client, local_dir.path(), &mut db).await;

    // Verify both files are in S3
    let objects = client.list_all_objects().await.unwrap();
    assert!(
        objects.iter().any(|o| o.key == "x.txt"),
        "x.txt missing from S3"
    );
    assert!(
        objects.iter().any(|o| o.key == "y.txt"),
        "y.txt missing from S3"
    );

    // Sync 2: nothing changed — must be entirely UpToDate
    let plan2 = gather_and_plan(&client, local_dir.path(), &db).await;
    assert!(
        plan2.iter().all(|a| matches!(a, SyncAction::UpToDate(_))),
        "expected all UpToDate on second sync (DB tracking prevents re-uploads), got: {:?}",
        plan2
    );

    cleanup(&client).await;
}
