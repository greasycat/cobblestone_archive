/// Integration tests for S3Client CRUD operations.
///
/// Run with real credentials after sourcing .env:
///   source .env && cargo test --test s3_integration -- --ignored --nocapture

fn test_prefix(label: &str) -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("cobblestone-test/{}/{}", ts, label)
}

async fn make_client(prefix: &str) -> cobblestone::aws::s3_client::S3Client {
    let bucket = std::env::var("S3_BUCKET_NAME")
        .expect("S3_BUCKET_NAME must be set (source .env first)");
    cobblestone::aws::s3_client::S3Client::new(&bucket, prefix)
        .await
        .expect("failed to create S3Client")
}

/// Upload a file and verify it appears in list_all_objects with the correct size.
#[tokio::test]
#[ignore = "requires live AWS credentials (source .env first)"]
async fn s3_upload_and_list() {
    let prefix = test_prefix("upload-and-list");
    let client = make_client(&prefix).await;

    let content = b"hello from s3_upload_and_list";
    let tmp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(tmp.path(), content).unwrap();

    let key = "test-file.txt";
    client
        .upload_object(key, tmp.path(), "")
        .await
        .expect("upload_object failed");

    let objects = client
        .list_all_objects()
        .await
        .expect("list_all_objects failed");

    let found = objects.iter().find(|o| o.key == key);
    assert!(found.is_some(), "uploaded key '{key}' not found in listing");
    assert_eq!(
        found.unwrap().size,
        content.len() as u64,
        "listed size does not match uploaded content length"
    );

    // Cleanup
    client.delete_object(key).await.ok();
}

/// Upload a file and download it; verify the downloaded bytes match the original.
#[tokio::test]
#[ignore = "requires live AWS credentials (source .env first)"]
async fn s3_download_roundtrip() {
    let prefix = test_prefix("download-roundtrip");
    let client = make_client(&prefix).await;

    let content = b"roundtrip content: \xde\xad\xbe\xef";
    let src = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(src.path(), content).unwrap();

    let key = "roundtrip.bin";
    client
        .upload_object(key, src.path(), "")
        .await
        .expect("upload_object failed");

    let dst_dir = tempfile::tempdir().unwrap();
    let dst_path = dst_dir.path().join("downloaded.bin");
    client
        .download_object(key, &dst_path)
        .await
        .expect("download_object failed");

    let downloaded = std::fs::read(&dst_path).expect("failed to read downloaded file");
    assert_eq!(
        downloaded, content,
        "downloaded content does not match uploaded content"
    );

    // Cleanup
    client.delete_object(key).await.ok();
}

/// Upload a file, confirm it is listed, delete it, confirm it is gone.
#[tokio::test]
#[ignore = "requires live AWS credentials (source .env first)"]
async fn s3_delete_removes_from_listing() {
    let prefix = test_prefix("delete-removes");
    let client = make_client(&prefix).await;

    let tmp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(tmp.path(), b"to be deleted").unwrap();

    let key = "delete-me.txt";
    client
        .upload_object(key, tmp.path(), "")
        .await
        .expect("upload_object failed");

    let before = client
        .list_all_objects()
        .await
        .expect("list_all_objects failed (before delete)");
    assert!(
        before.iter().any(|o| o.key == key),
        "key '{key}' not found before delete"
    );

    client
        .delete_object(key)
        .await
        .expect("delete_object failed");

    let after = client
        .list_all_objects()
        .await
        .expect("list_all_objects failed (after delete)");
    assert!(
        !after.iter().any(|o| o.key == key),
        "key '{key}' still present after delete"
    );
}

/// Two clients with different prefixes should see only their own objects.
#[tokio::test]
#[ignore = "requires live AWS credentials (source .env first)"]
async fn s3_prefix_isolation() {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let prefix_a = format!("cobblestone-test/{}/alpha", ts);
    let prefix_b = format!("cobblestone-test/{}/beta", ts);

    let client_a = make_client(&prefix_a).await;
    let client_b = make_client(&prefix_b).await;

    let tmp_a = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(tmp_a.path(), b"alpha content").unwrap();
    let tmp_b = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(tmp_b.path(), b"beta content").unwrap();

    let key_a = "alpha.txt";
    let key_b = "beta.txt";

    client_a
        .upload_object(key_a, tmp_a.path(), "")
        .await
        .expect("upload alpha failed");
    client_b
        .upload_object(key_b, tmp_b.path(), "")
        .await
        .expect("upload beta failed");

    let list_a = client_a
        .list_all_objects()
        .await
        .expect("list alpha failed");
    let list_b = client_b
        .list_all_objects()
        .await
        .expect("list beta failed");

    assert!(
        list_a.iter().any(|o| o.key == key_a),
        "alpha client should see '{key_a}'"
    );
    assert!(
        !list_a.iter().any(|o| o.key == key_b),
        "alpha client must NOT see '{key_b}'"
    );
    assert!(
        list_b.iter().any(|o| o.key == key_b),
        "beta client should see '{key_b}'"
    );
    assert!(
        !list_b.iter().any(|o| o.key == key_a),
        "beta client must NOT see '{key_a}'"
    );

    // Cleanup
    client_a.delete_object(key_a).await.ok();
    client_b.delete_object(key_b).await.ok();
}

/// After upload via upload_object (which sets ChecksumAlgorithm::Sha256),
/// list_all_objects should return a 64-char hex checksum — not an "etag:" sentinel.
#[tokio::test]
#[ignore = "requires live AWS credentials (source .env first)"]
async fn s3_upload_stores_sha256_checksum() {
    let prefix = test_prefix("sha256-checksum");
    let client = make_client(&prefix).await;

    let tmp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(tmp.path(), b"checksum verification content").unwrap();

    let key = "checksum-test.txt";
    client
        .upload_object(key, tmp.path(), "")
        .await
        .expect("upload_object failed");

    let objects = client
        .list_all_objects()
        .await
        .expect("list_all_objects failed");

    let found = objects
        .iter()
        .find(|o| o.key == key)
        .expect("uploaded key not found in listing");

    assert!(
        !found.checksum.starts_with("etag:"),
        "checksum should be SHA-256 hex, got etag fallback: {}",
        found.checksum
    );
    assert_eq!(
        found.checksum.len(),
        64,
        "SHA-256 hex checksum must be 64 characters, got: {}",
        found.checksum
    );
    assert!(
        found.checksum.chars().all(|c| c.is_ascii_hexdigit()),
        "checksum must be lowercase hex, got: {}",
        found.checksum
    );

    // Cleanup
    client.delete_object(key).await.ok();
}
