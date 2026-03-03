use cobblestone::aws::s3_client::S3Client;
use cobblestone::core::merger::generate_sync_plan;
use cobblestone::core::models::SyncAction;
use cobblestone::core::scanner::scan_local_dir;
use cobblestone::db::sync_db::SyncDb;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about = "A 3-way S3 sync tool in Rust", long_about = None)]
struct Args {
    /// Local directory path to sync
    #[arg(short, long)]
    local_dir: PathBuf,

    /// S3 Bucket name
    #[arg(short, long)]
    bucket: String,

    /// S3 Prefix (optional, acts like a subfolder)
    #[arg(short, long, default_value = "")]
    prefix: String,

    /// Perform a dry run (do not actually upload/download/delete)
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    println!("Starting S3 Sync Tool...");
    println!("Local Dir : {:?}", args.local_dir);
    println!("S3 Bucket : {}", args.bucket);
    println!("S3 Prefix : {}", args.prefix);
    println!("Dry Run   : {}", args.dry_run);
    println!("-----------------------------------");

    // 0. Ensure local directory exists
    if !args.local_dir.exists() {
        println!("Local dir does not exist — creating {:?}", args.local_dir);
        std::fs::create_dir_all(&args.local_dir)?;
    }

    // 1. Initialize AWS SDK Client
    let s3 = S3Client::new(&args.bucket, &args.prefix).await?;

    // 2. Initialize Local Tracking Database
    let db_path = args.local_dir.join(".s3sync.db");
    let mut db = SyncDb::new(&db_path)?;

    // 3. Gather Base State (exclude the sync db itself — it is never a sync target)
    let base_files = db
        .get_all_files()
        .unwrap_or_default()
        .into_iter()
        .filter(|f| f.key != ".s3sync.db")
        .collect::<Vec<_>>();

    // 4. Gather Local State (scanner already skips .s3sync.db)
    let local_files = scan_local_dir(&args.local_dir)?;

    // 5. Gather Remote State (exclude the sync db in case it was previously uploaded)
    let remote_files = s3
        .list_all_objects()
        .await?
        .into_iter()
        .filter(|f| f.key != ".s3sync.db")
        .collect::<Vec<_>>();

    println!(
        "Scanned {} base files, {} local files, {} remote files.",
        base_files.len(),
        local_files.len(),
        remote_files.len()
    );

    // 6. Generate Sync Plan via 3-Way Merge
    let sync_plan = generate_sync_plan(local_files, remote_files, base_files);

    if sync_plan.is_empty() {
        println!("Everything is up to date.");
        return Ok(());
    }

    // 7. Execute Plan
    for action in sync_plan {
        match action {
            SyncAction::Upload(meta) => {
                println!("[UPLOAD] {}", meta.key);
                if !args.dry_run {
                    let local_path = args.local_dir.join(&meta.key);
                    s3.upload_object(&meta.key, &local_path, &meta.checksum)
                        .await?;
                    db.upsert_file(&meta)?;
                }
            }
            SyncAction::Download(meta) => {
                println!("[DOWNLOAD] {}", meta.key);
                if !args.dry_run {
                    let local_path = args.local_dir.join(&meta.key);
                    s3.download_object(&meta.key, &local_path).await?;
                    db.upsert_file(&meta)?;
                }
            }
            SyncAction::DeleteLocal(key) => {
                println!("[DELETE LOCAL] {}", key);
                if !args.dry_run {
                    let local_path = args.local_dir.join(&key);
                    if local_path.exists() {
                        std::fs::remove_file(local_path)?;
                    }
                    db.remove_file(&key)?;
                }
            }
            SyncAction::DeleteRemote(key) => {
                println!("[DELETE REMOTE] {}", key);
                if !args.dry_run {
                    s3.delete_object(&key).await?;
                    db.remove_file(&key)?;
                }
            }
            SyncAction::Conflict { key, local, remote } => {
                println!("[CONFLICT] {}", key);
                println!(
                    "  local:  checksum={} ts={}",
                    local.checksum, local.modified_ts
                );
                println!(
                    "  remote: checksum={} ts={}",
                    remote.checksum, remote.modified_ts
                );

                if args.dry_run {
                    println!("  (dry run: would prompt for interactive resolution)");
                    continue;
                }

                use std::io::IsTerminal;
                if !std::io::stdin().is_terminal() {
                    println!("  [SKIPPED] stdin is not a terminal; skipping");
                    continue;
                }

                // Download remote to companion file
                let local_path = args.local_dir.join(&key);
                let companion_path = {
                    let mut p = local_path.clone().into_os_string();
                    p.push(".remote");
                    std::path::PathBuf::from(p)
                };
                println!(
                    "  Downloading remote version to: {}",
                    companion_path.display()
                );
                s3.download_object(&remote.key, &companion_path).await?;

                // Interactive prompt loop
                loop {
                    use std::io::Write as _;
                    print!("  Resolve '{}': [L]ocal / [R]emote / [S]kip? ", key);
                    std::io::stdout().flush()?;

                    let mut input = String::new();
                    std::io::stdin().read_line(&mut input)?;
                    match input.trim().to_lowercase().as_str() {
                        "l" | "local" => {
                            println!("  -> Keeping local. Uploading to S3...");
                            s3.upload_object(&local.key, &local_path, &local.checksum)
                                .await?;
                            std::fs::remove_file(&companion_path).ok();
                            db.upsert_file(&local)?;
                            break;
                        }
                        "r" | "remote" => {
                            println!("  -> Keeping remote.");
                            std::fs::rename(&companion_path, &local_path)?;
                            db.upsert_file(&remote)?;
                            break;
                        }
                        "s" | "skip" => {
                            println!(
                                "  -> Skipping. Companion file left at: {}",
                                companion_path.display()
                            );
                            println!(
                                "  Note: '{}' will appear as a new local file on the next sync.",
                                companion_path.display()
                            );
                            break;
                        }
                        other => {
                            println!("  Unknown choice '{}'. Enter L, R, or S.", other);
                        }
                    }
                }
            }
            SyncAction::SkipConflict(key) => {
                println!("[CONFLICT - SKIPPED] {}", key);
            }
            SyncAction::UpToDate(_key) => {
                // Ignore, already up to date
            }
        }
    }

    println!("Sync complete.");
    Ok(())
}
