# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

`cobblestone` is a CLI tool that performs **3-way S3 sync** between a local directory and an S3 bucket. Unlike simple one-way sync, it uses a local SQLite database (`.s3sync.db`) as the "base state" to determine who changed what since the last sync, allowing intelligent conflict resolution.

## Commands

```bash
# Build
cargo build

# Run (requires AWS credentials in env or ~/.aws/)
cargo run -- --local-dir /path/to/dir --bucket my-bucket --prefix optional/prefix
cargo run -- --local-dir /path/to/dir --bucket my-bucket --dry-run

# Test
cargo test

# Run a single test
cargo test test_merge_no_changes
cargo test --test-name <name>

# Lint / format
cargo clippy
cargo fmt
```

## Architecture

The sync pipeline in `main.rs` runs five sequential steps:

1. **`db::SyncDb`** — reads the SQLite base state (`get_all_files`)
2. **`core::scanner::scan_local_dir`** — walks the local directory, computes MD5 checksums
3. **`aws::S3Client::list_all_objects`** — lists S3 objects (paginated), strips prefix to produce relative keys
4. **`core::merger::generate_sync_plan`** — 3-way merge of local / remote / base states → `Vec<SyncAction>`
5. Execute the plan: upload, download, delete local, delete remote, or skip conflicts; then update the DB

### Key types (`src/core/models.rs`)

- **`FileMeta`** — `{ key, size, modified_ts, checksum }`. Keys are always relative, using `/` separators (S3 format).
- **`SyncAction`** — `Upload | Download | DeleteLocal | DeleteRemote | SkipConflict | UpToDate`
- **`FileState`** — `Present(FileMeta) | Absent` (used internally)

### 3-Way Merge Logic (`src/core/merger.rs`)

`generate_sync_plan` computes per-file `ChangeType` (Unchanged / Added / Modified / Deleted) for both local and remote relative to base, then resolves:

| Local change | Remote change | Action |
|---|---|---|
| Unchanged | Unchanged | UpToDate |
| Added/Modified | Unchanged | Upload |
| Deleted | Unchanged | DeleteRemote |
| Unchanged | Added/Modified | Download |
| Unchanged | Deleted | DeleteLocal |
| Added/Modified | Added/Modified (same checksum) | UpToDate |
| Added/Modified | Added/Modified (conflict) | Newer timestamp wins |
| Modified | Deleted | Upload (rescue local) |
| Deleted | Modified | Download (rescue remote) |
| Other | Other | SkipConflict |

### S3 Client (`src/aws/s3_client.rs`)

Uses `aws-config` default credential chain. ETag values from S3 are double-quoted and stripped. Multipart upload ETags (containing `-`) are not true MD5s — this is a known limitation affecting conflict detection.

### Database (`src/db/sync_db.rs`)

SQLite via `rusqlite` with bundled feature (no system SQLite required). Schema: single table `base_state (key TEXT PRIMARY KEY, size, modified_ts, checksum)`. `SyncDb::in_memory()` is available for tests.

## Git Commit Format

Follow the conventional commits style from `AGENTS.md`:

```
<type>[optional scope]: <description>

[optional body]
[optional footer(s)]
```

Types: `feat fix docs style refactor perf test chore`. Breaking changes append `!`. All lowercase.
