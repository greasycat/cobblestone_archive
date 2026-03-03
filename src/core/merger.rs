use crate::core::models::{FileMeta, SyncAction};
use std::collections::HashMap;

#[derive(Debug, PartialEq)]
enum ChangeType {
    Unchanged,
    Added,
    Modified,
    Deleted,
}

/// Computes the change between the current state and the base state.
fn compute_change(current: Option<&FileMeta>, base: Option<&FileMeta>) -> ChangeType {
    match (current, base) {
        (Some(c), Some(b)) => {
            if c.checksum == b.checksum {
                ChangeType::Unchanged
            } else {
                ChangeType::Modified
            }
        }
        (Some(_), None) => ChangeType::Added,
        (None, Some(_)) => ChangeType::Deleted,
        (None, None) => ChangeType::Unchanged, // Should logically never happen in the loop
    }
}

/// Generates a list of SyncActions based on a 3-way merge of Local, Remote, and Base states.
pub fn generate_sync_plan(
    local_files: Vec<FileMeta>,
    remote_files: Vec<FileMeta>,
    base_files: Vec<FileMeta>,
) -> Vec<SyncAction> {
    // Index arrays by key for fast O(1) lookups
    let local_map: HashMap<String, FileMeta> = local_files
        .into_iter()
        .map(|f| (f.key.clone(), f))
        .collect();
    let remote_map: HashMap<String, FileMeta> = remote_files
        .into_iter()
        .map(|f| (f.key.clone(), f))
        .collect();
    let base_map: HashMap<String, FileMeta> =
        base_files.into_iter().map(|f| (f.key.clone(), f)).collect();

    // Collect all unique keys across all three states
    let mut all_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
    for key in local_map.keys() {
        all_keys.insert(key.clone());
    }
    for key in remote_map.keys() {
        all_keys.insert(key.clone());
    }
    for key in base_map.keys() {
        all_keys.insert(key.clone());
    }

    let mut actions = Vec::new();

    for key in all_keys {
        let local = local_map.get(&key);
        let remote = remote_map.get(&key);
        let base = base_map.get(&key);

        let local_change = compute_change(local, base);
        let remote_change = compute_change(remote, base);

        // Perform the 3-Way Merge resolution
        match (local_change, remote_change) {
            // No changes anywhere -> UpToDate
            (ChangeType::Unchanged, ChangeType::Unchanged) => {
                actions.push(SyncAction::UpToDate(key));
            }

            // Only local changed -> Upload
            (ChangeType::Added, ChangeType::Unchanged)
            | (ChangeType::Modified, ChangeType::Unchanged) => {
                actions.push(SyncAction::Upload(local.unwrap().clone()));
            }

            // Only local deleted -> Delete Remote
            (ChangeType::Deleted, ChangeType::Unchanged) => {
                actions.push(SyncAction::DeleteRemote(key.clone()));
            }

            // Only remote changed -> Download
            (ChangeType::Unchanged, ChangeType::Added)
            | (ChangeType::Unchanged, ChangeType::Modified) => {
                actions.push(SyncAction::Download(remote.unwrap().clone()));
            }

            // Only remote deleted -> Delete Local
            (ChangeType::Unchanged, ChangeType::Deleted) => {
                actions.push(SyncAction::DeleteLocal(key.clone()));
            }

            // BOTH Added -> Verify if they are the exact same or conflicted
            (ChangeType::Added, ChangeType::Added) => {
                let l = local.unwrap();
                let r = remote.unwrap();
                if l.checksum == r.checksum {
                    actions.push(SyncAction::UpToDate(key));
                } else {
                    actions.push(SyncAction::Conflict {
                        key,
                        local: l.clone(),
                        remote: r.clone(),
                    });
                }
            }

            // BOTH Modified -> Conflict, needs interactive resolution
            (ChangeType::Modified, ChangeType::Modified) => {
                let l = local.unwrap();
                let r = remote.unwrap();
                if l.checksum == r.checksum {
                    actions.push(SyncAction::UpToDate(key));
                } else {
                    actions.push(SyncAction::Conflict {
                        key,
                        local: l.clone(),
                        remote: r.clone(),
                    });
                }
            }

            // BOTH Deleted -> Already handled, nothing to do
            (ChangeType::Deleted, ChangeType::Deleted) => {
                actions.push(SyncAction::UpToDate(key)); // Represent successful "nothing to do"
            }

            // Conflicts: Modified one side, Deleted on the other
            (ChangeType::Modified, ChangeType::Deleted) => {
                // Local was modified, remote was deleted. A safe policy: rescue the modified file (Upload).
                actions.push(SyncAction::Upload(local.unwrap().clone()));
            }
            (ChangeType::Deleted, ChangeType::Modified) => {
                // Local was deleted, remote was modified. A safe policy: rescue the modified remote file (Download).
                actions.push(SyncAction::Download(remote.unwrap().clone()));
            }

            // Other complex or impossible states (e.g. Added on one side, Modified/Deleted on the other)
            _ => {
                actions.push(SyncAction::SkipConflict(key.clone()));
            }
        }
    }

    // Optional: Sort actions for consistent output
    actions.sort_by_key(|a| match a {
        SyncAction::Upload(m) => format!("1_{}", m.key),
        SyncAction::Download(m) => format!("2_{}", m.key),
        SyncAction::Conflict { key, .. } => format!("2c_{}", key),
        SyncAction::DeleteLocal(k) => format!("3_{}", k),
        SyncAction::DeleteRemote(k) => format!("4_{}", k),
        SyncAction::SkipConflict(k) => format!("5_{}", k),
        SyncAction::UpToDate(k) => format!("6_{}", k),
    });

    actions
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::scanner::scan_local_dir;
    use std::fs;
    use std::io::Write;
    use std::path::Path;
    use tempfile::tempdir;

    /// Write a file (and any required parent directories) inside a temp dir.
    fn create_file(dir: &Path, name: &str, content: &[u8]) {
        let path = dir.join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let mut f = fs::File::create(path).unwrap();
        f.write_all(content).unwrap();
    }

    fn scan(dir: &Path) -> Vec<FileMeta> {
        scan_local_dir(dir).unwrap()
    }

    // ── single-side new file ──────────────────────────────────────────────────

    #[test]
    fn test_local_only_new_file_uploads() {
        let local = tempdir().unwrap();
        let remote = tempdir().unwrap();
        let base = tempdir().unwrap();

        create_file(local.path(), "new.txt", b"hello local");

        let plan = generate_sync_plan(scan(local.path()), scan(remote.path()), scan(base.path()));
        assert_eq!(plan.len(), 1);
        assert!(matches!(&plan[0], SyncAction::Upload(m) if m.key == "new.txt"));
    }

    #[test]
    fn test_remote_only_new_file_downloads() {
        let local = tempdir().unwrap();
        let remote = tempdir().unwrap();
        let base = tempdir().unwrap();

        create_file(remote.path(), "new.txt", b"hello remote");

        let plan = generate_sync_plan(scan(local.path()), scan(remote.path()), scan(base.path()));
        assert_eq!(plan.len(), 1);
        assert!(matches!(&plan[0], SyncAction::Download(m) if m.key == "new.txt"));
    }

    // ── unchanged ────────────────────────────────────────────────────────────

    #[test]
    fn test_unchanged_file_is_uptodate() {
        let local = tempdir().unwrap();
        let remote = tempdir().unwrap();
        let base = tempdir().unwrap();

        let content = b"same on all three";
        create_file(local.path(), "file.txt", content);
        create_file(remote.path(), "file.txt", content);
        create_file(base.path(), "file.txt", content);

        let plan = generate_sync_plan(scan(local.path()), scan(remote.path()), scan(base.path()));
        assert_eq!(plan.len(), 1);
        assert!(matches!(&plan[0], SyncAction::UpToDate(k) if k == "file.txt"));
    }

    // ── single-side deletion ─────────────────────────────────────────────────

    #[test]
    fn test_local_deletion_removes_remote() {
        // File was present in base and remote but locally deleted → DeleteRemote
        let local = tempdir().unwrap();
        let remote = tempdir().unwrap();
        let base = tempdir().unwrap();

        let content = b"original";
        create_file(remote.path(), "gone.txt", content);
        create_file(base.path(), "gone.txt", content);
        // local has no file

        let plan = generate_sync_plan(scan(local.path()), scan(remote.path()), scan(base.path()));
        assert_eq!(plan.len(), 1);
        assert!(matches!(&plan[0], SyncAction::DeleteRemote(k) if k == "gone.txt"));
    }

    #[test]
    fn test_remote_deletion_removes_local() {
        // File was present in base and local but remotely deleted → DeleteLocal
        let local = tempdir().unwrap();
        let remote = tempdir().unwrap();
        let base = tempdir().unwrap();

        let content = b"original";
        create_file(local.path(), "gone.txt", content);
        create_file(base.path(), "gone.txt", content);
        // remote has no file

        let plan = generate_sync_plan(scan(local.path()), scan(remote.path()), scan(base.path()));
        assert_eq!(plan.len(), 1);
        assert!(matches!(&plan[0], SyncAction::DeleteLocal(k) if k == "gone.txt"));
    }

    // ── single-side modification ─────────────────────────────────────────────

    #[test]
    fn test_local_modification_uploads() {
        let local = tempdir().unwrap();
        let remote = tempdir().unwrap();
        let base = tempdir().unwrap();

        let original = b"original content";
        let modified = b"locally modified";

        create_file(local.path(), "file.txt", modified);
        create_file(remote.path(), "file.txt", original);
        create_file(base.path(), "file.txt", original);

        let plan = generate_sync_plan(scan(local.path()), scan(remote.path()), scan(base.path()));
        assert_eq!(plan.len(), 1);
        assert!(matches!(&plan[0], SyncAction::Upload(m) if m.key == "file.txt"));
    }

    #[test]
    fn test_remote_modification_downloads() {
        let local = tempdir().unwrap();
        let remote = tempdir().unwrap();
        let base = tempdir().unwrap();

        let original = b"original content";
        let modified = b"remotely modified";

        create_file(local.path(), "file.txt", original);
        create_file(remote.path(), "file.txt", modified);
        create_file(base.path(), "file.txt", original);

        let plan = generate_sync_plan(scan(local.path()), scan(remote.path()), scan(base.path()));
        assert_eq!(plan.len(), 1);
        assert!(matches!(&plan[0], SyncAction::Download(m) if m.key == "file.txt"));
    }

    // ── both-side changes, same content ──────────────────────────────────────

    #[test]
    fn test_both_added_same_content_is_uptodate() {
        // Same file independently created on both sides → no conflict
        let local = tempdir().unwrap();
        let remote = tempdir().unwrap();
        let base = tempdir().unwrap();

        let content = b"identical on both sides";
        create_file(local.path(), "file.txt", content);
        create_file(remote.path(), "file.txt", content);
        // base is empty

        let plan = generate_sync_plan(scan(local.path()), scan(remote.path()), scan(base.path()));
        assert_eq!(plan.len(), 1);
        assert!(matches!(&plan[0], SyncAction::UpToDate(_)));
    }

    #[test]
    fn test_both_modified_to_same_content_is_uptodate() {
        let local = tempdir().unwrap();
        let remote = tempdir().unwrap();
        let base = tempdir().unwrap();

        let original = b"old content";
        let modified = b"new identical content";

        create_file(local.path(), "file.txt", modified);
        create_file(remote.path(), "file.txt", modified);
        create_file(base.path(), "file.txt", original);

        let plan = generate_sync_plan(scan(local.path()), scan(remote.path()), scan(base.path()));
        assert_eq!(plan.len(), 1);
        assert!(matches!(&plan[0], SyncAction::UpToDate(_)));
    }

    // ── both-side deletion ───────────────────────────────────────────────────

    #[test]
    fn test_both_deleted_is_uptodate() {
        // File deleted everywhere; base is the only place it ever existed
        let local = tempdir().unwrap();
        let remote = tempdir().unwrap();
        let base = tempdir().unwrap();

        create_file(base.path(), "gone.txt", b"was here");
        // neither local nor remote have the file

        let plan = generate_sync_plan(scan(local.path()), scan(remote.path()), scan(base.path()));
        assert_eq!(plan.len(), 1);
        assert!(matches!(&plan[0], SyncAction::UpToDate(_)));
    }

    // ── modify/delete conflicts ───────────────────────────────────────────────

    #[test]
    fn test_local_modified_remote_deleted_rescues_local() {
        // Local kept and modified the file; remote deleted it → Upload
        let local = tempdir().unwrap();
        let remote = tempdir().unwrap();
        let base = tempdir().unwrap();

        create_file(local.path(), "file.txt", b"local keeps this");
        create_file(base.path(), "file.txt", b"original");
        // remote has no file

        let plan = generate_sync_plan(scan(local.path()), scan(remote.path()), scan(base.path()));
        assert_eq!(plan.len(), 1);
        assert!(matches!(&plan[0], SyncAction::Upload(m) if m.key == "file.txt"));
    }

    #[test]
    fn test_local_deleted_remote_modified_rescues_remote() {
        // Remote kept and modified the file; local deleted it → Download
        let local = tempdir().unwrap();
        let remote = tempdir().unwrap();
        let base = tempdir().unwrap();

        create_file(remote.path(), "file.txt", b"remote keeps this");
        create_file(base.path(), "file.txt", b"original");
        // local has no file

        let plan = generate_sync_plan(scan(local.path()), scan(remote.path()), scan(base.path()));
        assert_eq!(plan.len(), 1);
        assert!(matches!(&plan[0], SyncAction::Download(m) if m.key == "file.txt"));
    }

    // ── both-side different content: interactive conflict ─────────────────────

    #[test]
    fn test_both_added_different_content_produces_conflict() {
        let local = tempdir().unwrap();
        let remote = tempdir().unwrap();
        let base = tempdir().unwrap();

        create_file(local.path(), "conflict.txt", b"local version");
        create_file(remote.path(), "conflict.txt", b"remote version");

        let plan = generate_sync_plan(scan(local.path()), scan(remote.path()), scan(base.path()));
        assert_eq!(plan.len(), 1);
        assert!(
            matches!(&plan[0], SyncAction::Conflict { key, .. } if key == "conflict.txt"),
            "Expected Conflict because both sides added with different content"
        );
    }

    #[test]
    fn test_both_modified_different_content_produces_conflict() {
        let local = tempdir().unwrap();
        let remote = tempdir().unwrap();
        let base = tempdir().unwrap();

        create_file(base.path(), "file.txt", b"original");
        create_file(remote.path(), "file.txt", b"remote edit");
        create_file(local.path(), "file.txt", b"local edit");

        let plan = generate_sync_plan(scan(local.path()), scan(remote.path()), scan(base.path()));
        assert_eq!(plan.len(), 1);
        assert!(
            matches!(&plan[0], SyncAction::Conflict { key, .. } if key == "file.txt"),
            "Expected Conflict because both sides modified with different content"
        );
    }

    // ── multiple files ────────────────────────────────────────────────────────

    #[test]
    fn test_multiple_files_mixed_actions() {
        let local = tempdir().unwrap();
        let remote = tempdir().unwrap();
        let base = tempdir().unwrap();

        // a.txt: local only → Upload
        create_file(local.path(), "a.txt", b"local only");

        // b.txt: remote only → Download
        create_file(remote.path(), "b.txt", b"remote only");

        // c.txt: unchanged everywhere → UpToDate
        let common = b"common content";
        create_file(local.path(), "c.txt", common);
        create_file(remote.path(), "c.txt", common);
        create_file(base.path(), "c.txt", common);

        // d.txt: in base and remote, deleted from local → DeleteRemote
        create_file(remote.path(), "d.txt", b"was synced");
        create_file(base.path(), "d.txt", b"was synced");

        // e.txt: in base and local, deleted from remote → DeleteLocal
        create_file(local.path(), "e.txt", b"was synced too");
        create_file(base.path(), "e.txt", b"was synced too");

        // f.txt: local modified, remote unchanged → Upload
        create_file(local.path(), "f.txt", b"local modified f");
        create_file(remote.path(), "f.txt", b"original f");
        create_file(base.path(), "f.txt", b"original f");

        let plan = generate_sync_plan(scan(local.path()), scan(remote.path()), scan(base.path()));
        assert_eq!(plan.len(), 6);

        assert!(
            plan.iter()
                .any(|a| matches!(a, SyncAction::Upload(m) if m.key == "a.txt"))
        );
        assert!(
            plan.iter()
                .any(|a| matches!(a, SyncAction::Download(m) if m.key == "b.txt"))
        );
        assert!(
            plan.iter()
                .any(|a| matches!(a, SyncAction::UpToDate(k) if k == "c.txt"))
        );
        assert!(
            plan.iter()
                .any(|a| matches!(a, SyncAction::DeleteRemote(k) if k == "d.txt"))
        );
        assert!(
            plan.iter()
                .any(|a| matches!(a, SyncAction::DeleteLocal(k) if k == "e.txt"))
        );
        assert!(
            plan.iter()
                .any(|a| matches!(a, SyncAction::Upload(m) if m.key == "f.txt"))
        );
    }

    // ── nested directory paths ────────────────────────────────────────────────

    #[test]
    fn test_nested_directory_keys_use_forward_slashes() {
        let local = tempdir().unwrap();
        let remote = tempdir().unwrap();
        let base = tempdir().unwrap();

        create_file(local.path(), "docs/sub/readme.md", b"nested file");

        let plan = generate_sync_plan(scan(local.path()), scan(remote.path()), scan(base.path()));
        assert_eq!(plan.len(), 1);
        assert!(matches!(&plan[0], SyncAction::Upload(m) if m.key == "docs/sub/readme.md"));
    }

    #[test]
    fn test_nested_remote_file_downloads_with_correct_key() {
        let local = tempdir().unwrap();
        let remote = tempdir().unwrap();
        let base = tempdir().unwrap();

        create_file(remote.path(), "assets/images/logo.png", b"png data");

        let plan = generate_sync_plan(scan(local.path()), scan(remote.path()), scan(base.path()));
        assert_eq!(plan.len(), 1);
        assert!(matches!(&plan[0], SyncAction::Download(m) if m.key == "assets/images/logo.png"));
    }

    // ── empty sync ────────────────────────────────────────────────────────────

    #[test]
    fn test_empty_everything_produces_no_actions() {
        let local = tempdir().unwrap();
        let remote = tempdir().unwrap();
        let base = tempdir().unwrap();

        let plan = generate_sync_plan(scan(local.path()), scan(remote.path()), scan(base.path()));
        assert!(plan.is_empty());
    }

    // ── manual inspection ─────────────────────────────────────────────────────
    // Run with: cargo test manual_ -- --ignored --nocapture
    // Files are written to /tmp/cobblestone_inspect/<scenario>/ and persist
    // after the run. A summary.txt is written to each scenario root.

    fn setup_dir(path: &Path) {
        if path.exists() {
            fs::remove_dir_all(path).unwrap();
        }
        fs::create_dir_all(path).unwrap();
    }

    fn collect_paths(dir: &Path, base: &Path, out: &mut Vec<(String, String)>) {
        if let Ok(entries) = fs::read_dir(dir) {
            let mut entries: Vec<_> = entries.filter_map(|e| e.ok()).collect();
            entries.sort_by_key(|e| e.path());
            for entry in entries {
                let path = entry.path();
                if path.is_dir() {
                    collect_paths(&path, base, out);
                } else {
                    let rel = path
                        .strip_prefix(base)
                        .unwrap()
                        .to_string_lossy()
                        .to_string();
                    let content = fs::read_to_string(&path).unwrap_or_else(|_| "<binary>".into());
                    out.push((rel, content));
                }
            }
        }
    }

    // ── random content helpers ────────────────────────────────────────────────

    struct SimpleRng {
        state: u64,
    }

    impl SimpleRng {
        /// Seed from wall-clock nanoseconds so each run produces fresh content.
        fn new() -> Self {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos() as u64;
            Self {
                state: nanos.wrapping_add(0x9e37_79b9_7f4a_7c15),
            }
        }

        fn next_u64(&mut self) -> u64 {
            self.state ^= self.state << 13;
            self.state ^= self.state >> 7;
            self.state ^= self.state << 17;
            self.state
        }

        fn next_usize(&mut self, max: usize) -> usize {
            (self.next_u64() % max as u64) as usize
        }

        fn pick<'a>(&mut self, items: &'a [&str]) -> &'a str {
            items[self.next_usize(items.len())]
        }
    }

    const WORDS: &[&str] = &[
        "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india",
        "juliet", "kilo", "lima", "mike", "november", "oscar", "papa", "quebec", "romeo",
    ];

    fn random_phrase(rng: &mut SimpleRng) -> String {
        let n = rng.next_usize(3) + 2;
        (0..n)
            .map(|_| rng.pick(WORDS))
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Generates a 10-line file body with random content.
    fn random_lines(rng: &mut SimpleRng) -> Vec<String> {
        (1..=10)
            .map(|i| format!("{i:02}: {}", random_phrase(rng)))
            .collect()
    }

    /// Returns a modified copy of `lines` with exactly 3 changes:
    ///   update – appends "  <UPDATED>" to one existing line
    ///   delete – removes a different line
    ///   add    – inserts a new "NEW: …" line at a random position
    fn modify_lines(lines: &[String], rng: &mut SimpleRng) -> Vec<String> {
        let mut v = lines.to_vec();

        let u = rng.next_usize(v.len());
        v[u] = format!("{}  <UPDATED>", v[u]);

        let d = {
            let mut i = rng.next_usize(v.len());
            if i == u && v.len() > 1 {
                i = (i + 1) % v.len();
            }
            i
        };
        v.remove(d);

        let ins = rng.next_usize(v.len() + 1);
        v.insert(ins, format!("NEW: {}", random_phrase(rng)));

        v
    }

    // ── summary collector ─────────────────────────────────────────────────────

    struct SyncSummary {
        buf: Vec<String>,
    }

    impl SyncSummary {
        fn new(scenario: &str) -> Self {
            let secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let mut s = Self { buf: Vec::new() };
            s.ln(format!("scenario : {scenario}"));
            s.ln(format!("timestamp: {secs}"));
            s
        }

        fn ln(&mut self, line: impl Into<String>) {
            self.buf.push(line.into());
        }

        /// Print a section header to stdout and record it in the summary.
        fn section(&mut self, title: &str) {
            println!("\n=== {title} ===");
            self.ln(String::new());
            self.ln(format!("=== {title} ==="));
        }

        /// Snapshot a directory: prints each file's content to stdout and
        /// records it line-by-line in the summary buffer.
        fn snapshot(&mut self, label: &str, dir: &Path) {
            println!("  [{label}]");
            self.ln(format!("  [{label}]"));
            let mut paths = Vec::new();
            collect_paths(dir, dir, &mut paths);
            if paths.is_empty() {
                println!("    (empty)");
                self.ln("    (empty)");
            }
            for (rel, content) in &paths {
                println!("  {rel}:");
                self.ln(format!("  {rel}:"));
                for line in content.lines() {
                    println!("    {line}");
                    self.ln(format!("    {line}"));
                }
            }
        }

        fn write_to(&self, path: &Path) {
            fs::write(path, self.buf.join("\n")).unwrap();
        }
    }

    // ── plan executor ─────────────────────────────────────────────────────────

    fn execute_plan_locally(
        plan: &[SyncAction],
        local: &Path,
        remote: &Path,
        summary: &mut SyncSummary,
    ) {
        for action in plan {
            let msg = match action {
                SyncAction::Upload(m) => {
                    let dst = remote.join(&m.key);
                    if let Some(p) = dst.parent() {
                        fs::create_dir_all(p).unwrap();
                    }
                    fs::copy(local.join(&m.key), &dst).unwrap();
                    format!("  [UPLOAD]        {}", m.key)
                }
                SyncAction::Download(m) => {
                    let dst = local.join(&m.key);
                    if let Some(p) = dst.parent() {
                        fs::create_dir_all(p).unwrap();
                    }
                    fs::copy(remote.join(&m.key), &dst).unwrap();
                    format!("  [DOWNLOAD]      {}", m.key)
                }
                SyncAction::DeleteLocal(k) => {
                    let p = local.join(k);
                    if p.exists() {
                        fs::remove_file(&p).unwrap();
                    }
                    format!("  [DELETE LOCAL]  {k}")
                }
                SyncAction::DeleteRemote(k) => {
                    let p = remote.join(k);
                    if p.exists() {
                        fs::remove_file(&p).unwrap();
                    }
                    format!("  [DELETE REMOTE] {k}")
                }
                SyncAction::Conflict { key, .. } => {
                    format!("  [CONFLICT]      {} (needs interactive resolution)", key)
                }
                SyncAction::SkipConflict(k) => format!("  [CONFLICT]      {k}"),
                SyncAction::UpToDate(k) => format!("  [UP TO DATE]    {k}"),
            };
            println!("{msg}");
            summary.ln(&msg);
        }
    }

    // ── scenario setup helper ─────────────────────────────────────────────────

    fn scenario_dirs(
        name: &str,
    ) -> (
        std::path::PathBuf,
        std::path::PathBuf,
        std::path::PathBuf,
        std::path::PathBuf,
    ) {
        let root = std::path::PathBuf::from("/tmp/cobblestone_inspect").join(name);
        let local = root.join("local");
        let remote = root.join("remote");
        let base = root.join("base");
        setup_dir(&local);
        setup_dir(&remote);
        setup_dir(&base);
        (root, local, remote, base)
    }

    // ── scenarios ─────────────────────────────────────────────────────────────

    /// First-ever sync: no base state, files only on one side each.
    /// Expected: all local files uploaded, all remote files downloaded.
    #[test]
    #[ignore]
    fn manual_initial_sync() {
        let mut rng = SimpleRng::new();
        let (root, local, remote, base) = scenario_dirs("initial_sync");
        let mut summary = SyncSummary::new("initial_sync");

        create_file(
            &local,
            "readme.txt",
            random_lines(&mut rng).join("\n").as_bytes(),
        );
        create_file(
            &local,
            "src/main.rs",
            random_lines(&mut rng).join("\n").as_bytes(),
        );
        create_file(
            &remote,
            "docs/guide.md",
            random_lines(&mut rng).join("\n").as_bytes(),
        );
        create_file(
            &remote,
            "src/lib.rs",
            random_lines(&mut rng).join("\n").as_bytes(),
        );

        summary.section("BEFORE");
        summary.snapshot("local", &local);
        summary.snapshot("remote", &remote);
        summary.snapshot("base (empty)", &base);

        let plan = generate_sync_plan(scan(&local), scan(&remote), scan(&base));

        summary.section("ACTIONS");
        execute_plan_locally(&plan, &local, &remote, &mut summary);

        summary.section("AFTER");
        summary.snapshot("local", &local);
        summary.snapshot("remote", &remote);

        summary.write_to(&root.join("summary.txt"));
        println!("\nsummary → {}/summary.txt", root.display());
    }

    /// Typical day: each side independently modified a different file and added a new one.
    /// modify_lines applies: 1 update, 1 delete, 1 add (3 changes total per file).
    #[test]
    #[ignore]
    fn manual_typical_day() {
        let mut rng = SimpleRng::new();
        let (root, local, remote, base) = scenario_dirs("typical_day");
        let mut summary = SyncSummary::new("typical_day");

        let base_a = random_lines(&mut rng);
        let base_b = random_lines(&mut rng);
        let base_c = random_lines(&mut rng);

        for (name, lines) in [
            ("file_a.txt", &base_a),
            ("file_b.txt", &base_b),
            ("file_c.txt", &base_c),
        ] {
            let bytes = lines.join("\n");
            create_file(&base, name, bytes.as_bytes());
            create_file(&local, name, bytes.as_bytes());
            create_file(&remote, name, bytes.as_bytes());
        }

        // local modifies file_a (update+delete+add) and adds a new file
        create_file(
            &local,
            "file_a.txt",
            modify_lines(&base_a, &mut rng).join("\n").as_bytes(),
        );
        create_file(
            &local,
            "new_from_local.txt",
            random_lines(&mut rng).join("\n").as_bytes(),
        );

        // remote modifies file_b (update+delete+add) and adds a new file
        create_file(
            &remote,
            "file_b.txt",
            modify_lines(&base_b, &mut rng).join("\n").as_bytes(),
        );
        create_file(
            &remote,
            "new_from_remote.txt",
            random_lines(&mut rng).join("\n").as_bytes(),
        );

        summary.section("BEFORE");
        summary.snapshot("local", &local);
        summary.snapshot("remote", &remote);

        let plan = generate_sync_plan(scan(&local), scan(&remote), scan(&base));

        summary.section("ACTIONS");
        execute_plan_locally(&plan, &local, &remote, &mut summary);

        summary.section("AFTER");
        summary.snapshot("local", &local);
        summary.snapshot("remote", &remote);

        summary.write_to(&root.join("summary.txt"));
        println!("\nsummary → {}/summary.txt", root.display());
    }

    /// Deletions: each side deleted a different file; one deleted on both sides.
    /// Expected: DeleteRemote, DeleteLocal, UpToDate.
    #[test]
    #[ignore]
    fn manual_deletions() {
        let mut rng = SimpleRng::new();
        let (root, local, remote, base) = scenario_dirs("deletions");
        let mut summary = SyncSummary::new("deletions");

        for name in [
            "keep.txt",
            "del_local.txt",
            "del_remote.txt",
            "del_both.txt",
        ] {
            let bytes = random_lines(&mut rng).join("\n");
            create_file(&base, name, bytes.as_bytes());
            create_file(&local, name, bytes.as_bytes());
            create_file(&remote, name, bytes.as_bytes());
        }

        // Simulate deletions by removing from each side
        fs::remove_file(local.join("del_local.txt")).unwrap();
        fs::remove_file(local.join("del_both.txt")).unwrap();
        fs::remove_file(remote.join("del_remote.txt")).unwrap();
        fs::remove_file(remote.join("del_both.txt")).unwrap();

        summary.section("BEFORE");
        summary.snapshot("local", &local);
        summary.snapshot("remote", &remote);
        summary.snapshot("base (last sync)", &base);

        let plan = generate_sync_plan(scan(&local), scan(&remote), scan(&base));

        summary.section("ACTIONS");
        execute_plan_locally(&plan, &local, &remote, &mut summary);

        summary.section("AFTER");
        summary.snapshot("local", &local);
        summary.snapshot("remote", &remote);

        summary.write_to(&root.join("summary.txt"));
        println!("\nsummary → {}/summary.txt", root.display());
    }

    /// Conflict: both sides ran modify_lines on the same base; remote written 1.1 s later → Download wins.
    #[test]
    #[ignore]
    fn manual_conflict_newer_wins() {
        let mut rng = SimpleRng::new();
        let (root, local, remote, base) = scenario_dirs("conflict");
        let mut summary = SyncSummary::new("conflict");

        let original = random_lines(&mut rng);
        create_file(&base, "raced.txt", original.join("\n").as_bytes());
        create_file(
            &local,
            "raced.txt",
            modify_lines(&original, &mut rng).join("\n").as_bytes(),
        );
        std::thread::sleep(std::time::Duration::from_millis(1100));
        create_file(
            &remote,
            "raced.txt",
            modify_lines(&original, &mut rng).join("\n").as_bytes(),
        );

        summary.section("BEFORE");
        summary.snapshot("local (older)", &local);
        summary.snapshot("remote (newer, written 1.1s later)", &remote);

        let plan = generate_sync_plan(scan(&local), scan(&remote), scan(&base));

        summary.section("ACTIONS");
        execute_plan_locally(&plan, &local, &remote, &mut summary);

        summary.section("AFTER");
        summary.snapshot("local", &local);
        summary.snapshot("remote", &remote);

        summary.write_to(&root.join("summary.txt"));
        println!("\nsummary → {}/summary.txt", root.display());
    }

    /// Rescue: modified-vs-deleted on each side; both survivors are kept.
    /// keep_local: local ran modify_lines, remote deleted → Upload.
    /// keep_remote: local deleted, remote ran modify_lines → Download.
    #[test]
    #[ignore]
    fn manual_rescue_from_deletion() {
        let mut rng = SimpleRng::new();
        let (root, local, remote, base) = scenario_dirs("rescue");
        let mut summary = SyncSummary::new("rescue");

        let orig_a = random_lines(&mut rng);
        let orig_b = random_lines(&mut rng);

        create_file(&base, "keep_local.txt", orig_a.join("\n").as_bytes());
        create_file(
            &local,
            "keep_local.txt",
            modify_lines(&orig_a, &mut rng).join("\n").as_bytes(),
        );
        // remote has no keep_local.txt (deleted)

        create_file(&base, "keep_remote.txt", orig_b.join("\n").as_bytes());
        // local has no keep_remote.txt (deleted)
        create_file(
            &remote,
            "keep_remote.txt",
            modify_lines(&orig_b, &mut rng).join("\n").as_bytes(),
        );

        summary.section("BEFORE");
        summary.snapshot("local", &local);
        summary.snapshot("remote", &remote);
        summary.snapshot("base", &base);

        let plan = generate_sync_plan(scan(&local), scan(&remote), scan(&base));

        summary.section("ACTIONS");
        execute_plan_locally(&plan, &local, &remote, &mut summary);

        summary.section("AFTER");
        summary.snapshot("local", &local);
        summary.snapshot("remote", &remote);

        summary.write_to(&root.join("summary.txt"));
        println!("\nsummary → {}/summary.txt", root.display());
    }
}
