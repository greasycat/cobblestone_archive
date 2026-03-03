use crate::core::models::FileMeta;
use rusqlite::{Connection, Result, params};
use std::path::Path;

pub struct SyncDb {
    conn: Connection,
}

impl SyncDb {
    /// Connect to the database or create it if it doesn't exist.
    pub fn new<P: AsRef<Path>>(db_path: P) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        let db = Self { conn };
        db.init()?;
        Ok(db)
    }

    #[cfg(test)]
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let db = Self { conn };
        db.init()?;
        Ok(db)
    }

    /// Initialize the database schema and run any pending migrations.
    fn init(&self) -> Result<()> {
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS base_state (
                key TEXT PRIMARY KEY,
                size INTEGER NOT NULL,
                modified_ts INTEGER NOT NULL,
                checksum TEXT NOT NULL
            )",
            [],
        )?;

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)",
            [],
        )?;

        let version: Option<i64> = self
            .conn
            .query_row(
                "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1",
                [],
                |r| r.get(0),
            )
            .ok();

        // Migrate to v2: remove any rows whose checksum is not 64-char lowercase hex.
        // This covers legacy MD5 (32 chars) and old ETag entries from before SHA-256 support.
        if version.unwrap_or(0) < 2 {
            self.conn.execute(
                "DELETE FROM base_state
                 WHERE length(checksum) <> 64 OR checksum GLOB '*[^0-9a-f]*'",
                [],
            )?;
            self.conn
                .execute("INSERT INTO schema_version VALUES (2)", [])?;
        }

        Ok(())
    }

    /// Insert or update a file metadata record in the base state.
    pub fn upsert_file(&mut self, meta: &FileMeta) -> Result<()> {
        self.conn.execute(
            "INSERT INTO base_state (key, size, modified_ts, checksum)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(key) DO UPDATE SET
                size=excluded.size,
                modified_ts=excluded.modified_ts,
                checksum=excluded.checksum",
            params![
                meta.key,
                meta.size as i64,
                meta.modified_ts as i64,
                meta.checksum
            ],
        )?;
        Ok(())
    }

    /// Remove a file metadata record from the base state.
    pub fn remove_file(&mut self, key: &str) -> Result<()> {
        self.conn
            .execute("DELETE FROM base_state WHERE key = ?1", params![key])?;
        Ok(())
    }

    /// Gets all files currently known in the base state
    pub fn get_all_files(&self) -> Result<Vec<FileMeta>> {
        let mut stmt = self
            .conn
            .prepare("SELECT key, size, modified_ts, checksum FROM base_state")?;
        let metas_iter = stmt.query_map([], |row| {
            Ok(FileMeta {
                key: row.get(0)?,
                size: row.get::<_, i64>(1)? as u64,
                modified_ts: row.get::<_, i64>(2)? as u64,
                checksum: row.get(3)?,
            })
        })?;

        let mut files = Vec::new();
        for meta in metas_iter {
            files.push(meta?);
        }
        Ok(files)
    }

    #[cfg(test)]
    fn current_schema_version(&self) -> Option<i64> {
        self.conn
            .query_row(
                "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1",
                [],
                |r| r.get(0),
            )
            .ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_fresh_db_is_version_2() {
        let db = SyncDb::in_memory().unwrap();
        assert_eq!(db.current_schema_version(), Some(2));
    }

    #[test]
    fn test_migration_removes_md5_rows_preserves_sha256() {
        let tmp = NamedTempFile::new().unwrap();

        // Pre-populate the DB without a schema_version table (simulates an old v1 DB).
        {
            let conn = Connection::open(tmp.path()).unwrap();
            conn.execute(
                "CREATE TABLE IF NOT EXISTS base_state (
                    key TEXT PRIMARY KEY,
                    size INTEGER NOT NULL,
                    modified_ts INTEGER NOT NULL,
                    checksum TEXT NOT NULL
                )",
                [],
            )
            .unwrap();
            // MD5 checksum (32 chars) — must be deleted by migration
            conn.execute(
                "INSERT INTO base_state VALUES ('old.txt', 100, 1000, 'd41d8cd98f00b204e9800998ecf8427e')",
                [],
            )
            .unwrap();
            // Valid SHA-256 checksum (64 lowercase hex chars) — must survive migration
            conn.execute(
                "INSERT INTO base_state VALUES ('new.txt', 200, 2000, 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855')",
                [],
            )
            .unwrap();
        }

        // Open via SyncDb — triggers the v2 migration.
        let db = SyncDb::new(tmp.path()).unwrap();
        let files = db.get_all_files().unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].key, "new.txt");
        assert_eq!(db.current_schema_version(), Some(2));
    }
}
