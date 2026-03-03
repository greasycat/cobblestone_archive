#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMeta {
    pub key: String, // Relative path, S3 object key format using '/'
    pub size: u64,
    pub modified_ts: u64, // Unix timestamp in seconds
    pub checksum: String, // SHA-256 lowercase hex (64 chars), or "etag:<hex>" for external uploads
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncAction {
    Upload(FileMeta),
    Download(FileMeta),
    DeleteLocal(String),
    DeleteRemote(String),
    /// Both local and remote changed with different content.
    /// The executor downloads the remote copy to a companion ".remote" file
    /// and asks the user how to resolve.
    Conflict {
        key: String,
        local: FileMeta,
        remote: FileMeta,
    },
    SkipConflict(String), // Path of the conflict
    UpToDate(String),
}
