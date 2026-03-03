use crate::core::models::FileMeta;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::{self, Read};
use std::path::Path;

/// Computes the SHA-256 hex checksum of a file.
/// Reads the file in chunks to keep memory usage low for large files.
pub fn compute_sha256(path: &Path) -> io::Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 8192];

    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }

    let result = hasher.finalize();
    Ok(format!("{:x}", result))
}

/// Recursively scans a directory and computes FileMeta for all files.
/// `base_dir` is the root where the scan starts, used to calculate relative paths (keys).
pub fn scan_local_dir(base_dir: &Path) -> io::Result<Vec<FileMeta>> {
    let mut results = Vec::new();
    let mut dirs_to_visit = vec![base_dir.to_path_buf()];

    while let Some(dir) = dirs_to_visit.pop() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                dirs_to_visit.push(path);
            } else if path.is_file() {
                // Skip the local sync database — it is not a user file.
                if path.file_name().and_then(|n| n.to_str()) == Some(".s3sync.db") {
                    continue;
                }

                let meta = entry.metadata()?;
                let size = meta.len();

                // Get modified time in seconds
                let modified_ts = meta
                    .modified()?
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);

                // Compute relative key using standard S3 formats (forward slashes)
                let relative_path = path
                    .strip_prefix(base_dir)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

                let key = relative_path
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy().to_string())
                    .collect::<Vec<_>>()
                    .join("/"); // S3 mandates '/'

                let checksum = compute_sha256(&path)?;

                results.push(FileMeta {
                    key,
                    size,
                    modified_ts,
                    checksum,
                });
            }
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_sha256_empty_file() {
        let f = NamedTempFile::new().unwrap();
        let hash = compute_sha256(f.path()).unwrap();
        // SHA-256 of empty string
        assert_eq!(
            hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_sha256_hello() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"hello").unwrap();
        f.flush().unwrap();
        let hash = compute_sha256(f.path()).unwrap();
        // SHA-256 of "hello"
        assert_eq!(
            hash,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }
}
