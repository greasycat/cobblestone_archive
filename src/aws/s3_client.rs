use crate::core::models::FileMeta;
use aws_sdk_s3::Client;
use aws_sdk_s3::types::{ChecksumAlgorithm, ObjectAttributes};
use base64::{Engine as _, engine::general_purpose};
use std::error::Error;

pub struct S3Client {
    client: Client,
    bucket: String,
    prefix: String,
}

impl S3Client {
    /// Initialize a new S3 client using the default AWS configuration chain.
    pub async fn new(bucket: &str, prefix: &str) -> Result<Self, Box<dyn Error>> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = Client::new(&config);

        Ok(Self {
            client,
            bucket: bucket.to_string(),
            prefix: prefix.to_string(),
        })
    }

    /// Decodes a base64 string to a lowercase hex string.
    fn base64_to_hex(b64: &str) -> Result<String, Box<dyn Error>> {
        let bytes = general_purpose::STANDARD.decode(b64)?;
        Ok(bytes.iter().map(|b| format!("{:02x}", b)).collect())
    }

    /// Encodes a lowercase hex string to a base64 string.
    #[allow(dead_code)]
    fn hex_to_base64(hex: &str) -> Result<String, Box<dyn Error>> {
        let bytes: Result<Vec<u8>, _> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
            .collect();
        Ok(general_purpose::STANDARD.encode(bytes?))
    }

    /// Retrieves the SHA-256 checksum (as lowercase hex) of an S3 object via GetObjectAttributes.
    /// Returns None if no SHA-256 checksum is stored (e.g., externally uploaded objects).
    async fn get_object_sha256(&self, full_key: &str) -> Result<Option<String>, Box<dyn Error>> {
        let resp = self
            .client
            .get_object_attributes()
            .bucket(&self.bucket)
            .key(full_key)
            .object_attributes(ObjectAttributes::Checksum)
            .send()
            .await?;

        let hex = resp
            .checksum()
            .and_then(|c| c.checksum_sha256())
            .map(|b64| Self::base64_to_hex(b64))
            .transpose()?;

        Ok(hex)
    }

    /// Fetches all objects in the bucket/prefix and returns their FileMeta.
    /// Handles pagination automatically.
    pub async fn list_all_objects(&self) -> Result<Vec<FileMeta>, Box<dyn Error>> {
        let mut results = Vec::new();
        let mut continuation_token = None;

        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&self.prefix);

            if let Some(token) = continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req.send().await?;

            for obj in resp.contents() {
                let key = obj.key().unwrap_or("").to_string();

                // Strip the base prefix for the relative key
                let relative_key = if !self.prefix.is_empty() && key.starts_with(&self.prefix) {
                    let stripped = &key[self.prefix.len()..];
                    // Remove leading slash if it exists after stripping prefix
                    stripped.trim_start_matches('/').to_string()
                } else {
                    key
                };

                if relative_key.is_empty() || relative_key.ends_with('/') {
                    // Skip folder placeholder objects
                    continue;
                }

                let full_key = self.full_key(&relative_key);

                // ETag from S3 is double-quoted, e.g., "\"hash\"". Strip quotes.
                let etag_stripped = obj
                    .e_tag()
                    .map(|s| s.trim_matches('"').to_string())
                    .unwrap_or_default();

                // Retrieve SHA-256 checksum via GetObjectAttributes.
                // Fall back to "etag:<hash>" sentinel for externally uploaded objects without a stored checksum.
                let checksum = match self.get_object_sha256(&full_key).await? {
                    Some(hex) => hex,
                    None => format!("etag:{}", etag_stripped),
                };

                let size = obj.size().unwrap_or(0) as u64;

                // AWS timestamp is in seconds since epoch + nanos, we just want seconds
                let modified_ts = match obj.last_modified() {
                    Some(date) => date.secs() as u64,
                    None => 0,
                };

                results.push(FileMeta {
                    key: relative_key,
                    size,
                    modified_ts,
                    checksum,
                });
            }

            if resp.is_truncated().unwrap_or(false) {
                continuation_token = resp.next_continuation_token().map(String::from);
            } else {
                break;
            }
        }

        Ok(results)
    }

    /// Helper to get the full S3 object key including the configured prefix
    fn full_key(&self, relative_key: &str) -> String {
        if self.prefix.is_empty() {
            relative_key.to_string()
        } else {
            let prefix = self.prefix.trim_end_matches('/');
            format!("{}/{}", prefix, relative_key)
        }
    }

    /// Downloads an object from S3 and saves it to a local path.
    pub async fn download_object(
        &self,
        relative_key: &str,
        dest_path: &std::path::Path,
    ) -> Result<(), Box<dyn Error>> {
        use tokio::fs::{File, create_dir_all};
        use tokio::io::AsyncWriteExt;

        let full_key = self.full_key(relative_key);

        if let Some(parent) = dest_path.parent() {
            create_dir_all(parent).await?;
        }

        let mut resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await?;

        let mut file = File::create(dest_path).await?;

        while let Some(bytes) = resp.body.try_next().await? {
            file.write_all(&bytes).await?;
        }

        file.flush().await?;
        Ok(())
    }

    /// Uploads a local file to S3 with SHA-256 checksum validation.
    /// `sha256_hex` is the pre-computed SHA-256 of the file (used by the caller to update the DB
    /// without re-reading the file). The SDK computes the checksum independently for S3 storage.
    pub async fn upload_object(
        &self,
        relative_key: &str,
        source_path: &std::path::Path,
        _sha256_hex: &str,
    ) -> Result<(), Box<dyn Error>> {
        use aws_sdk_s3::primitives::ByteStream;

        let full_key = self.full_key(relative_key);
        let body = ByteStream::from_path(source_path).await?;

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .body(body)
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .send()
            .await?;

        Ok(())
    }

    /// Deletes an object from S3.
    pub async fn delete_object(&self, relative_key: &str) -> Result<(), Box<dyn Error>> {
        let full_key = self.full_key(relative_key);

        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&full_key)
            .send()
            .await?;

        Ok(())
    }
}
