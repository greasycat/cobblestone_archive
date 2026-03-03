/// Integration test for S3 connectivity.
///
/// Run with real credentials after sourcing .env:
///   source .env && cargo test s3_connectivity -- --ignored
#[tokio::test]
#[ignore = "requires live AWS credentials (source .env first)"]
async fn s3_connectivity() {
    let bucket = std::env::var("S3_BUCKET_NAME")
        .expect("S3_BUCKET_NAME must be set (source .env first)");

    let client = cobblestone::aws::s3_client::S3Client::new(&bucket, "")
        .await
        .expect("failed to create S3Client");

    let objects = client
        .list_all_objects()
        .await
        .expect("list_all_objects failed — check credentials and bucket name");

    println!(
        "Connected to bucket '{}'. Found {} object(s).",
        bucket,
        objects.len()
    );
}
