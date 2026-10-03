//! Read a frame from a ZIP stored in S3.
//!
//! Usage: cargo run -p timsrust --features zip --example zip_s3 -- \
//!   BUCKET KEY MEMBER_PREFIX FRAME_ID
//! The binary member must use ZIP's Stored compression method.

use std::error::Error;
use std::sync::Arc;

use object_store::{
    ObjectStore, aws::AmazonS3Builder, path::Path as ObjectPath,
};
use timsrust::core::io::{
    formats::binary::ObjectStoreReadAt, zip_archive::ZipArchive,
};
use timsrust::tdf::TdfFrameReader;

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 5 {
        return Err("usage: zip_s3 BUCKET KEY MEMBER_PREFIX FRAME_ID".into());
    }
    let store: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::from_env()
            .with_bucket_name(&args[1])
            .build()?,
    );
    let object =
        ObjectStoreReadAt::new(store, ObjectPath::from(args[2].as_str()))?;
    let mut archive = ZipArchive::open(Box::new(object))?;
    let scratch = tempfile::tempdir()?;
    let sqlite_path = scratch.path().join("analysis.tdf");
    let prefix = args[3].trim_end_matches('/');
    archive.extract_to(&format!("{prefix}/analysis.tdf"), &sqlite_path)?;
    let binary = archive.member(&format!("{prefix}/analysis.tdf_bin"))?;
    let reader = TdfFrameReader::from_parts(sqlite_path, Box::new(binary))?;
    let frame_id = args[4].parse()?;
    let frame = reader.get_frame(frame_id)?;
    println!("{frame:?}");
    Ok(())
}
