extern crate s3;
use color_eyre::Result;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::Region;
use std::{error::Error, time::Instant};
use url::Url;
use crate::data::source_model::File;
use std::time::Duration;
use tracing::{info, debug};

pub async fn read_file(region: &str, file: String) -> Result<File, Box<dyn Error + Sync + Send>> {
    info!("Reading file: {}", file);
    let now = Instant::now();
    let credentials = Credentials::from_env()?;

    debug!("Credentials: {:?}", credentials);

    let region: Region =  region.parse()?;

    let file_struct: Url = Url::parse(&file)?;

    debug!("file structure: {:?}", file_struct);

    let bucket = file_struct.host_str().unwrap();
    debug!("Bucket {}, Region {:?}", bucket, region);

    let mut bucket = Bucket::new(bucket, region, credentials)?;

    bucket.set_request_timeout(Some(Duration::new(290, 0)));

    let elapsed_b = now.elapsed();
    info!("bucket creation. Took {:.2?}", elapsed_b);
    
    let path = file_struct.path();
    debug!("path: {:?}", path);

    let data = bucket.get_object(path).await?;

    debug!("file code: {:?}", data.status_code());

    let file: File = serde_json::from_slice(&data.bytes())?;

    let elapsed = now.elapsed();
    info!("read_file. Took {:.2?}", elapsed);

    Ok(file)
}
