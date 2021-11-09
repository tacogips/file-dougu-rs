use crate::compression::*;

use crate::mime;
use crate::mime::MimeType;
use backoff::future::retry;
use backoff::{Error as BackoffError, ExponentialBackoff};
use cloud_storage::bucket::{Location, MultiRegion};
use cloud_storage::{Bucket, Error as CloudStorageError, ListRequest, NewBucket, Object};
use futures::future;
use futures::stream::TryStreamExt;
use futures_util::future::TryFutureExt;
use lazy_static::lazy_static;
use log;
use regex::Regex;
use std::convert::Into;
use std::fmt;
use thiserror::Error;
use url::Url;

#[derive(Error, Debug)]
pub enum FileUtilGcsError {
    #[error("gcs buclet path error: {0}")]
    GcsInvalidBucketPathError(String),

    #[error("url parse error: {0}")]
    UrlParseError(#[from] url::ParseError),

    #[error("srtorage acess error: {0}")]
    StorageAccessError(#[from] CloudStorageError),

    #[error("invalid gcs url: {0}")]
    InvalidGcsUrl(String),

    #[error("compression error: {0}")]
    CompressionError(#[from] CompressionError),
}
pub type Result<T> = std::result::Result<T, FileUtilGcsError>;

lazy_static! {
    static ref GCS_BUCKET_RE: Regex = Regex::new(r"gs://(?P<bucket>.*?)/(?P<name>.*)").unwrap();
}

#[derive(Debug, PartialEq)]
pub struct GcsFile {
    bucket: String,
    name: String,
}

impl GcsFile {
    fn parse_bucket_and_name_from_url(url: &Url) -> Result<(String, String)> {
        GCS_BUCKET_RE.captures(url.as_str()).map_or(
            Err(FileUtilGcsError::GcsInvalidBucketPathError(
                url.as_str().to_string(),
            )),
            |captured| {
                let bucket = captured["bucket"].to_string();
                let name = captured["name"].to_string();
                if bucket.is_empty()
                    || name.is_empty()
                    || name.starts_with("/")
                    || name.ends_with("/")
                {
                    Err(FileUtilGcsError::InvalidGcsUrl(url.as_str().to_string()))
                } else {
                    Ok((bucket, name))
                }
            },
        )
    }

    pub fn new(maybe_url_string: String) -> Result<Self> {
        let url = Url::parse(maybe_url_string.as_str())?;
        Self::new_with_url(&url)
    }

    pub async fn list_objects_with_retry(
        &self,
        backoff: Option<ExponentialBackoff>,
    ) -> Result<Vec<String>> {
        retry(backoff.unwrap_or_default(), || async {
            let objects = match list_objects(&self.bucket, &self.name).await {
                Ok(objects) => objects,
                Err(e) => {
                    log::warn!("list object failed {}", e);
                    return Err(BackoffError::Transient(e));
                }
            };

            Ok(objects
                .into_iter()
                .map(|obj| {
                    Self {
                        bucket: obj.bucket,
                        name: obj.name,
                    }
                    .to_string()
                })
                .collect())
        })
        .await
    }

    pub fn new_with_url(url: &Url) -> Result<Self> {
        let url_str = url.as_str();

        if !url_str.starts_with("gs://") {
            return Err(FileUtilGcsError::GcsInvalidBucketPathError(format!(
                "is not a valid gs address  {}",
                url_str
            )));
        }
        let (bucket, name) = Self::parse_bucket_and_name_from_url(url)?;

        Ok(Self { bucket, name })
    }

    pub async fn is_exists(bucket: &str, name: &str) -> Result<bool> {
        object_exists(bucket, name).await
    }

    pub async fn is_exists_with_retry(&self, backoff: Option<ExponentialBackoff>) -> Result<bool> {
        retry(backoff.unwrap_or_default(), || async {
            match Self::is_exists(&self.bucket, &self.name).await {
                Ok(v) => Ok(v),
                Err(e) => {
                    log::warn!(
                        "exists Retring. [{}/{}] error:{:?}",
                        self.bucket,
                        self.name,
                        e
                    );
                    Err(BackoffError::Transient(e))
                }
            }
        })
        .await
    }

    pub async fn download(bucket: &str, name: &str) -> Result<Option<Vec<u8>>> {
        if let Ok(true) = object_exists(bucket, name).await {
            download_object(&bucket, &name).await.map(|body| Some(body))
        } else {
            Ok(None)
        }
    }

    pub async fn download_with_retry(
        &self,
        backoff: Option<ExponentialBackoff>,
        decompression: Option<Compression>,
    ) -> Result<Option<Vec<u8>>> {
        let contents: Option<Vec<u8>> = retry(backoff.unwrap_or_default(), || async {
            match GcsFile::download(&self.bucket, &self.name).await {
                Ok(v) => Ok(v),
                Err(e) => {
                    log::warn!(
                        "download from gcs failed. Retring. [{}/{}] error:{:?}",
                        self.bucket,
                        self.name,
                        e
                    );
                    Err(BackoffError::Transient(e))
                }
            }
        })
        .await?;
        let result = decompress_opt(contents, decompression)?;
        Ok(result)
    }

    pub async fn write_with_retry(
        &self,
        body: &[u8],
        mime_type: mime::MimeType,
        backoff: Option<ExponentialBackoff>,
        compression: Option<Compression>,
    ) -> Result<()> {
        let body = compress_opt(body, compression)?;

        retry(backoff.unwrap_or_default(), || async {
            create_object(&self.bucket, &self.name, body.to_vec(), mime_type.clone())
                .await
                .map(|_| ())
                .map_err(|e| {
                    log::warn!("gcs write error {:?}", e);
                    BackoffError::Transient(e)
                })
        })
        .await
    }

    pub async fn delete_with_retry(&self, backoff: Option<ExponentialBackoff>) -> Result<()> {
        retry(backoff.unwrap_or_default(), || async {
            delete_object(&self.bucket, &self.name)
                .await
                .map(|_| ())
                .map_err(BackoffError::Transient)
        })
        .await
    }
}

impl fmt::Display for GcsFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "gs://{}/{}", self.bucket, self.name)
    }
}

pub async fn object_exists(bucket: &str, path: &str) -> Result<bool> {
    find_object(bucket, path)
        .await
        .and_then(|object_if_found| Ok(object_if_found.is_some()))
}

fn list_prefix_request(prefix: String) -> ListRequest {
    ListRequest {
        /// When specified, allows the `list` to operate like a directory listing by splitting the
        /// object location on this delimiter.
        delimiter: None,

        /// Filter results to objects whose names are lexicographically before `end_offset`.
        /// If `start_offset` is also set, the objects listed have names between `start_offset`
        /// (inclusive) and `end_offset` (exclusive).
        end_offset: None,

        /// If true, objects that end in exactly one instance of `delimiter` have their metadata
        /// included in `items` in addition to the relevant part of the object name appearing in
        /// `prefixes`.
        include_trailing_delimiter: None,

        /// Maximum combined number of entries in `items` and `prefixes` to return in a single
        /// page of responses. Because duplicate entries in `prefixes` are omitted, fewer total
        /// results may be returned than requested. The service uses this parameter or 1,000
        /// items, whichever is smaller.
        max_results: None,

        /// A previously-returned page token representing part of the larger set of results to view.
        /// The `page_token` is an encoded field that marks the name and generation of the last object
        /// in the returned list. In a subsequent request using the `page_token`, items that come after
        /// the `page_token` are shown (up to `max_results`).
        ///
        /// If the page token is provided, all objects starting at that page token are queried
        page_token: None,

        /// Filter results to include only objects whose names begin with this prefix.
        prefix: Some(prefix),

        /// Set of properties to return. Defaults to `NoAcl`.
        projection: None,

        /// Filter results to objects whose names are lexicographically equal to or after
        /// `start_offset`. If `end_offset` is also set, the objects listed have names between
        /// `start_offset` (inclusive) and `end_offset` (exclusive).
        start_offset: None,

        /// If true, lists all versions of an object as distinct results in order of increasing
        /// generation number. The default value for versions is false. For more information, see
        /// Object Versioning.
        versions: None,
    }
}

pub async fn find_object(bucket: &str, name: &str) -> Result<Option<Object>> {
    if name.ends_with("/") {
        return Err(FileUtilGcsError::GcsInvalidBucketPathError(format!(
            "path must not be ends with `/` : {}",
            name
        )));
    }

    //TODO(tacogips) it's unsafficient to use `await` for performance
    let object_chunks = Object::list(bucket, list_prefix_request(name.to_string()))
        .and_then(|objs_stream| objs_stream.try_collect::<Vec<_>>())
        .await?;
    for each_objs in object_chunks.into_iter() {
        let found = each_objs
            .items
            .into_iter()
            .find(|each_obj| each_obj.name == name);
        if found.is_some() {
            return Ok(found);
        }
    }
    Ok(None)
}

pub async fn list_objects(bucket: &str, name: &str) -> Result<Vec<Object>> {
    if name.ends_with("/") {
        return Err(FileUtilGcsError::GcsInvalidBucketPathError(format!(
            "path must not be ends with `/` : {}",
            name
        )));
    }

    //TODO(tacogips) it's unsafficient to use `await` for performance
    let object_chunks = Object::list(bucket, list_prefix_request(name.to_string()))
        .and_then(|objs_stream| objs_stream.try_collect::<Vec<_>>())
        .await?;

    let mut result = Vec::<Object>::new();
    for mut each_objs_list in object_chunks.into_iter() {
        result.append(&mut each_objs_list.items);
    }
    Ok(result)
}

pub async fn download_object(bucket: &str, name: &str) -> Result<Vec<u8>> {
    if name.ends_with("/") {
        return Err(FileUtilGcsError::GcsInvalidBucketPathError(format!(
            "path must not be ends with `/` : {}",
            name
        )));
    }

    let result = Object::download(bucket, name).await?;
    Ok(result)
}

pub async fn create_object(
    bucket: &str,
    path: &str,
    body: Vec<u8>,
    mime_type: MimeType,
) -> Result<Object> {
    let object = Object::create(bucket, body, path, mime_type.into()).await?;
    Ok(object)
}

pub async fn delete_object(bucket: &str, path: &str) -> Result<()> {
    Object::delete(bucket, path).await?;
    Ok(())
}

pub async fn create_bucket(bucket: &str) -> Result<Bucket> {
    let new_bucket = NewBucket {
        name: bucket.to_owned(), // this is the only mandatory field
        location: Location::Multi(MultiRegion::Asia),
        ..Default::default()
    };

    let bucket = Bucket::create(&new_bucket).await?;
    Ok(bucket)
}

pub async fn bucket_exists(bucket: &str) -> bool {
    let a = find_bucket(bucket)
        .and_then(|found_or_not| future::ok(found_or_not.is_some()))
        .await;
    a.unwrap_or_else(|e| {
        log::warn!("bucket exists error {} {}", bucket, e);
        false
    })
}

pub async fn find_bucket(bucket: &str) -> Result<Option<Bucket>> {
    let buckets = Bucket::list().await?;
    Ok(buckets
        .into_iter()
        .find(|each_bucket| each_bucket.name == bucket))
}

/// cloud-storage.rs has a problem with the global reqwest Client
/// that cause `dispatch dropped without returning error` error.
/// https://github.com/hyperium/hyper/issues/2112
/// We use Mutex lock to let only single test run to avoid it.
#[cfg(test)]
mod tests {

    use super::GcsFile;
    use lazy_static::lazy_static;
    use std::env;
    use std::sync::Mutex;
    use tokio;
    use url::Url;
    use uuid::Uuid;

    macro_rules! env_value {
        ($env_key:expr) => {
            env::var($env_key).expect(&format!("env {} not found.", $env_key))
        };
    }
    const FILE_UTIL_TEST_BUCKET_NAME_ENV: &str = "FILE_UTIL_TEST_GCS_BUCKET_ENV";

    fn test_bucket_name() -> String {
        env_value!(FILE_UTIL_TEST_BUCKET_NAME_ENV)
    }

    lazy_static! {
        static ref TEST_BUCKET_MUTEX: Mutex<()> = Mutex::new(());
    }

    #[cfg(feature = "cloud_test")]
    #[tokio::test]
    async fn bucket_exists() {
        let _lock = TEST_BUCKET_MUTEX.lock();

        let actual = super::bucket_exists(&test_bucket_name()).await;
        assert_eq!(true, actual)
    }

    #[cfg(feature = "cloud_test")]
    #[tokio::test]
    async fn object_exists_create_delete() {
        let _lock = TEST_BUCKET_MUTEX.lock();
        let test_objects_name = format!("file_manager_test/{}/test_file", Uuid::new_v4().to_urn());

        assert_eq!(
            false,
            super::object_exists(&test_bucket_name(), &test_objects_name)
                .await
                .unwrap(),
            "assert file is not exists yet.",
        );

        let body_str = String::from("this is a test &&%2f %;[[;!");
        assert_eq!(
            true,
            super::create_object(
                &test_bucket_name(),
                &test_objects_name,
                body_str.into_bytes(),
                super::MimeType::OctetStream
            )
            .await
            .is_ok(),
            "file is created without error",
        );

        assert_eq!(
            true,
            super::object_exists(&test_bucket_name(), &test_objects_name)
                .await
                .unwrap(),
            "find the created file ",
        );

        //TODO(tacogips) test to get contents here

        assert_eq!(
            true,
            super::delete_object(&test_bucket_name(), &test_objects_name)
                .await
                .is_ok(),
            "remove created file ",
        );
    }

    #[test]
    fn parse_gcs_file() {
        let url = Url::parse("gs://zdb_test/zdb").unwrap();
        let result = GcsFile::new_with_url(&url);

        assert!(result.is_ok());
        let result = result.unwrap();

        assert_eq!(
            result,
            GcsFile {
                bucket: "zdb_test".to_string(),
                name: "zdb".to_string(),
            }
        );
    }
}
