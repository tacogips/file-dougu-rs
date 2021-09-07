use crate::compression::*;
use backoff::future::retry;
use backoff::{Error as BackoffError, ExponentialBackoff};
use http::StatusCode;
use lazy_static::lazy_static;
use reqwest;
use std::time::Duration;
use thiserror::Error;
use url::Url;

#[derive(Error, Debug)]
pub enum FileUtilWebError {
    #[error("http access error: {0}")]
    HttpAccessError(#[from] reqwest::Error),

    #[error("compression error: {0}")]
    CompressionError(#[from] CompressionError),
}
pub type Result<T> = std::result::Result<T, FileUtilWebError>;

lazy_static! {
    static ref HTTP_CLI: reqwest::Client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
}

pub async fn url_exists_with_retry(url: Url, backoff: Option<ExponentialBackoff>) -> Result<bool> {
    retry(backoff.unwrap_or(ExponentialBackoff::default()), || async {
        match HTTP_CLI.get(url.clone()).send().await {
            Ok(response) => {
                if response.status().is_success() {
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Err(e) => Err(BackoffError::Transient(FileUtilWebError::HttpAccessError(
                e,
            ))),
        }
    })
    .await
}

pub async fn download_from_url_with_retry(
    url: Url,
    backoff: Option<ExponentialBackoff>,
    decompression: Option<Compression>,
) -> Result<Option<Vec<u8>>> {
    let contents = retry(backoff.unwrap_or(ExponentialBackoff::default()), || async {
        let result = HTTP_CLI.get(url.clone()).send().await;

        let bytes = match result {
            Ok(bytes) => bytes,
            Err(err) => {
                if let Some(status) = err.status() {
                    if StatusCode::NOT_FOUND == status {
                        return Ok(None);
                    }
                }
                return Err(BackoffError::Transient(FileUtilWebError::HttpAccessError(
                    err,
                )));
            }
        };

        match bytes.bytes().await {
            Ok(bytes) => Ok(Some(bytes.as_ref().to_vec())),
            Err(e) => Err(BackoffError::Transient(FileUtilWebError::HttpAccessError(
                e,
            ))),
        }
    })
    .await?;

    let result = decompress_opt(contents, decompression)?;
    Ok(result)
}
