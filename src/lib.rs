#[cfg(feature = "fs")]
pub mod fs;
#[cfg(feature = "gcs")]
pub mod gcs;
#[cfg(feature = "web")]
pub mod web;

pub mod compression;
pub mod mime;

use backoff::ExponentialBackoff;
use compression::*;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FileUtilError {
    #[error("gcs error: {0}")]
    GcsError(#[from] gcs::FileUtilGcsError),

    #[error("web error: {0}")]
    WebError(#[from] web::FileUtilWebError),

    #[error("fs error: {0}")]
    FsError(#[from] fs::FileUtilFsError),
}

pub type Result<T> = std::result::Result<T, FileUtilError>;
use url::Url;

pub async fn list_files(
    url_or_path_str: &str,
    backoff: Option<ExponentialBackoff>,
) -> Result<Vec<String>> {
    #[cfg(any(feature = "gcs", feature = "web"))]
    if let Ok(url) = Url::parse(url_or_path_str.as_ref()) {
        #[cfg(feature = "gcs")]
        if let Ok(gcs_file) = gcs::GcsFile::new_with_url(&url) {
            let gcs_data = gcs_file.list_objects_with_retry(backoff).await?;
            return Ok(gcs_data);
        }

        #[cfg(feature = "web")]
        {
            unimplemented!(
                "listing directories under a url is not implemented yet. {}",
                url_or_path_str
            )
        }
    };

    #[cfg(feature = "fs")]
    {
        let local_file = fs::FileAccessor::new(url_or_path_str.into())?;
        let result = local_file.list_directory()?;
        Ok(result)
    }
}

pub async fn get_file_contents_str(
    url_or_path_str: &str,
    backoff: Option<ExponentialBackoff>,
    decompression: Option<Compression>,
) -> Result<Option<String>> {
    match get_file_contents(url_or_path_str, backoff, decompression).await {
        Err(e) => Err(e),
        Ok(c) => Ok(c.map(|contents| std::str::from_utf8(contents.as_ref()).unwrap().to_string())),
    }
}

pub async fn get_file_contents(
    url_or_path_str: &str,
    backoff: Option<ExponentialBackoff>,
    decompression: Option<Compression>,
) -> Result<Option<Vec<u8>>> {
    #[cfg(any(feature = "gcs", feature = "web"))]
    if let Ok(url) = Url::parse(url_or_path_str.as_ref()) {
        #[cfg(feature = "gcs")]
        if let Ok(gcs_file) = gcs::GcsFile::new_with_url(&url) {
            let gcs_data = gcs_file
                .download_with_retry(backoff, decompression.clone())
                .await?;
            return Ok(gcs_data);
        }

        #[cfg(feature = "web")]
        {
            let web_data = web::download_from_url_with_retry(url, backoff, decompression).await?;
            return Ok(web_data);
        }
    };

    #[cfg(feature = "fs")]
    {
        let local_file = fs::FileAccessor::new(url_or_path_str.into())?;
        let result = local_file.read()?;
        Ok(result)
    }
}

pub async fn is_exists(url_or_path_str: &str, backoff: Option<ExponentialBackoff>) -> Result<bool> {
    #[cfg(any(feature = "gcs", feature = "web"))]
    if let Ok(url) = Url::parse(url_or_path_str.as_ref()) {
        #[cfg(feature = "gcs")]
        if let Ok(gcs_file) = gcs::GcsFile::new_with_url(&url) {
            let gcs_data = gcs_file.is_exists_with_retry(backoff).await?;
            return Ok(gcs_data);
        }

        #[cfg(feature = "web")]
        {
            let web_data = web::url_exists_with_retry(url, backoff).await?;
            return Ok(web_data);
        }
    };

    #[cfg(feature = "fs")]
    {
        let local_file = fs::FileAccessor::new(url_or_path_str.into())?;
        let result = local_file.is_exists()?;
        Ok(result)
    }
}

pub async fn write_contents<'a>(
    url_or_path_str: &'a str,
    body: Vec<u8>,
    mime_type: mime::MimeType,
    backoff: Option<ExponentialBackoff>,
    compression: Option<compression::Compression>,
) -> Result<()> {
    #[cfg(any(feature = "gcs", feature = "web"))]
    if let Ok(url) = Url::parse(url_or_path_str.as_ref()) {
        #[cfg(feature = "gcs")]
        if let Ok(gcs_file) = gcs::GcsFile::new_with_url(&url) {
            gcs_file
                .write_with_retry(body, mime_type, backoff, compression)
                .await?;
            return Ok(());
        }

        #[cfg(feature = "web")]
        {
            unimplemented!("writing at url is not implemented yet. {}", url_or_path_str)
        }
    };

    #[cfg(feature = "fs")]
    {
        let local_file = fs::FileAccessor::new(url_or_path_str.into())?;
        local_file.write(body, compression)?;
        Ok(())
    }
}

pub async fn delete_contents(
    url_or_path_str: &str,
    backoff: Option<ExponentialBackoff>,
) -> Result<()> {
    #[cfg(any(feature = "gcs", feature = "web"))]
    if let Ok(url) = Url::parse(url_or_path_str.as_ref()) {
        #[cfg(feature = "gcs")]
        if let Ok(gcs_file) = gcs::GcsFile::new_with_url(&url) {
            gcs_file.delete_with_retry(backoff).await?;
            return Ok(());
        }

        #[cfg(feature = "web")]
        {
            unimplemented!("deleting url is not implemented yet. {}", url_or_path_str)
        }
    };

    #[cfg(feature = "fs")]
    {
        let local_file = fs::FileAccessor::new(url_or_path_str.into())?;
        local_file.delete()?;
        Ok(())
    }
}
