use std::path::PathBuf;

use lazy_static::lazy_static;

use anyhow::{anyhow, Error, Result};
use backoff::future::retry;
use backoff::{Error as BackoffError, ExponentialBackoff};
use regex::Regex;
use std::fs::{self, DirEntry};
use url::Url;

use http::StatusCode;

mod gcs;
pub mod mime;
use reqwest;
mod gzip;
use std::time::Duration;

lazy_static! {
    static ref HTTP_CLI: reqwest::Client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    static ref BUCKET_RE: Regex = Regex::new(r"gs://(?P<bucket>.*?)/(?P<name>.*)").unwrap();
}

pub struct LocalFile {
    path: PathBuf,
}

impl LocalFile {
    pub fn new(file_path: PathBuf) -> Result<Self> {
        return Ok(LocalFile { path: file_path });
    }
}

impl LocalFile {
    pub fn is_exists(&self) -> Result<bool> {
        Ok(self.path.exists())
    }
    pub fn read(&self) -> Result<Option<Vec<u8>>> {
        match Self::is_exists(&self) {
            Ok(true) => fs::read(&self.path)
                .map(|body| Some(body))
                .map_err(Error::msg),
            Ok(false) => Ok(None),
            Err(e) => Err(e),
        }
    }
    pub fn write<C: AsRef<[u8]>>(&self, body: C) -> Result<()> {
        fs::write(&self.path, body).map_err(Error::msg)
    }

    pub fn list_directory(&self) -> Result<Vec<String>> {
        let mut dirs = Vec::<String>::new();
        for entry in fs::read_dir(self.path.as_path().as_os_str())? {
            let entry = entry?;
            dirs.push(entry.path().display().to_string());
        }
        Ok(dirs)
    }

    pub fn delete(&self) -> Result<()> {
        unimplemented!("localfile deletion is not implemented yet");
    }
}

pub struct GcsFile {
    bucket: String,
    name: String,
}

impl GcsFile {
    fn parse_bucket_and_name_from_url(url: &Url) -> Result<(String, String)> {
        BUCKET_RE.captures(url.as_str()).map_or(
            Err(anyhow!("invalid gcs path: {}", url.as_str())),
            |captured| {
                let bucket = captured["bucket"].to_string();
                let name = captured["name"].to_string();
                if bucket.is_empty()
                    || name.is_empty()
                    || name.starts_with("/")
                    || name.ends_with("/")
                {
                    Err(anyhow!(
                        "not bucket and name found in url : {}",
                        url.as_str()
                    ))
                } else {
                    Ok((bucket, name))
                }
            },
        )
    }

    pub fn new(maybe_url_string: String) -> Result<Self> {
        let url = Url::parse(maybe_url_string.as_str())
            .map_err(|_| anyhow!("it's not valid gcs url {}", maybe_url_string))?;
        Self::new_with_url(&url)
    }

    pub fn into_string(&self) -> String {
        format!("gs://{}/{}", self.bucket, self.name)
    }

    pub async fn list_objects(&self) -> Result<Vec<Self>> {
        let objects = gcs::list_objects(&self.bucket, &self.name).await?;
        Ok(objects
            .into_iter()
            .map(|obj| Self {
                bucket: obj.bucket,
                name: obj.name,
            })
            .collect())
    }

    pub fn new_with_url(url: &Url) -> Result<Self> {
        let url_str = url.as_str();
        if !url_str.starts_with("gs://") {
            return Err(anyhow!("it's not gs address {}", url_str));
        }

        //TODO(tacogips) actually needed?
        if url_str.ends_with("/") {
            return Err(anyhow!("gcs path must not be ends with '/' {}", url_str));
        }

        let (bucket, name) = Self::parse_bucket_and_name_from_url(url)?;

        Ok(Self { bucket, name })
    }

    pub async fn is_exists(&self) -> Result<bool> {
        gcs::object_exists(&self.bucket, &self.name).await
    }

    pub async fn download(&self) -> Result<Option<Vec<u8>>> {
        if let Ok(true) = gcs::object_exists(&self.bucket, &self.name).await {
            gcs::download_object(&self.bucket, &self.name)
                .await
                .map(|body| Some(body))
        } else {
            Ok(None)
        }
    }

    pub async fn write<C: AsRef<[u8]>>(&self, body: C, mime_type: mime::MimeType) -> Result<()> {
        retry(ExponentialBackoff::default(), || async {
            gcs::create_object(
                &self.bucket,
                &self.name,
                body.as_ref().to_vec(),
                mime_type.clone(),
            )
            .await
            .map(|_| ())
            .map_err(|e| {
                log::warn!("gcs write error {:?}", e);
                BackoffError::Transient(e)
            })
        })
        .await
    }

    pub async fn delete(&self) -> Result<()> {
        retry(ExponentialBackoff::default(), || async {
            gcs::delete_object(&self.bucket, &self.name)
                .await
                .map(|_| ())
                .map_err(BackoffError::Transient)
        })
        .await
    }
}

pub async fn url_exists(url: Url) -> Result<bool> {
    match HTTP_CLI.get(url).send().await {
        Ok(response) => {
            if response.status().is_success() {
                Ok(true)
            } else {
                Ok(false)
            }
        }
        Err(e) => Err(anyhow!("url check eror {:?}", e)),
    }
}

pub async fn download_from_url(url: Url) -> Result<Option<Vec<u8>>> {
    let result = HTTP_CLI.get(url).send().await;

    let bytes = match result {
        Ok(bytes) => bytes,
        Err(err) => {
            if let Some(status) = err.status() {
                if StatusCode::NOT_FOUND == status {
                    return Ok(None);
                }
            }
            return Err(anyhow!(err));
        }
    };

    let bytes = bytes.bytes().await?;
    Ok(Some(bytes.as_ref().to_vec()))
}

pub async fn list_files(url_or_path_str: &str) -> Result<Vec<String>> {
    match Url::parse(url_or_path_str.as_ref()) {
        Ok(url) => match GcsFile::new_with_url(&url) {
            Ok(gcs_file) => {
                let objects = gcs_file.list_objects().await?;
                Ok(objects.into_iter().map(|e| e.into_string()).collect())
            }

            Err(e) => Err(e),
        },

        Err(_) => {
            let local_file = LocalFile::new(url_or_path_str.into())?;
            local_file.list_directory()
        }
    }
}

pub async fn get_file_contents_as_str(
    url_or_path_str: &str,
    gzip_decomporess: bool,
) -> Result<Option<String>> {
    match get_file_contents(url_or_path_str, gzip_decomporess).await {
        Err(e) => Err(e),
        Ok(c) => Ok(c.map(|contents| std::str::from_utf8(contents.as_ref()).unwrap().to_string())),
    }
}

pub async fn get_file_contents(
    url_or_path_str: &str,
    gzip_decomporess: bool,
) -> Result<Option<Vec<u8>>> {
    let contents = retry(ExponentialBackoff::default(), || async {
        match Url::parse(url_or_path_str.as_ref()) {
            Ok(url) => match GcsFile::new_with_url(&url) {
                Ok(gcs_file) => match gcs_file.download().await {
                    Ok(v) => Ok(v),
                    Err(e) => {
                        log::warn!("is_exists check failed:{}, {}", url_or_path_str, e);
                        Err(BackoffError::Transient(e))
                    }
                },
                Err(_) => match download_from_url(url).await {
                    Ok(v) => Ok(v),
                    Err(e) => {
                        // comment out cause too noizy
                        //log::warn!("get file contents error : {}", e);
                        Err(BackoffError::Transient(e))
                    }
                },
            },

            Err(e) => {
                log::warn!("is_exists check failed:{}, {}", url_or_path_str, e);
                let local_file = LocalFile::new(url_or_path_str.into())?;
                local_file.read().map_err(BackoffError::Transient)
            }
        }
    })
    .await?;

    match contents {
        None => Ok(None),
        Some(c) => {
            if gzip_decomporess {
                Ok(Some(gzip::gzip_decompress(c.as_ref())?.to_vec()))
            } else {
                Ok(Some(c))
            }
        }
    }
}

pub async fn is_exists(url_or_path_str: &str) -> Result<bool> {
    retry(ExponentialBackoff::default(), || async {
        match Url::parse(url_or_path_str.as_ref()) {
            Ok(url) => match GcsFile::new_with_url(&url) {
                Ok(gcs_file) => match gcs_file.is_exists().await {
                    Ok(v) => Ok(v),
                    Err(e) => {
                        log::warn!("is_exists check failed:{}, {}", url_or_path_str, e);
                        Err(BackoffError::Transient(e))
                    }
                },
                Err(_) => match url_exists(url).await {
                    Ok(v) => Ok(v),
                    Err(e) => {
                        log::warn!("check file exists error : {}", e);
                        Err(BackoffError::Transient(e))
                    }
                },
            },

            Err(_) => {
                let local_file = LocalFile::new(url_or_path_str.into())?;
                local_file.is_exists().map_err(BackoffError::Transient)
            }
        }
    })
    .await
}

pub async fn write_contents<'a>(
    url_or_path_str: &'a str,
    body: Vec<u8>,
    mime_type: mime::MimeType,
    compress: bool,
) -> Result<&'a str> {
    let body = if compress {
        gzip::gzip_compress(body.as_ref())?.to_vec()
    } else {
        body
    };

    match Url::parse(url_or_path_str) {
        Ok(url) => match GcsFile::new_with_url(&url) {
            Ok(gcs_file) => gcs_file.write(body, mime_type).await,
            Err(_) => unimplemented!(
                "update to general url is not supported :{}",
                url_or_path_str
            ),
        },

        Err(_) => {
            let local_file = LocalFile::new(url_or_path_str.into())?;
            local_file.write(body)
        }
    }?;
    Ok(url_or_path_str)
}

pub async fn delete_contents(url_or_path_str: &str) -> Result<()> {
    match Url::parse(url_or_path_str) {
        Ok(url) => match GcsFile::new_with_url(&url) {
            Ok(gcs_file) => gcs_file.delete().await,
            Err(_) => unimplemented!("delet to general url is not supported :{}", url_or_path_str),
        },

        Err(_) => {
            let local_file = LocalFile::new(url_or_path_str.into())?;
            local_file.delete()
        }
    }
}

#[cfg(test)]
mod test {
    use super::get_file_contents_as_str;
    use test_util::test_dmm_video_file_path;
    use url::Url;

    #[test]
    fn gcs_file_parse() {
        let gcs_url = Url::parse("gs://some_bucket/path/to/file").unwrap();
        let parsed = super::GcsFile::parse_bucket_and_name_from_url(&gcs_url);
        assert_eq!(
            (String::from("some_bucket"), String::from("path/to/file")),
            parsed.unwrap()
        );

        let gcs_url = Url::parse("gs://some_bucket//path").unwrap();
        let parsed = super::GcsFile::parse_bucket_and_name_from_url(&gcs_url);
        assert_eq!(true, parsed.is_err());

        let gcs_url = Url::parse("gs://some_bucket/path/").unwrap();
        let parsed = super::GcsFile::parse_bucket_and_name_from_url(&gcs_url);
        assert_eq!(true, parsed.is_err());

        let gcs_url = Url::parse("gs://some_bucket/").unwrap();
        let parsed = super::GcsFile::parse_bucket_and_name_from_url(&gcs_url);
        assert_eq!(true, parsed.is_err());

        let gcs_url = Url::parse("gs:///path/to/file").unwrap();
        let parsed = super::GcsFile::parse_bucket_and_name_from_url(&gcs_url);
        assert_eq!(true, parsed.is_err());
    }

    #[tokio::test]
    async fn load_file_decompressing() {
        let file_path = test_dmm_video_file_path().as_path().display().to_string();
        let contents = get_file_contents_as_str(&file_path, true).await.unwrap();

        assert_ne!("".to_owned(), contents.unwrap());
    }
}
