pub mod gzip;
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CompressionError {
    #[error("file io error {0}")]
    IOError(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, CompressionError>;

#[derive(Clone)]
pub enum Compression {
    Gzip,
}

impl Compression {
    pub fn compress(&self, bytes: &[u8]) -> Result<Vec<u8>> {
        match *self {
            Compression::Gzip => gzip::gzip_compress(bytes),
        }
    }

    pub fn decompress(&self, bytes: &[u8]) -> Result<Vec<u8>> {
        match *self {
            Compression::Gzip => gzip::gzip_decompress(bytes),
        }
    }

    pub fn from_extention<P: AsRef<Path>>(path: P) -> Option<Compression> {
        match path
            .as_ref()
            .extension()
            .map(|os_str| os_str.to_str().unwrap_or(""))
        {
            None => None,
            Some(ext) => match ext {
                "gzip" | "gz" => Some(Compression::Gzip),
                _ => None,
            },
        }
    }
}

pub(crate) fn decompress_opt(
    data: Option<Vec<u8>>,
    decompression: Option<Compression>,
) -> Result<Option<Vec<u8>>> {
    match (data, decompression) {
        (None, _) => Ok(None),
        (contents @ _, None) => Ok(contents),
        (Some(contents), Some(compression)) => {
            let data = compression.decompress(&contents)?;
            Ok(Some(data))
        }
    }
}

pub(crate) fn compress_opt(data: &[u8], compression: Option<Compression>) -> Result<Vec<u8>> {
    match compression {
        None => Ok(data.to_vec()),
        Some(compression) => {
            let data = compression.compress(data)?;
            Ok(data)
        }
    }
}
