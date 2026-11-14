use super::compression;
use std::fs;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FileUtilFsError {
    #[error("file io error: {0}")]
    IOError(#[from] std::io::Error),

    #[error("compression error: {0}")]
    CompressionError(#[from] crate::compression::CompressionError),
}

pub type Result<T> = std::result::Result<T, FileUtilFsError>;

pub struct FileAccessor {
    path: PathBuf,
}

impl FileAccessor {
    pub fn new(file_path: PathBuf) -> Result<Self> {
        return Ok(Self { path: file_path });
    }

    pub fn is_exists(&self) -> Result<bool> {
        Ok(self.path.exists())
    }

    pub fn read(&self) -> Result<Option<Vec<u8>>> {
        match Self::is_exists(&self) {
            Ok(true) => {
                let result = fs::read(&self.path).map(|body| Some(body))?;
                Ok(result)
            }
            Ok(false) => Ok(None),
            Err(e) => Err(e),
        }
    }
    pub fn write(&self, body: &[u8], compression: Option<compression::Compression>) -> Result<()> {
        let body = compression::compress_opt(body, compression)?;
        fs::write(&self.path, body)?;
        Ok(())
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
