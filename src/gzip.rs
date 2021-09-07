use anyhow::Result;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::{Read, Write};

pub(crate) fn gzip_decompress(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut gz = GzDecoder::new(bytes);
    let mut dest = Vec::<u8>::new();

    gz.read_to_end(&mut dest)?;
    Ok(dest)
}

pub(crate) fn gzip_compress(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    gz.write_all(bytes)?;
    let compressed = gz.finish()?;
    Ok(compressed)
}
