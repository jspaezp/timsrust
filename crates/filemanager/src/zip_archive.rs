//! Range-based access to ZIP archives. Only stored members support random reads.

use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use crate::formats::binary::ReadAt;

/// Errors from ZIP indexing, member lookup, and extraction.
#[derive(Debug, thiserror::Error)]
pub enum ZipArchiveError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Zip(#[from] zip::result::ZipError),
    #[error("random reads require an uncompressed ZIP member: {0}")]
    CompressedMember(String),
}

#[derive(Debug)]
struct RangeCursor {
    source: Arc<dyn ReadAt>,
    position: u64,
}

impl Read for RangeCursor {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let remaining = self.source.len().saturating_sub(self.position);
        let count = remaining.min(buf.len() as u64) as usize;
        if count > 0 {
            self.source
                .read_exact_at(self.position, &mut buf[..count])?;
            self.position += count as u64;
        }
        Ok(count)
    }
}

impl Seek for RangeCursor {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        let next = match from {
            SeekFrom::Start(offset) => offset as i128,
            SeekFrom::Current(delta) => self.position as i128 + delta as i128,
            SeekFrom::End(delta) => self.source.len() as i128 + delta as i128,
        };
        self.position = u64::try_from(next)
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        Ok(self.position)
    }
}

/// A ZIP archive backed by a caller-supplied byte-range source.
pub struct ZipArchive {
    source: Arc<dyn ReadAt>,
    inner: zip::ZipArchive<BufReader<RangeCursor>>,
}

impl ZipArchive {
    /// Reads the ZIP directory through a range-readable source.
    pub fn open(source: Box<dyn ReadAt>) -> Result<Self, ZipArchiveError> {
        let source: Arc<dyn ReadAt> = source.into();
        let cursor = RangeCursor {
            source: source.clone(),
            position: 0,
        };
        let inner =
            zip::ZipArchive::new(BufReader::with_capacity(64 * 1024, cursor))?;
        Ok(Self { source, inner })
    }

    /// Returns a bounded reader for a stored member.
    pub fn member(&mut self, name: &str) -> Result<ZipMember, ZipArchiveError> {
        let member = self.inner.by_name(name)?;
        if member.compression() != zip::CompressionMethod::Stored {
            return Err(ZipArchiveError::CompressedMember(name.to_string()));
        }
        Ok(ZipMember {
            source: self.source.clone(),
            offset: member.data_start(),
            len: member.size(),
        })
    }

    /// Extracts one member, including deflated members, to an explicit path.
    pub fn extract_to(
        &mut self,
        name: &str,
        destination: impl AsRef<Path>,
    ) -> Result<(), ZipArchiveError> {
        let mut member = self.inner.by_name(name)?;
        let destination = destination.as_ref();
        let parent = destination
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let mut output = tempfile::NamedTempFile::new_in(parent)?;
        io::copy(&mut member, &mut output)?;
        output.flush()?;
        output.as_file().sync_all()?;
        output.persist(destination).map_err(|error| error.error)?;
        Ok(())
    }
}

/// A stored ZIP member whose offsets are relative to the uncompressed file.
#[derive(Debug)]
pub struct ZipMember {
    source: Arc<dyn ReadAt>,
    offset: u64,
    len: u64,
}

impl ReadAt for ZipMember {
    fn len(&self) -> u64 {
        self.len
    }

    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let _end = offset
            .checked_add(buf.len() as u64)
            .filter(|&end| end <= self.len)
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        let absolute = self
            .offset
            .checked_add(offset)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        self.source.read_exact_at(absolute, buf)
    }
}
