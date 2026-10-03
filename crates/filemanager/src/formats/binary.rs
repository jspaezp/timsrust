use memmap2::Mmap;
use std::io;

use crate::{Uri, cloud_store::CloudObject};

/// A fixed-size byte source that fills buffers owned by the caller.
pub trait ReadAt: std::fmt::Debug + Send + Sync {
    /// Returns the byte length of the source.
    fn len(&self) -> u64;
    /// Returns whether the source has no bytes.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Fills the caller's buffer from the requested byte offset.
    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()>;
}

/// Reads byte ranges from an injected object store.
#[cfg(feature = "cloud")]
#[derive(Debug)]
pub struct ObjectStoreReadAt {
    store: std::sync::Arc<dyn object_store::ObjectStore>,
    path: object_store::path::Path,
    len: u64,
    e_tag: Option<String>,
}

#[cfg(feature = "cloud")]
impl ObjectStoreReadAt {
    /// Opens one object and records its size and ETag for later range reads.
    pub fn new(
        store: std::sync::Arc<dyn object_store::ObjectStore>,
        path: object_store::path::Path,
    ) -> io::Result<Self> {
        let metadata = crate::runtime::block_on(store.head(&path))
            .map_err(io::Error::other)?;
        Ok(Self {
            store,
            path,
            len: metadata.size as u64,
            e_tag: metadata.e_tag,
        })
    }
}

#[cfg(feature = "cloud")]
impl ReadAt for ObjectStoreReadAt {
    fn len(&self) -> u64 {
        self.len
    }

    fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let end = offset
            .checked_add(buf.len() as u64)
            .filter(|&end| end <= self.len)
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        let start = usize::try_from(offset).map_err(io::Error::other)?;
        let end = usize::try_from(end).map_err(io::Error::other)?;
        if buf.is_empty() {
            return Ok(());
        }
        let options = object_store::GetOptions {
            range: Some(object_store::GetRange::Bounded(start..end)),
            if_match: self.e_tag.clone(),
            ..Default::default()
        };
        let bytes = crate::runtime::block_on(async {
            self.store
                .get_opts(&self.path, options)
                .await?
                .bytes()
                .await
        })
        .map_err(io::Error::other)?;
        if bytes.len() != buf.len() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        buf.copy_from_slice(&bytes);
        Ok(())
    }
}

/// Errors from binary read/write operations.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum BinaryError {
    /// An I/O error occurred.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// The requested byte range is out of bounds.
    #[error("out of bounds: {start}..{end} (len {len})")]
    OutOfBounds {
        start: usize,
        end: usize,
        len: usize,
    },
    /// Memory-mapped view is not available for this file type.
    #[error("memory-mapped view unavailable for this file type")]
    ViewUnavailable,
    /// A cloud storage error occurred.
    #[error("cloud error: {0}")]
    Cloud(#[from] crate::CloudError),
}

/// Writes raw bytes to a local file.
///
/// # Examples
///
/// ```
/// use filemanager::formats::binary::BinaryWriter;
///
/// let dir = tempfile::tempdir().unwrap();
/// let path = dir.path().join("out.bin");
/// let mut writer = BinaryWriter::new(path).unwrap();
/// writer.write(b"hello world").unwrap();
/// ```
#[derive(Debug)]
pub struct BinaryWriter {
    file: std::fs::File,
}

impl BinaryWriter {
    /// Creates a new `BinaryWriter` for the given local path.
    ///
    /// # Examples
    ///
    /// ```
    /// use filemanager::formats::binary::BinaryWriter;
    ///
    /// let dir = tempfile::tempdir().unwrap();
    /// let path = dir.path().join("data.bin");
    /// let _writer = BinaryWriter::new(path).unwrap();
    /// ```
    pub fn new(path: impl Into<Uri>) -> Result<Self, BinaryError> {
        let uri: Uri = path.into();
        let p = uri.as_path().ok_or_else(|| {
            BinaryError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Can only write to local paths",
            ))
        })?;
        let file = std::fs::File::create(p)?;
        Ok(BinaryWriter { file })
    }

    /// Writes bytes to the file.
    ///
    /// # Examples
    ///
    /// ```
    /// use filemanager::formats::binary::BinaryWriter;
    ///
    /// let dir = tempfile::tempdir().unwrap();
    /// let path = dir.path().join("data.bin");
    /// let mut writer = BinaryWriter::new(path).unwrap();
    /// writer.write(b"hello").unwrap();
    /// writer.write(b" world").unwrap();
    /// ```
    pub fn write(&mut self, data: &[u8]) -> Result<(), BinaryError> {
        use std::io::Write;
        self.file.write_all(data)?;
        Ok(())
    }

    pub fn upload(
        source: impl Into<Uri>,
        dest: impl Into<Uri>,
    ) -> Result<(), BinaryError> {
        let source_uri: Uri = source.into();
        let source_path = source_uri.as_path().ok_or_else(|| {
            BinaryError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Can only upload from local paths",
            ))
        })?;
        let dest_uri: Uri = dest.into();
        if !dest_uri.is_cloud() {
            return Err(BinaryError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Can only upload to cloud paths",
            )));
        }
        CloudObject::new(dest_uri.clone())?.upload_from(source_path)?;
        Ok(())
    }

    pub fn upload_bytes(
        bytes: Vec<u8>,
        dest: impl Into<Uri>,
    ) -> Result<(), BinaryError> {
        let dest_uri: Uri = dest.into();
        if !dest_uri.is_cloud() {
            return Err(BinaryError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Can only upload to cloud paths",
            )));
        }
        CloudObject::new(dest_uri.clone())?.upload_bytes(bytes)?;
        Ok(())
    }
}

#[derive(Debug)]
enum Inner {
    Mmap(Mmap),
    Cloud(CloudObject),
    External(Box<dyn ReadAt>),
}

impl Inner {
    fn range(
        &self,
        range: std::ops::Range<usize>,
    ) -> Result<Vec<u8>, BinaryError> {
        match self {
            Inner::Mmap(m) => m
                .get(range.clone())
                .ok_or_else(|| BinaryError::OutOfBounds {
                    start: range.start,
                    end: range.end,
                    len: m.len(),
                })
                .map(|s| s.to_vec()),
            Inner::Cloud(c) => c.range(range).map_err(BinaryError::Cloud),
            Inner::External(source) => {
                let mut data = vec![0; range.end - range.start];
                source.read_exact_at(range.start as u64, &mut data)?;
                Ok(data)
            },
        }
    }

    fn view_range(
        &self,
        range: std::ops::Range<usize>,
    ) -> Result<&[u8], BinaryError> {
        match self {
            Inner::Mmap(m) => {
                m.get(range.clone())
                    .ok_or_else(|| BinaryError::OutOfBounds {
                        start: range.start,
                        end: range.end,
                        len: m.len(),
                    })
            },
            Inner::Cloud(_) => Err(BinaryError::ViewUnavailable),
            Inner::External(_) => Err(BinaryError::ViewUnavailable),
        }
    }
}

impl TryFrom<&Uri> for Inner {
    type Error = BinaryError;

    fn try_from(uri: &Uri) -> Result<Self, Self::Error> {
        if uri.is_local() {
            let path = uri.as_path().ok_or_else(|| {
                BinaryError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "not a local path",
                ))
            })?;
            let file = std::fs::File::open(path)?;
            // SAFETY: `memmap2::Mmap::map` is the only way to memory-map a
            // file; the `unsafe` is imposed by the crate's API, not by any
            // unsoundness introduced here.
            let mmap = unsafe { memmap2::MmapOptions::new().map(&file) }?;
            Ok(Inner::Mmap(mmap))
        } else if uri.is_cloud() {
            let cloud_object =
                CloudObject::new(uri.clone()).map_err(BinaryError::Cloud)?;
            Ok(Inner::Cloud(cloud_object))
        } else {
            Err(BinaryError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unsupported URI: {uri}"),
            )))
        }
    }
}

/// Reads raw bytes from a local or cloud file.
///
/// Local files are memory-mapped; cloud files support range reads.
///
/// # Examples
///
/// ```
/// use filemanager::formats::binary::{BinaryWriter, BinaryReader};
///
/// let dir = tempfile::tempdir().unwrap();
/// let path = dir.path().join("data.bin");
/// let mut writer = BinaryWriter::new(&path).unwrap();
/// writer.write(b"hello world").unwrap();
/// drop(writer);
///
/// let reader = BinaryReader::from(&path).unwrap();
/// assert_eq!(reader.read_range(..).unwrap(), b"hello world");
/// ```
#[derive(Debug)]
pub struct BinaryReader {
    inner: Inner,
    len: usize,
    original_uri: Uri,
    effective_uri: Uri,
}

impl BinaryReader {
    /// Opens an injected byte-range source.
    pub fn from_read_at(source: Box<dyn ReadAt>) -> Result<Self, BinaryError> {
        let len = usize::try_from(source.len()).map_err(io::Error::other)?;
        Ok(Self {
            inner: Inner::External(source),
            len,
            original_uri: Uri::from("<read-at>"),
            effective_uri: Uri::from("<read-at>"),
        })
    }

    /// Opens a file by URI for reading.
    ///
    /// Local paths are memory-mapped. Cloud URIs (`s3://`, `az://`, `gs://`, …)
    /// are opened for range reads via the object store.
    ///
    /// # Examples
    ///
    /// ```
    /// use filemanager::formats::binary::BinaryReader;
    ///
    /// let dir = tempfile::tempdir().unwrap();
    /// let path = dir.path().join("test.bin");
    /// std::fs::write(&path, b"data").unwrap();
    /// let _reader = BinaryReader::from(&path).unwrap();
    /// ```
    pub fn from(uri: impl Into<Uri>) -> Result<Self, BinaryError> {
        let original_uri = uri.into();
        let effective_uri = original_uri.soft_cache();
        let inner = Inner::try_from(&effective_uri)?;
        let len = match &inner {
            Inner::Mmap(m) => m.len(),
            Inner::Cloud(c) => c.len().map_err(BinaryError::Cloud)?,
            Inner::External(source) => {
                usize::try_from(source.len()).map_err(io::Error::other)?
            },
        };
        Ok(BinaryReader {
            inner,
            len,
            original_uri,
            effective_uri,
        })
    }

    /// Returns the length of the file in bytes.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the file is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reads a byte range from the file.
    ///
    /// # Examples
    ///
    /// ```
    /// use filemanager::formats::binary::BinaryReader;
    ///
    /// let dir = tempfile::tempdir().unwrap();
    /// let path = dir.path().join("test.bin");
    /// std::fs::write(&path, b"hello world").unwrap();
    /// let reader = BinaryReader::from(&path).unwrap();
    /// assert_eq!(reader.read_range(0..5).unwrap(), b"hello");
    /// ```
    pub fn read_range(
        &self,
        range: impl std::ops::RangeBounds<usize>,
    ) -> Result<Vec<u8>, BinaryError> {
        let range = crate::range(range, self.len());
        if range.start > range.end || range.end > self.len {
            return Err(BinaryError::OutOfBounds {
                start: range.start,
                end: range.end,
                len: self.len,
            });
        }
        self.inner.range(range)
    }

    /// Returns a copy of a byte range using memory-mapped access.
    ///
    /// Returns `Err(BinaryError::ViewUnavailable)` if the file is not mmap-backed.
    ///
    /// # Examples
    ///
    /// ```
    /// use filemanager::formats::binary::BinaryReader;
    ///
    /// let dir = tempfile::tempdir().unwrap();
    /// let path = dir.path().join("test.bin");
    /// std::fs::write(&path, b"hello world").unwrap();
    /// let reader = BinaryReader::from(&path).unwrap();
    /// assert_eq!(reader.view_range(6..11).unwrap(), b"world");
    /// ```
    pub fn view_range(
        &self,
        range: impl std::ops::RangeBounds<usize>,
    ) -> Result<&[u8], BinaryError> {
        let range = crate::range(range, self.len());
        self.inner.view_range(range)
    }

    pub fn original_uri(&self) -> &Uri {
        &self.original_uri
    }

    pub fn effective_uri(&self) -> &Uri {
        &self.effective_uri
    }
}
