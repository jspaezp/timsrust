pub(crate) mod compression1;
pub(crate) mod compression2;
pub(crate) mod frame_info_reader;

use std::collections::HashMap;
use std::path::Path;

use timsrust_core::io::formats::binary::{BinaryError, BinaryReader, ReadAt};
use timsrust_core::{FrameIons, utils::reader::Reader};

pub(crate) use frame_info_reader::FrameInfoReader;

use crate::{
    Metadata, TDFPathError,
    file_readers::sql_reader::{
        ReadableSqlHashMap, SqlReader, metadata::SqlMetadata,
    },
    frame_reader::{
        compression1::{
            TdfBlobReaderCompression1, TdfBlobReaderErrorCompression1,
        },
        compression2::{TdfBlobReader, TdfBlobReaderError},
    },
};

pub use frame_info_reader::FrameReaderErrorInternal;

use super::{
    MetadataReaderError, QuadrupoleSettingsReaderError, TDFPathLike,
    file_readers::sql_reader::SqlReaderError,
};

/// Adapts a raw blob reader (indexed by binary file offset) to be indexed by
/// frame id, using the offset table from [`FrameInfoReader`].
#[derive(Debug)]
struct TdfOffsetIonReader<B> {
    blob_reader: B,
    offsets: HashMap<usize, usize>,
}

impl<B> TdfOffsetIonReader<B> {
    fn new(blob_reader: B, offsets: HashMap<usize, usize>) -> Self {
        Self {
            blob_reader,
            offsets,
        }
    }
}

impl<B: Reader<FrameIons>> Reader<FrameIons> for TdfOffsetIonReader<B>
where
    FrameReaderError: From<B::Error>,
{
    type Error = FrameReaderError;

    fn get(&self, index: usize) -> Result<FrameIons, Self::Error> {
        let offset = self
            .offsets
            .get(&index)
            .copied()
            .ok_or(FrameReaderError::IndexOutOfBounds)?;
        self.blob_reader.get(offset).map_err(FrameReaderError::from)
    }
}

/// Unifies the two TDF compression schemes into a single [`Reader<FrameIons>`].
#[allow(private_interfaces)]
#[derive(Debug)]
pub enum TdfIonReader {
    Compression1(TdfOffsetIonReader<TdfBlobReaderCompression1>),
    Compression2(TdfOffsetIonReader<TdfBlobReader>),
}

impl Reader<FrameIons> for TdfIonReader {
    type Error = FrameReaderError;

    fn get(&self, index: usize) -> Result<FrameIons, FrameReaderError> {
        match self {
            Self::Compression1(r) => r.get(index),
            Self::Compression2(r) => r.get(index),
        }
    }
}

/// A concrete frame reader for Bruker TDF files.
///
/// Thin newtype around
/// [`timsrust_core::FrameReader<TdfIonReader, FrameInfoReader>`].  All
/// [`FrameReader`] methods (`get_frame`, `get_info`, `iter_indices`,
/// `parallel_filter`, …) are available via [`Deref`].
#[derive(Debug)]
pub struct TdfFrameReader(
    timsrust_core::FrameReader<TdfIonReader, FrameInfoReader>,
);

impl std::ops::Deref for TdfFrameReader {
    type Target = timsrust_core::FrameReader<TdfIonReader, FrameInfoReader>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TdfFrameReader {
    /// Opens a local SQLite file and an independently supplied binary source.
    /// The SQLite file is read during construction and may then be removed.
    pub fn from_parts(
        sqlite_path: impl AsRef<Path>,
        binary: Box<dyn ReadAt>,
    ) -> Result<Self, FrameReaderError> {
        let sql = SqlReader::open_file(sqlite_path)?;
        let metadata = SqlMetadata::from_sql_reader(&sql)?;
        let compression_type = metadata
            .get("TimsCompressionType")
            .ok_or_else(|| {
                FrameReaderError::InvalidMetadata("TimsCompressionType".into())
            })?
            .parse::<u8>()
            .map_err(|_| {
                FrameReaderError::InvalidMetadata("TimsCompressionType".into())
            })?;
        let max_peaks_per_scan = metadata
            .get("MaxNumPeaksPerScan")
            .ok_or_else(|| {
                FrameReaderError::InvalidMetadata("MaxNumPeaksPerScan".into())
            })?
            .parse::<usize>()
            .map_err(|_| {
                FrameReaderError::InvalidMetadata("MaxNumPeaksPerScan".into())
            })?;
        let info_reader = FrameInfoReader::from_sql_reader(&sql)?;
        let offsets = info_reader.offsets_map();
        let binary = BinaryReader::from_read_at(binary)?;
        let ion_reader = match compression_type {
            1 => {
                let mut blob = TdfBlobReaderCompression1::from_binary(binary);
                blob.set_max_peaks_per_scan(max_peaks_per_scan);
                TdfIonReader::Compression1(TdfOffsetIonReader::new(
                    blob, offsets,
                ))
            },
            2 => {
                let blob = TdfBlobReader::from_binary(binary);
                TdfIonReader::Compression2(TdfOffsetIonReader::new(
                    blob, offsets,
                ))
            },
            _ => {
                return Err(FrameReaderError::CompressionTypeError(
                    compression_type,
                ));
            },
        };
        Ok(Self(timsrust_core::FrameReader::new(
            ion_reader,
            info_reader,
        )))
    }

    pub fn new(path: impl TDFPathLike) -> Result<Self, FrameReaderError> {
        let metadata = Metadata::new(&path)?;
        Self::without_metadata(
            &path,
            metadata.compression_type(),
            metadata.max_peaks_per_scan(),
        )
    }

    pub fn without_metadata(
        path: impl TDFPathLike,
        compression_type: u8,
        max_peaks_per_scan: usize,
    ) -> Result<Self, FrameReaderError> {
        let info_reader = FrameInfoReader::new(&path)?;
        let offsets = info_reader.offsets_map();
        let ion_reader = match compression_type {
            1 => {
                let mut blob =
                    TdfBlobReaderCompression1::new(path.to_timstof_path())?;
                blob.set_max_peaks_per_scan(max_peaks_per_scan);
                TdfIonReader::Compression1(TdfOffsetIonReader::new(
                    blob, offsets,
                ))
            },
            2 => {
                let blob = TdfBlobReader::new(path.to_timstof_path())?;
                TdfIonReader::Compression2(TdfOffsetIonReader::new(
                    blob, offsets,
                ))
            },
            _ => {
                return Err(FrameReaderError::CompressionTypeError(
                    compression_type,
                ));
            },
        };
        Ok(Self(timsrust_core::FrameReader::new(
            ion_reader,
            info_reader,
        )))
    }

    /// Consume `self` and return the underlying generic
    /// [`timsrust_core::FrameReader`].
    pub fn into_inner(
        self,
    ) -> timsrust_core::FrameReader<TdfIonReader, FrameInfoReader> {
        self.0
    }

    /// Return the acquisition type detected from the frame metadata.
    pub fn get_acquisition(&self) -> timsrust_core::AcquisitionType {
        self.info_reader().get_acquisition()
    }
}

#[allow(private_interfaces)]
#[derive(Debug, thiserror::Error)]
pub enum FrameReaderError {
    #[error("invalid or missing metadata: {0}")]
    InvalidMetadata(String),
    #[error(transparent)]
    Binary(#[from] BinaryError),
    #[error("Timscompress error")]
    TimscompressError,
    #[error("{0}")]
    TdfBlobReaderError(#[from] TdfBlobReaderError),
    #[error("{0}")]
    TdfBlobReaderErrorCompression1(#[from] TdfBlobReaderErrorCompression1),
    #[error("{0}")]
    MetadataReaderError(#[from] MetadataReaderError),
    #[error("{0}")]
    FileNotFound(String),
    #[error("{0}")]
    SqlReaderError(#[from] SqlReaderError),
    #[error("Corrupt Frame")]
    CorruptFrame,
    #[error("{0}")]
    QuadrupoleSettingsReaderError(#[from] QuadrupoleSettingsReaderError),
    #[error("Index out of bounds")]
    IndexOutOfBounds,
    #[error("Compression type {0} not understood")]
    CompressionTypeError(u8),
    #[error("Failed to read path: {0}")]
    PathError(#[from] TDFPathError),
    #[error("Got unexpected TdfBlob type")]
    UnexpectedTdfBlobError,
    #[error("{0}")]
    FrameInfoReaderError(#[from] frame_info_reader::FrameReaderErrorInternal),
    #[error("{0}")]
    CoreFrameReaderError(#[from] timsrust_core::FrameReaderError),
}

#[cfg(all(test, feature = "zip"))]
mod zip_tests {
    use super::TdfFrameReader;
    use object_store::{local::LocalFileSystem, path::Path as ObjectPath};
    use std::{
        fs::File,
        io::{self, Read},
        path::Path,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };
    use timsrust_core::io::{
        formats::binary::{ObjectStoreReadAt, ReadAt},
        zip_archive::{ZipArchive, ZipArchiveError},
    };
    use zip::{CompressionMethod, ZipWriter, write::SimpleFileOptions};

    #[derive(Debug)]
    struct MeteredReadAt {
        inner: ObjectStoreReadAt,
        fetched: Arc<AtomicU64>,
    }

    #[derive(Debug)]
    struct FailingReadAt {
        len: u64,
    }

    impl ReadAt for FailingReadAt {
        fn len(&self) -> u64 {
            self.len
        }

        fn read_exact_at(
            &self,
            _offset: u64,
            _buf: &mut [u8],
        ) -> io::Result<()> {
            Err(io::Error::other("simulated range failure"))
        }
    }

    impl ReadAt for MeteredReadAt {
        fn len(&self) -> u64 {
            self.inner.len()
        }

        fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
            self.inner.read_exact_at(offset, buf)?;
            self.fetched.fetch_add(buf.len() as u64, Ordering::Relaxed);
            Ok(())
        }
    }

    #[test]
    fn reads_stored_binary_member_from_object_store()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/test.d");
        let scratch = tempfile::tempdir()?;
        let archive_path = scratch.path().join("sample.zip");
        let mut writer = ZipWriter::new(File::create(&archive_path)?);
        writer.start_file(
            "padding.bin",
            SimpleFileOptions::default()
                .compression_method(CompressionMethod::Stored),
        )?;
        io::copy(&mut io::repeat(0).take(4 * 1024 * 1024), &mut writer)?;
        for (name, compression) in [
            ("analysis.tdf", CompressionMethod::Deflated),
            ("analysis.tdf_bin", CompressionMethod::Stored),
        ] {
            writer.start_file(
                format!("sample.d/{name}"),
                SimpleFileOptions::default().compression_method(compression),
            )?;
            io::copy(&mut File::open(fixture.join(name))?, &mut writer)?;
        }
        writer.finish()?;

        let store = Arc::new(LocalFileSystem::new_with_prefix(scratch.path())?);
        let object =
            ObjectStoreReadAt::new(store, ObjectPath::from("sample.zip"))?;
        let fetched = Arc::new(AtomicU64::new(0));
        let mut archive = ZipArchive::open(Box::new(MeteredReadAt {
            inner: object,
            fetched: fetched.clone(),
        }))?;
        let sqlite_path = scratch.path().join("extracted.tdf");
        archive.extract_to("sample.d/analysis.tdf", &sqlite_path)?;
        assert!(matches!(
            archive.member("sample.d/analysis.tdf"),
            Err(ZipArchiveError::CompressedMember(_))
        ));
        let binary = archive.member("sample.d/analysis.tdf_bin")?;
        let mut last_byte = [0];
        assert!(binary.read_exact_at(binary.len(), &mut last_byte).is_err());
        let reader =
            TdfFrameReader::from_parts(&sqlite_path, Box::new(binary))?;
        let failing_reader = TdfFrameReader::from_parts(
            &sqlite_path,
            Box::new(FailingReadAt { len: 1024 }),
        )?;
        assert!(
            failing_reader
                .get_frame(1)
                .unwrap_err()
                .to_string()
                .contains("simulated range failure")
        );
        let expected =
            TdfFrameReader::without_metadata(fixture.to_str().unwrap(), 2, 0)?;
        assert_eq!(reader.get_frame(1)?, expected.get_frame(1)?);
        std::fs::remove_file(sqlite_path)?;
        assert_eq!(reader.get_frame(2)?, expected.get_frame(2)?);
        assert!(
            fetched.load(Ordering::Relaxed)
                < std::fs::metadata(archive_path)?.len() / 2
        );

        Ok(())
    }
}
