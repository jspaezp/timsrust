mod minitdf;
mod tdf;

use core::fmt;
use std::path::Path;

use minitdf::{MiniTDFPrecursorReader, MiniTDFPrecursorReaderError};
use tdf::{TDFPrecursorReader, TDFPrecursorReaderError};

use crate::ms_data::Precursor;

use super::FrameWindowSplittingStrategy;

pub struct PrecursorReader {
    precursor_reader: Box<dyn PrecursorReaderTrait>,
}

impl fmt::Debug for PrecursorReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PrecursorReader {{ /* fields omitted */ }}")
    }
}

impl PrecursorReader {
    pub fn new(
        path: impl AsRef<Path>,
        config: Option<FrameWindowSplittingStrategy>,
    ) -> Result<Self, PrecursorReaderError> {
        let tmp = path.as_ref().extension().and_then(|e| e.to_str());
        let precursor_reader: Box<dyn PrecursorReaderTrait> =
            match (tmp, config) {
                (Some("parquet"), None) => {
                    Box::new(MiniTDFPrecursorReader::new(path)?)
                },
                (Some("tdf"), strat) => {
                    Box::new(TDFPrecursorReader::new(path, strat)?)
                },
                _ => panic!(),
            };
        let reader = Self { precursor_reader };
        Ok(reader)
    }

    pub fn get(&self, index: usize) -> Option<Precursor> {
        self.precursor_reader.get(index)
    }

    pub fn len(&self) -> usize {
        self.precursor_reader.len()
    }
}

trait PrecursorReaderTrait: Sync {
    fn get(&self, index: usize) -> Option<Precursor>;
    fn len(&self) -> usize;
}

#[derive(Debug, thiserror::Error)]
pub enum PrecursorReaderError {
    #[error("{0}")]
    MiniTDFPrecursorReaderError(#[from] MiniTDFPrecursorReaderError),
    #[error("{0}")]
    TDFPrecursorReaderError(#[from] TDFPrecursorReaderError),
}
