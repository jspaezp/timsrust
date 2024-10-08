pub mod precursors;

use parquet::file::reader::{FileReader, SerializedFileReader};
use std::{fs::File, io, path::Path, str::FromStr};

pub trait ReadableParquetTable {
    fn update_from_parquet_file(&mut self, key: &str, value: String);

    fn parse_default_field<T: FromStr + Default>(field: String) -> T {
        field.parse().unwrap_or_default()
    }

    fn from_parquet_file(
        file_name: impl AsRef<Path>,
    ) -> Result<Vec<Self>, ParquetError>
    where
        Self: Sized + Default,
    {
        let file: File = File::open(file_name)?;
        let reader: SerializedFileReader<File> =
            SerializedFileReader::new(file)?;
        let results: Vec<Self> = reader
            .get_row_iter(None)?
            .map(|record| {
                let mut result = Self::default();
                for (name, field) in record.get_column_iter() {
                    result.update_from_parquet_file(
                        name.to_string().as_str(),
                        field.to_string(),
                    );
                }
                result
            })
            .collect();
        Ok(results)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParquetError {
    #[error("{0}")]
    IO(#[from] io::Error),
    #[error("Cannot iterate over row {0}")]
    ParquetIO(#[from] parquet::errors::ParquetError),
}
