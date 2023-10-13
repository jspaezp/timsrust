use {
    crate::{
        acquisition::AcquisitionType,
        converters::{
            ConvertableIndex, Frame2RtConverter, Scan2ImConverter,
            Tof2MzConverter,
        },
        file_readers::{
            common::{
                ms_data_blobs::{BinFileReader, ReadableFromBinFile},
                sql_reader::{FrameTable, DiaFramesInfoTable, DiaFramesMsMsTable, ReadableFromSql, SqlReader},
            },
            ReadableFrames,
        },
        Frame, FrameType,
    },
    rayon::prelude::*,
    std::path::Path,
};

#[derive(Debug)]
pub struct TDFReader {
    pub path: String,
    pub tdf_sql_reader: SqlReader,
    tdf_bin_reader: BinFileReader,
    pub rt_converter: Frame2RtConverter,
    pub im_converter: Scan2ImConverter,
    pub mz_converter: Tof2MzConverter,
    pub frame_table: FrameTable,
    pub frame_types: Vec<FrameType>,
    pub dia_frame_table: DiaFramesInfoTable,
    pub dia_frame_msms_table: DiaFramesMsMsTable,
}

impl TDFReader {
    pub fn new(path: &String) -> Self {
        let tdf_sql_reader: SqlReader = SqlReader {
            path: String::from(path),
        };
        let frame_table: FrameTable = FrameTable::from_sql(&tdf_sql_reader);
        let file_name: String = Path::new(&path)
            .join("analysis.tdf_bin")
            .to_string_lossy()
            .to_string();
        let tdf_bin_reader: BinFileReader = BinFileReader::new(
            String::from(&file_name),
            frame_table.offsets.clone(),
        );
        let frame_types: Vec<FrameType> = frame_table
            .msms_type
            .iter()
            .map(|msms_type| match msms_type {
                0 => FrameType::MS1,
                8 => FrameType::MS2(AcquisitionType::DDAPASEF),
                9 => FrameType::MS2(AcquisitionType::DIAPASEF),
                _ => FrameType::Unknown,
            })
            .collect();
        let dia_frames_table: DiaFramesInfoTable =
            DiaFramesInfoTable::from_sql(&tdf_sql_reader);
        let dia_frames_table: DiaFramesInfoTable = DiaFramesInfoTable::from_sql(&tdf_sql_reader);
        let dia_frames_msms_table: DiaFramesMsMsTable = DiaFramesMsMsTable::from_sql(&tdf_sql_reader);
        Self {
            path: path.to_string(),
            tdf_bin_reader: tdf_bin_reader,
            rt_converter: Self::get_rt_converter(&frame_table),
            im_converter: Scan2ImConverter::from_sql(&tdf_sql_reader),
            mz_converter: Tof2MzConverter::from_sql(&tdf_sql_reader),
            frame_table: frame_table,
            tdf_sql_reader: tdf_sql_reader,
            frame_types: frame_types,
            dia_frame_table: dia_frames_table,
            dia_frame_msms_table: dia_frames_msms_table,
        }
    }

    fn get_rt_converter(frame_table: &FrameTable) -> Frame2RtConverter {
        let retention_times: Vec<f64> = frame_table.rt.clone();
        Frame2RtConverter::new(retention_times)
    }
}

impl ReadableFrames for TDFReader {
    fn read_single_frame(&self, index: usize) -> Frame {
        let mut frame: Frame =
            Frame::read_from_file(&self.tdf_bin_reader, index);
        frame.rt = self.rt_converter.convert(index as u32);
        frame.index = self.frame_table.id[index];
        frame.frame_type = self.frame_types[index];
        frame
    }

    fn read_all_frames(&self) -> Vec<Frame> {
        (0..self.tdf_bin_reader.size())
            .into_par_iter()
            .map(|index| self.read_single_frame(index))
            .collect()
    }

    fn read_all_ms1_frames(&self) -> Vec<Frame> {
        (0..self.tdf_bin_reader.size())
            .into_par_iter()
            .map(|index| match self.frame_types[index] {
                FrameType::MS1 => self.read_single_frame(index),
                _ => Frame::default(),
            })
            .collect()
    }

    fn read_all_ms2_frames(&self) -> Vec<Frame> {
        (0..self.tdf_bin_reader.size())
            .into_par_iter()
            .map(|index| match self.frame_types[index] {
                FrameType::MS2(_) => self.read_single_frame(index),
                _ => Frame::default(),
            })
    fn read_all_dia_frames(&self) -> Vec<Frame> {
        let dia_frame_ids: Vec<usize> = self.dia_frame_table.frame.clone();
        let dia_frame_ids: Vec<usize> = dia_frame_ids
            .into_iter()
            .filter(|&x| x < self.frame_table.id.len())
            .collect();
        dia_frame_ids
            .into_par_iter()
            .map(|index| self.read_single_frame(index))
            .collect()
    }
}
