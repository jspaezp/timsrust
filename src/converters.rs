mod frame_to_rt;
mod scan_to_im;
mod tof_to_mz;

pub use frame_to_rt::Frame2RtConverter;
pub use scan_to_im::Scan2ImConverter;
pub use tof_to_mz::Tof2MzConverter;

/// Convert from one domain (e.g. Time of Flight) to a another (m/z).
pub trait ConvertableDomain {
    fn convert<T: Into<f64> + Copy>(&self, value: T) -> f64;
}
