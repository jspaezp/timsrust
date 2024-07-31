use std::path::Path;

use crate::{ms_data::QuadrupoleSettings, utils::vec_utils::argsort};

use super::file_readers::sql_reader::{
    frame_groups::SqlWindowGroup, quad_settings::SqlQuadSettings,
    ReadableSqlTable, SqlError, SqlReader,
};

pub struct QuadrupoleSettingsReader {
    quadrupole_settings: Vec<QuadrupoleSettings>,
    sql_quadrupole_settings: Vec<SqlQuadSettings>,
}

impl QuadrupoleSettingsReader {
    pub fn new(
        path: impl AsRef<Path>,
    ) -> Result<Vec<QuadrupoleSettings>, QuadrupoleSettingsReaderError> {
        let sql_path = path.as_ref();
        let tdf_sql_reader = SqlReader::open(&sql_path)?;
        Self::from_sql_settings(&tdf_sql_reader)
    }

    pub fn from_sql_settings(
        tdf_sql_reader: &SqlReader,
    ) -> Result<Vec<QuadrupoleSettings>, QuadrupoleSettingsReaderError> {
        let sql_quadrupole_settings =
            SqlQuadSettings::from_sql_reader(&tdf_sql_reader)?;
        let window_group_count = sql_quadrupole_settings
            .iter()
            .map(|x| x.window_group)
            .max()
            .unwrap() as usize; // SqlReader cannot return empty vecs, so always succeeds
        let quadrupole_settings = (0..window_group_count)
            .map(|window_group| {
                let mut quad = QuadrupoleSettings::default();
                quad.index = window_group + 1;
                quad
            })
            .collect();
        let mut quad_reader = Self {
            quadrupole_settings,
            sql_quadrupole_settings,
        };
        quad_reader.update_from_sql_quadrupole_settings();
        quad_reader.resort_groups();
        Ok(quad_reader.quadrupole_settings)
    }

    pub fn from_splitting(
        path: impl AsRef<Path>,
        splitting_strat: FrameWindowSplittingStrategy,
    ) -> Result<Vec<QuadrupoleSettings>, QuadrupoleSettingsReaderError> {
        let sql_path = path.as_ref();
        let tdf_sql_reader = SqlReader::open(&sql_path)?;
        let quadrupole_settings = Self::from_sql_settings(&tdf_sql_reader)?;
        let window_groups = SqlWindowGroup::from_sql_reader(&tdf_sql_reader)?;
        let expanded_quadrupole_settings = match splitting_strat {
            FrameWindowSplittingStrategy::Quadrupole(x) => {
                expand_quadrupole_settings(
                    &window_groups,
                    &quadrupole_settings,
                    &x,
                )
            },
            FrameWindowSplittingStrategy::Window(x) => {
                expand_window_settings(&window_groups, &quadrupole_settings, &x)
            },
        };
        Ok(expanded_quadrupole_settings)
    }

    fn update_from_sql_quadrupole_settings(&mut self) {
        for window_group in self.sql_quadrupole_settings.iter() {
            let group = window_group.window_group - 1;
            self.quadrupole_settings[group]
                .scan_starts
                .push(window_group.scan_start);
            self.quadrupole_settings[group]
                .scan_ends
                .push(window_group.scan_end);
            self.quadrupole_settings[group]
                .collision_energy
                .push(window_group.collision_energy);
            self.quadrupole_settings[group]
                .isolation_mz
                .push(window_group.mz_center);
            self.quadrupole_settings[group]
                .isolation_width
                .push(window_group.mz_width);
        }
    }

    fn resort_groups(&mut self) {
        self.quadrupole_settings = self
            .quadrupole_settings
            .iter()
            .map(|_window| {
                let mut window = _window.clone();
                let order = argsort(&window.scan_starts);
                window.isolation_mz =
                    order.iter().map(|&i| window.isolation_mz[i]).collect();
                window.isolation_width =
                    order.iter().map(|&i| window.isolation_width[i]).collect();
                window.collision_energy =
                    order.iter().map(|&i| window.collision_energy[i]).collect();
                window.scan_starts =
                    order.iter().map(|&i| window.scan_starts[i]).collect();
                window.scan_ends =
                    order.iter().map(|&i| window.scan_ends[i]).collect();
                window
            })
            .collect();
    }
}

#[derive(Debug, thiserror::Error)]
pub enum QuadrupoleSettingsReaderError {
    #[error("{0}")]
    SqlError(#[from] SqlError),
}

type SpanStep = (usize, usize);

/// Strategy for expanding quadrupole settings
///
/// This enum is used to determine how to expand quadrupole settings
/// when reading in DIA data. And exporting spectra (not frames RN).
///
/// # Variants
///
/// For example if we have a window with scan start 50 and end 500
///
/// * `None` - Do not expand quadrupole settings; use the original settings
/// * `Even(usize)` - Split the quadrupole settings into `usize` evenly spaced
/// subwindows; e.g. if `usize` is 2, the window will be split into 2 subwindows
/// of equal width.
/// * `Uniform(SpanStep)` - Split the quadrupole settings into subwindows of
/// width `SpanStep.0` and step `SpanStep.1`; e.g. if `SpanStep` is (100, 50),
/// the window will be split into subwindows of width 100 and step 50 between their
/// scan start and end.
///
#[derive(Debug, Copy, Clone)]
pub enum QuadWindowExpansionStrategy {
    None,
    Even(usize),
    Uniform(SpanStep),
}

#[derive(Debug, Clone, Copy)]
pub enum FrameWindowSplittingStrategy {
    Quadrupole(QuadWindowExpansionStrategy),
    Window(QuadWindowExpansionStrategy),
}

impl Default for FrameWindowSplittingStrategy {
    fn default() -> Self {
        Self::Quadrupole(QuadWindowExpansionStrategy::Even(1))
    }
}

fn scan_range_subsplit(
    start: usize,
    end: usize,
    strategy: &QuadWindowExpansionStrategy,
) -> Vec<(usize, usize)> {
    let out = match strategy {
        QuadWindowExpansionStrategy::None => {
            vec![(start, end)]
        },
        QuadWindowExpansionStrategy::Even(num_splits) => {
            let sub_subwindow_width = (end - start) / (num_splits + 1);
            let mut out = Vec::new();
            for sub_subwindow in 0..num_splits.clone() {
                let sub_subwindow_scan_start =
                    start + (sub_subwindow_width * sub_subwindow);
                let sub_subwindow_scan_end =
                    start + (sub_subwindow_width * (sub_subwindow + 2));

                out.push((sub_subwindow_scan_start, sub_subwindow_scan_end))
            }
            out
        },
        QuadWindowExpansionStrategy::Uniform((span, step)) => {
            let mut curr_start = start.clone();
            let mut curr_end = start + span;
            let mut out = Vec::new();
            while curr_end < end {
                out.push((curr_start, curr_end));
                curr_start += step;
                curr_end += step;
            }
            if curr_start < end {
                out.push((curr_start, end));
            }
            out
        },
    };

    debug_assert!(
        out.iter().all(|(s, e)| s < e),
        "Invalid scan range: {:?}",
        out
    );
    debug_assert!(
        out.iter().all(|(s, e)| *s >= start && *e <= end),
        "Invalid scan range: {:?}",
        out
    );
    out
}

fn expand_window_settings(
    window_groups: &[SqlWindowGroup],
    quadrupole_settings: &[QuadrupoleSettings],
    strategy: &QuadWindowExpansionStrategy,
) -> Vec<QuadrupoleSettings> {
    let mut expanded_quadrupole_settings: Vec<QuadrupoleSettings> = vec![];
    for window_group in window_groups {
        let window = window_group.window_group;
        let frame = window_group.frame;
        let group = &quadrupole_settings[window as usize - 1];
        let window_group_start =
            group.scan_starts.iter().min().unwrap().clone(); // SqlReader cannot return empty vecs, so always succeeds
        let window_group_end = group.scan_ends.iter().max().unwrap().clone(); // SqlReader cannot return empty vecs, so always succeeds
        for (sws, swe) in
            scan_range_subsplit(window_group_start, window_group_end, &strategy)
        {
            let mut mz_min = std::f64::MAX;
            let mut mz_max = std::f64::MIN;
            let mut nce_sum = 0.0;
            let mut total_scan_width = 0.0;
            for i in 0..group.len() {
                let gss = group.scan_starts[i];
                let gse = group.scan_ends[i];
                if (swe <= gse) || (gss <= sws) {
                    continue;
                }
                let half_isolation_width = group.isolation_width[i] / 2.0;
                let isolation_mz = group.isolation_mz[i];
                mz_min = mz_min.min(isolation_mz - half_isolation_width);
                mz_max = mz_max.max(isolation_mz + half_isolation_width);
                let scan_width = (gse.min(swe) - gss.max(sws)) as f64;
                nce_sum += group.collision_energy[i] * scan_width;
                total_scan_width += scan_width
            }
            let sub_quad_settings = QuadrupoleSettings {
                index: frame,
                scan_starts: vec![sws],
                scan_ends: vec![swe],
                isolation_mz: vec![(mz_min + mz_max) / 2.0],
                isolation_width: vec![mz_min - mz_max],
                collision_energy: vec![nce_sum / total_scan_width],
            };
            expanded_quadrupole_settings.push(sub_quad_settings)
        }
    }
    expanded_quadrupole_settings
}

fn expand_quadrupole_settings(
    window_groups: &[SqlWindowGroup],
    quadrupole_settings: &[QuadrupoleSettings],
    strategy: &QuadWindowExpansionStrategy,
) -> Vec<QuadrupoleSettings> {
    let mut expanded_quadrupole_settings: Vec<QuadrupoleSettings> = vec![];
    for window_group in window_groups {
        let window = window_group.window_group;
        let frame = window_group.frame;
        let group = &quadrupole_settings[window as usize - 1];
        for sub_window in 0..group.isolation_mz.len() {
            let subwindow_scan_start = group.scan_starts[sub_window];
            let subwindow_scan_end = group.scan_ends[sub_window];
            for (sws, swe) in scan_range_subsplit(
                subwindow_scan_start,
                subwindow_scan_end,
                &strategy,
            ) {
                let sub_quad_settings = QuadrupoleSettings {
                    index: frame,
                    scan_starts: vec![sws],
                    scan_ends: vec![swe],
                    isolation_mz: vec![group.isolation_mz[sub_window]],
                    isolation_width: vec![group.isolation_width[sub_window]],
                    collision_energy: vec![group.collision_energy[sub_window]],
                };
                expanded_quadrupole_settings.push(sub_quad_settings)
            }
        }
    }
    expanded_quadrupole_settings
}
