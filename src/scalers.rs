//! Functions for processing the scalers from a run set.
use std::path::Path;

use super::reader::construct_run_path;
use color_eyre::eyre::{eyre, Result};
use hdf5_metno::File;
use polars::prelude::*;

/// The main loop of processing scalers. All scalers from all runs
/// are combined into a single polars DataFrame and written to a parquet
/// file.
pub fn process_scalers(
    merger_path: &Path,
    harmonic_path: &Path,
    run_min: i32,
    run_max: i32,
) -> Result<()> {
    let scaler_path = harmonic_path.join("scalers.parquet");
    let mut scalers: Vec<Vec<u32>> = vec![vec![]; 13];
    // The scalers we have
    let scaler_columns = [
        "run",
        "event",
        "clock_free",
        "clock_live",
        "trig_free",
        "trig_live",
        "ic_sca",
        "mesh_sca",
        "si1_cfd",
        "si2",
        "sipm",
        "ic_ds",
        "ic_cfd",
    ];
    for run in run_min..(run_max + 1) {
        if let Ok(merger_file) = File::open(construct_run_path(merger_path, run)) {
            let parent_groups = merger_file.member_names()?;
            if parent_groups.contains(&String::from("meta")) {
                read_scalers_010(&mut scalers, &merger_file, run)?;
            } else if parent_groups.contains(&String::from("events")) {
                read_scalers_020(&mut scalers, &merger_file, run)?;
            } else {
                return Err(eyre!("Invalid merger version at process scalers!"));
            }
        }
    }

    let mut frame = scalers
        .iter()
        .zip(scaler_columns)
        .map(|(data, name)| Series::new(name.into(), data))
        .collect();

    let mut parquet_file = std::fs::File::create(scaler_path)?;
    ParquetWriter::new(&mut parquet_file).finish(&mut frame)?;

    Ok(())
}

/// Read scalers from the 0.1.0 merger format
fn read_scalers_010(scalers: &mut [Vec<u32>], file: &File, run: i32) -> Result<()> {
    let scaler_group = file.group("frib")?.group("scaler")?;
    let mut scaler: u32 = 0;
    loop {
        if let Ok(event) = scaler_group.dataset(&format!("scaler{scaler}_data")) {
            let data = event.read_1d()?;
            scalers[0].push(run as u32);
            scalers[1].push(scaler);
            scalers[2].push(data[0]);
            scalers[3].push(data[1]);
            scalers[4].push(data[2]);
            scalers[5].push(data[3]);
            scalers[6].push(data[4]);
            scalers[7].push(data[5]);
            scalers[8].push(data[6]);
            scalers[9].push(data[7]);
            scalers[10].push(data[8]);
            scalers[11].push(data[9]);
            scalers[12].push(data[10]);
        } else {
            break;
        }
        scaler += 1;
    }
    Ok(())
}

/// Read scalers from the modern merger format
fn read_scalers_020(scalers: &mut [Vec<u32>], file: &File, run: i32) -> Result<()> {
    let scaler_group = file.group("scalers")?;
    let scaler_min = scaler_group.attr("min_event")?.read_scalar::<u32>()?;
    let scaler_max = scaler_group.attr("max_event")?.read_scalar::<u32>()?;
    for scaler in scaler_min..(scaler_max + 1) {
        if let Ok(event) = scaler_group.dataset(&format!("event_{scaler}"))?.read_1d() {
            scalers[0].push(run as u32);
            scalers[1].push(scaler);
            scalers[2].push(event[0]);
            scalers[3].push(event[1]);
            scalers[4].push(event[2]);
            scalers[5].push(event[3]);
            scalers[6].push(event[4]);
            scalers[7].push(event[5]);
            scalers[8].push(event[6]);
            scalers[9].push(event[7]);
            scalers[10].push(event[8]);
            scalers[11].push(event[9]);
            scalers[12].push(event[10]);
        }
    }
    Ok(())
}
