use super::reader::{construct_run_path, MergerEvent};
use color_eyre::eyre::Result;
use hdf5_metno::types::VarLenUnicode;
use hdf5_metno::File;
use std::path::{Path, PathBuf};
use std::str::FromStr;

#[derive(Debug)]
pub struct HarmonicWriter {
    harmonic_path: PathBuf,
    current_path: PathBuf,
    current_file: File,
    current_run: i32,
    current_event: u64,
    harmonic_size: u64,
}

impl HarmonicWriter {
    pub fn new(harmonic_path: &Path, harmonic_size: u64) -> Result<Self> {
        let current_run = 0;
        let current_path = construct_run_path(harmonic_path, current_run);
        let current_file = File::create(&current_path)?;

        let writer = Self {
            harmonic_path: harmonic_path.to_path_buf(),
            current_path,
            current_file,
            current_run,
            current_event: 0,
            harmonic_size,
        };

        writer.init_file()?;

        Ok(writer)
    }

    pub fn write(&mut self, event: MergerEvent) -> Result<()> {
        let event_group = self
            .current_file
            .group("events")?
            .create_group(&format!("event_{}", self.current_event))?;

        event_group
            .new_attr::<i32>()
            .create("orig_run")?
            .write_scalar(&event.run_number)?;

        event_group
            .new_attr::<u64>()
            .create("orig_event")?
            .write_scalar(&event.event)?;

        if let Some(get) = event.get.as_ref() {
            let traces = event_group
                .new_dataset_builder()
                .with_data(&get.traces)
                .create("get_traces")?;
            traces
                .new_attr::<u32>()
                .create("id")?
                .write_scalar(&get.id)?;
            traces
                .new_attr::<u64>()
                .create("timestamp")?
                .write_scalar(&get.timestamp)?;
            traces
                .new_attr::<u64>()
                .create("timestamp_other")?
                .write_scalar(&get.timestamp_other)?;
        }

        if let Some(frib) = event.frib.as_ref() {
            let frib_group = event_group.create_group("frib_physics")?;
            frib_group
                .new_attr::<u32>()
                .create("event")?
                .write_scalar(&frib.event)?;
            frib_group
                .new_attr::<u32>()
                .create("timestamp")?
                .write_scalar(&frib.timestamp)?;
            frib_group
                .new_dataset_builder()
                .with_data(&frib.traces)
                .create("1903")?;
            frib_group
                .new_dataset_builder()
                .with_data(&frib.coincidence)
                .create("977")?;
        }

        self.current_event += 1;

        if self.current_path.metadata()?.len() >= self.harmonic_size {
            self.finish_file()?;
            self.current_event = 0;
            self.current_run += 1;
            self.current_path = construct_run_path(&self.harmonic_path, self.current_run);
            self.current_file = File::create(&self.current_path)?;
            self.init_file()?;
        }

        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        self.finish_file()
    }

    fn init_file(&self) -> Result<()> {
        let harmonizer_version =
            format!("{}:{}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));

        let events_group = self.current_file.create_group("events")?;
        events_group
            .new_attr::<u64>()
            .create("min_event")?
            .write_scalar(&0)?;
        events_group.new_attr::<u64>().create("max_event")?;
        events_group
            .new_attr::<VarLenUnicode>()
            .create("version")?
            .write_scalar(&VarLenUnicode::from_str(&harmonizer_version).unwrap())?;
        Ok(())
    }

    fn finish_file(&self) -> Result<()> {
        self.current_file
            .group("events")?
            .attr("max_event")?
            .write_scalar(&self.current_event)?;

        Ok(())
    }
}
