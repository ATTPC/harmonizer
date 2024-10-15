use color_eyre::eyre::{eyre, Result};
use hdf5_metno::File;
use ndarray::{arr1, s};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum MergerVersion {
    V010,
    V020,
    Invalid,
}

fn construct_run_path(merger_path: &Path, run_number: i32) -> PathBuf {
    merger_path.join(format!("run_{:0>4}.h5", run_number))
}

#[derive(Debug)]
struct MergerReader {
    merger_path: PathBuf,
    min_run: i32,
    max_run: i32,
    version: MergerVersion,
    current_run: i32,
    current_file: File,
    current_event: u64,
    current_max_event: u64,
}

impl MergerReader {
    pub fn new(merger_path: &Path, min_run: i32, max_run: i32) -> Result<Self> {
        let first_file = File::open(construct_run_path(merger_path, min_run))?;
        let mut reader = Self {
            merger_path: merger_path.to_path_buf(),
            min_run,
            max_run,
            version: MergerVersion::Invalid,
            current_run: min_run,
            current_file: first_file,
            current_event: 0,
            current_max_event: 0,
        };
        reader.init_file()?;
        Ok(reader)
    }

    fn init_file(&mut self) -> Result<()> {
        let parent_groups = self.current_file.member_names()?;
        if parent_groups.contains(&String::from("meta")) {
            self.version = MergerVersion::V010;
            let meta_group = self.current_file.group("meta")?;
            let meta_data = meta_group.dataset("meta")?;
            let meta_array = meta_data.read_1d::<u64>()?;
            self.current_event = meta_array[0];
            self.current_max_event = meta_array[2];
        } else if parent_groups.contains(&String::from("events")) {
            let event_group = self.current_file.group("events")?;
            self.current_event = event_group.attr("min_event")?.read_scalar::<u64>()?;
            self.current_max_event = event_group.attr("max_event")?.read_scalar::<u64>()?;
        } else {
            return Err(eyre!("Invalid Merger Version!"));
        }

        Ok(())
    }
}
