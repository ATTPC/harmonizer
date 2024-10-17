//! Implementation of an attpc_merger Reader.
//! Also contains utility functions for getting cummulative statsistics about
//! the set of runs to be harmonized.
use color_eyre::eyre::{eyre, Result};
use hdf5_metno::File;
use ndarray::{Array1, Array2};
use std::path::{Path, PathBuf};

/// Enum for what version of the merger we are dealing with.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum MergerVersion {
    V010,
    V020,
    Invalid,
}

/// Construct the formated run path from a parent path and run number.
pub fn construct_run_path(path: &Path, run_number: i32) -> PathBuf {
    path.join(format!("run_{:0>4}.h5", run_number))
}

/// Traverse the set of runs and see how much data there is (in bytes).
pub fn get_total_merger_bytes(merger_path: &Path, min_run: i32, max_run: i32) -> Result<u64> {
    let mut bytes = 0;
    for run in min_run..(max_run + 1) {
        if let Ok(meta) = construct_run_path(merger_path, run).metadata() {
            bytes += meta.len();
        }
    }
    Ok(bytes)
}

/// Traverse the set of runs and see how many events there are.
pub fn get_total_merger_events(merger_path: &Path, min_run: i32, max_run: i32) -> Result<u64> {
    let mut events = 0;
    for run in min_run..(max_run + 1) {
        let path = construct_run_path(merger_path, run);
        if let Ok(merger_file) = File::open(&path) {
            if let Ok(meta_group) = merger_file.group("meta") {
                let meta_data = meta_group.dataset("meta")?;
                let meta_array = meta_data.read_1d::<f64>()?;
                events += (meta_array[2] - meta_array[0]) as u64 + 1;
            } else if let Ok(event_group) = merger_file.group("events") {
                events += event_group.attr("max_event")?.read_scalar::<u64>()?
                    - event_group.attr("min_event")?.read_scalar::<u64>()?;
            }
        }
    }
    Ok(events)
}

/// Unified definition of a GET event from the merger
#[derive(Debug)]
pub struct GetEvent {
    pub traces: Array2<i16>,
    pub id: u32,
    pub timestamp: u64,
    pub timestamp_other: u64,
}

/// Unified definition of an FRIBDAQ event from the merger
#[derive(Debug)]
pub struct FribEvent {
    pub traces: Array2<u16>,
    pub coincidence: Array1<u16>,
    pub event: u32,
    pub timestamp: u32,
}

/// Unified definition of a complete event from the merger
#[derive(Debug)]
pub struct MergerEvent {
    pub get: Option<GetEvent>,
    pub frib: Option<FribEvent>,
    pub run_number: i32,
    pub event: u64,
}

/// Representation of a Reader for data from attpc_merger. It is
/// capable of determining which version of the merger produced the
/// data and then parsing it appropriately.
#[derive(Debug)]
pub struct MergerReader {
    merger_path: PathBuf,
    max_run: i32,
    version: MergerVersion,
    current_run: i32,
    current_file: File,
    current_event: u64,
    current_max_event: u64,
}

impl MergerReader {
    /// Create a new reader. The first run is opened and initialized.
    pub fn new(merger_path: &Path, min_run: i32, max_run: i32) -> Result<Self> {
        let first_file = File::open(construct_run_path(merger_path, min_run))?;
        let mut reader = Self {
            merger_path: merger_path.to_path_buf(),
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

    /// Read the next event from the run set.
    /// If the currently open run is finished, the next run that
    /// exists within the range is opened. If there is no more data
    /// to be read it returns a None.
    pub fn read_event(&mut self) -> Result<Option<MergerEvent>> {
        if self.current_event > self.current_max_event {
            let result = self.find_next_file()?;
            match result {
                Some(()) => (),
                None => {
                    return Ok(None);
                }
            }
        }

        let result = match self.version {
            MergerVersion::V020 => self.read_event_020(),
            MergerVersion::V010 => self.read_event_010(),
            MergerVersion::Invalid => Err(eyre!("Attempting to read event from invalid reader!")),
        };

        self.current_event += 1;

        result
    }

    /// Initialize the current file, and update our state
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
            self.version = MergerVersion::V020;
            let event_group = self.current_file.group("events")?;
            self.current_event = event_group.attr("min_event")?.read_scalar::<u64>()?;
            self.current_max_event = event_group.attr("max_event")?.read_scalar::<u64>()?;
        } else {
            return Err(eyre!("Invalid Merger Version!"));
        }

        Ok(())
    }

    /// Find the next available file in the run range.
    /// If there are no more runs, returns None.
    fn find_next_file(&mut self) -> Result<Option<()>> {
        let mut path;
        loop {
            self.current_run += 1;
            if self.current_run > self.max_run {
                return Ok(None);
            }
            path = construct_run_path(&self.merger_path, self.current_run);
            if !path.exists() {
                continue;
            }
            break;
        }
        self.current_file = File::open(path)?;
        self.init_file()?;
        Ok(Some(()))
    }

    /// Read an event from the modern merger format.
    fn read_event_020(&mut self) -> Result<Option<MergerEvent>> {
        let event_group = self
            .current_file
            .group("events")?
            .group(&format!("event_{}", self.current_event))?;

        let mut maybe_get = None;
        let mut maybe_frib = None;
        if let Ok(get_data) = event_group.dataset("get_traces") {
            maybe_get = Some(GetEvent {
                traces: get_data.read_2d()?,
                id: get_data.attr("id")?.read_scalar()?,
                timestamp: get_data.attr("timestamp")?.read_scalar()?,
                timestamp_other: get_data.attr("timestamp_other")?.read_scalar()?,
            });
        }
        if let Ok(frib_group) = event_group.group("frib_physics") {
            let frib_977 = frib_group.dataset("977")?;
            let frib_1903 = frib_group.dataset("1903")?;
            maybe_frib = Some(FribEvent {
                traces: frib_1903.read_2d()?,
                coincidence: frib_977.read_1d()?,
                event: frib_group.attr("event")?.read_scalar()?,
                timestamp: frib_group.attr("timestamp")?.read_scalar()?,
            })
        }
        Ok(Some(MergerEvent {
            get: maybe_get,
            frib: maybe_frib,
            run_number: self.current_run,
            event: self.current_event,
        }))
    }

    /// Read an event from the 0.1.0 merger format
    fn read_event_010(&mut self) -> Result<Option<MergerEvent>> {
        let mut maybe_get = None;
        let mut maybe_frib = None;
        let get_group = self.current_file.group("get")?;
        if let Ok(get_data) = get_group.dataset(&format!("evt{}_data", self.current_event)) {
            let get_header = get_group
                .dataset(&format!("evt{}_header", self.current_event))?
                .read_1d::<f64>()?;
            maybe_get = Some(GetEvent {
                traces: get_data.read_2d()?,
                id: get_header[0] as u32,
                timestamp: get_header[1] as u64,
                timestamp_other: get_header[2] as u64,
            });
        }
        let frib_evt_group = self.current_file.group("frib")?.group("evt")?;
        if let Ok(frib_1903_data) =
            frib_evt_group.dataset(&format!("evt{}_1903", self.current_event))
        {
            let frib_977_data =
                frib_evt_group.dataset(&format!("evt{}_977", self.current_event))?;
            let frib_header = frib_evt_group
                .dataset(&format!("evt{}_header", self.current_event))?
                .read_1d::<u32>()?;
            maybe_frib = Some(FribEvent {
                traces: frib_1903_data.read_2d()?,
                coincidence: frib_977_data.read_1d()?,
                event: frib_header[0],
                timestamp: frib_header[1],
            });
        }
        Ok(Some(MergerEvent {
            get: maybe_get,
            frib: maybe_frib,
            run_number: self.current_run,
            event: self.current_event,
        }))
    }
}
