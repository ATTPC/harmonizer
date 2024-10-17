//! # harmonizer
//!
//! > We impose order on the chaos of organic evolution. You exist because we allow it. And you will end because we demand it.
//! >
//! > -- Sovereign, Mass Effect
//!
//! AT-TPC data is messy. Running analysis on messy data is hard.
//!
//! The harmonizer is an effort to impose order on the chaos of runs in real data. It takes in a set of merged AT-TPC runs, and re-organizes them into equal sized files.
//!
//! ## Installation
//!
//! The harmonizer is written in Rust and requires a Rust compiler. The Rust toolchain can be installed from [here](https://rust-lang.org).
//!
//! Once the Rust toolchain is installed, download the repository from GitHub
//!
//! ```bash
//! git clone https://github.com/ATTPC/harmonizer.git
//! ```
//!
//! From within the repository run
//!
//! ```bash
//! cargo install
//! ```
//!
//! This will install the harmonizer executable to your cargo installs and it will be available on your path as `harmonizer`.
//!
//! ## Use
//!
//! The harmonizer uses the following CLI:
//!
//! ```txt
//! harmonizer --config/-c /path/to/some/config.yml
//! ```
//!
//! where `/path/to/some/config.yml` should be replaced with the path to an appropriate configuration file. The harmonizer can generate a default template for you using the `new` command.
//!
//! ```txt
//! harmonizer --config/-c /path/to/some/config.yml new
//! ```
//!
//! ### Configuration
//!
//! Configurations are defined as the following YAML:
//!
//! ```yaml
//! merger_path: "/path/to/some/merger/data/"
//! harmonic_path: "/path/to/some/harmonic/data/"
//! harmonic_size_gb: 10
//! min_run: 55
//! max_run: 69
//! ```
//!
//! Some important notes:
//!
//! - The path given as the `harmonic_path` must exist before running the harmonizer
//! - The harmonic size is given in units of GB. This is the size of a harmonic run.
//! - Min run and max run are the range of run numbers (*merger run numbers*) to be harmonized. The range is inclusive; run numbers can be missing in the range.
//! - The harmonizer should **only ever be run on a set of runs from the same gas and beam combination**. If your range includes multiple gas/beams it will mix them together and it will become very difficult to disentangle these datasets.
//!
//! ### Output Format
//!
//! The harmonizer follows the current [attpc_merger](https://github.com/attpc_merger) format, with some minor changes. That format is
//!
//! ```txt
//! run_0001.h5
//! |---- events - min_event, max_event, version
//! |    |---- event_# - orig_run, orig_event
//! |    |    |---- get_traces(dset) - id, timestamp, timestamp_other
//! |    |    |---- frib_physics - id, timestamp
//! |    |    |    |---- 977(dset)
//! |    |    |    |---- 1903(dset)
//! ```
//!
//! The major differences:
//!
//! - Scalers are removed. The harmonizer takes all of the scalers over the run range and combines them into a single `scalers.parquet` file written to the harmonic path.
//! - Many of the top level attributes containing original run information are removed, as they are not relevant to the harmonic run.
//! - Each event has two new attributes, `orig_run` and `orig_event`. These are the original run number and event number for this event. These allow harmonized events to be traced back to their origins (and for downstream analyses to still operate over temporal changes).
//!
//! ## Why would you do this to me?
//!
//! Due to the scale of AT-TPC datasets, most analyses require some form of parallelization. Due to inherent limitations of the HDF5 format and the Python language, the simplest unit of work for a parallel analysis is a run file. However, runs are not balanced. Some runs are quite large, and some are basically empty. Balancing the load for parallel analysis is then quite a challenge, and in the case where you process each run in parallel, it is basically impossible.
//!
//! The harmonizer allows you to overcome these limitations. It will take your set of runs and split and combine them as needed such that each run contains the same total amount of data.
//!
//! Note that just because they have the same amount of data (in terms of size in bytes), does not mean each run will have *exactly* the same load in an analysis. Some events are garbage to be thrown out, some are really complicated, etc. The harmonizer doesn't know about any of that. Those are silly human concerns. The harmonizer only knows bytes.
//!
mod config;
mod reader;
mod scalers;
mod writer;

use clap::{Arg, Command};
use color_eyre::eyre::Result;
use config::Config;
use human_bytes::human_bytes;
use indicatif::{ProgressBar, ProgressStyle};
use reader::{get_total_merger_bytes, get_total_merger_events, MergerReader};
use scalers::process_scalers;
use std::path::PathBuf;
use writer::HarmonicWriter;

/// Main processing loop. Takes the config and harmonizes the data.
pub fn harmonize(config: Config) -> Result<()> {
    let total_events =
        get_total_merger_events(&config.merger_path, config.min_run, config.max_run)?;
    let progress = ProgressBar::new(total_events)
        .with_style(ProgressStyle::with_template(
            "{msg}: {bar:40.cyan/blue} [{human_pos}/{human_len} - {percent}%] (ETA: {eta}, Duration: {elapsed})",
        )?)
        .with_message("Progress");
    let mut reader = MergerReader::new(&config.merger_path, config.min_run, config.max_run)?;
    let mut writer = HarmonicWriter::new(&config.harmonic_path, config.get_harmonic_size())?;
    loop {
        let event = reader.read_event()?;
        match event {
            Some(e) => {
                writer.write(e)?;
                progress.inc(1);
            }
            None => break,
        }
    }
    writer.close()?;
    progress.finish();
    println!("Extracting scalers...");
    process_scalers(
        &config.merger_path,
        &config.harmonic_path,
        config.min_run,
        config.max_run,
    )?;
    Ok(())
}

/// Program entry point. Handles the CLI.
fn main() -> Result<()> {
    color_eyre::install()?;

    let cli = Command::new("harmonizer")
        .arg_required_else_help(true)
        .subcommand(Command::new("new").about("Create a new template config file"))
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .help("Path to a configuration file (YAML)"),
        )
        .get_matches();

    println!("--------------------- AT-TPC Harmonizer ---------------------");
    let config_path = PathBuf::from(cli.get_one::<String>("config").expect("We require args"));

    // Handle the new subcommand
    if let Some(("new", _)) = cli.subcommand() {
        println!(
            "Making a template configuration file at {}...",
            config_path.display()
        );
        Config::default().save(&config_path)?;
        println!("Done.");
        println!("-------------------------------------------------------------");
        return Ok(());
    }

    let config = Config::load(&config_path)?;
    println!(
        "Successfully loaded configuration from {}",
        config_path.display()
    );

    if !config.merger_path.exists() {
        println!(
            "Merger path {} does not exist! Quitting.",
            config.merger_path.display()
        );
        println!("-------------------------------------------------------------");
    } else if !config.harmonic_path.exists() {
        println!(
            "Harmonic path {} does not exist! Please create it before running the harmonizer.",
            config.harmonic_path.display()
        );
        println!("-------------------------------------------------------------");
    }

    println!(
        "Total amount of data to be harmonized: {}",
        human_bytes(
            get_total_merger_bytes(&config.merger_path, config.min_run, config.max_run)? as f64
        )
    );
    println!("Harmonizing...");
    harmonize(config)?;
    println!("Complete.");

    println!("-------------------------------------------------------------");

    Ok(())
}
