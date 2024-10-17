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
