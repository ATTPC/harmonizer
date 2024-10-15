use color_eyre::eyre::{eyre, Result};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    merger_path: PathBuf,
    harmonic_path: PathBuf,
    harmonic_size: u64,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Err(eyre!(
                "Attempted to load configuration from non-existant path: {}",
                path.display()
            ));
        }

        let yaml_str = std::fs::read_to_string(path)?;
        Ok(serde_yaml::from_str::<Self>(&yaml_str)?)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let yaml_str = serde_yaml::to_string(self)?;
        let mut file = std::fs::File::create(path)?;
        file.write_all(yaml_str.as_bytes())?;
        Ok(())
    }
}
