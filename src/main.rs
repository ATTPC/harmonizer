mod config;
mod reader;

use color_eyre::eyre::Result;
fn main() -> Result<()> {
    color_eyre::install()?;

    Ok(())
}
