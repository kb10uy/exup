use std::{
    fs::File,
    io::{BufReader, Read},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{bail, Context, Result};
use clap::Parser;
use flate2::read::GzDecoder;
use tar::Archive;
use tokio::{
    fs::{create_dir_all, write},
    spawn,
    sync::{OwnedSemaphorePermit, Semaphore},
};

/// unitypackage extractor
#[derive(Debug, Clone, Parser)]
#[clap(about, version, author)]
pub struct Arguments {
    /// unitypackage file to extract.
    pub unity_package: PathBuf,

    /// Target directory to extract to.
    pub root_directory: PathBuf,

    /// Remove path prefix and extract assets only that have specified prefix.
    #[clap(short, long)]
    pub prefix: Option<String>,

    /// Extracts .meta files.
    #[clap(short, long)]
    pub meta: bool,

    /// Dry-run mode. Never actually extract files.
    #[clap(short, long)]
    pub dry: bool,

    /// Extraction concurrency (default: 256).
    #[clap(short = 'c', long, default_value = "256")]
    pub max_concurrency: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Arguments::parse();

    let mut up_archive = {
        let up_file = File::open(args.unity_package)?;
        let up_tar_stream = GzDecoder::new(BufReader::new(up_file));
        Archive::new(up_tar_stream)
    };

    let mut prefix = String::new();
    let mut asset_bytes = vec![];
    let mut asset_meta_bytes = vec![];
    let mut asset_path = String::new();

    let mut asset_met = true;
    let mut asset_meta_met = true;
    let mut asset_path_met = true;
    // let mut asset_preview_met = true;

    let extract_gate = Arc::new(Semaphore::new(args.max_concurrency + 1));
    let parent_permit = extract_gate.clone().acquire_owned().await?;

    for entry in up_archive.entries()? {
        let mut entry = entry?;
        let entry_path = entry.path()?.into_owned();
        let entry_path_str = entry_path.to_string_lossy().to_string();

        // new asset paths start
        if prefix.is_empty() || !entry_path_str.starts_with(&prefix) {
            prefix = entry_path_str.to_string();
            asset_bytes.clear();
            asset_meta_bytes.clear();
            asset_path.clear();

            asset_met = false;
            asset_meta_met = false;
            asset_path_met = false;
            // asset_preview_met = false;

            continue;
        }

        let Some(filename) = entry_path_str.strip_prefix(&prefix) else {
            bail!("invalid package filename detected: {entry_path_str}");
        };
        match filename {
            "asset" => {
                asset_met = true;
                entry.read_to_end(&mut asset_bytes)?;
            }
            "asset.meta" => {
                asset_meta_met = true;
                entry.read_to_end(&mut asset_meta_bytes)?;
            }
            "pathname" => {
                asset_path_met = true;
                entry.read_to_string(&mut asset_path)?;
            }
            "preview.png" => {
                // asset_preview_met = true;
            }
            _ => bail!("unknown file contained: {filename}"),
        }

        // asset data has all met
        if asset_met && asset_meta_met && asset_path_met {
            let asset_path = if let Some(extract_prefix) = args.prefix.as_deref() {
                let Ok(stripped) = Path::new(&asset_path).strip_prefix(extract_prefix) else {
                    continue;
                };
                stripped.to_string_lossy().to_string()
            } else {
                asset_path.clone()
            };

            println!("Extracting \"{asset_path}\" ({} bytes)", asset_bytes.len());
            let permit = extract_gate.clone().acquire_owned().await?;

            if args.dry {
                spawn(async {
                    drop(permit);
                });
            } else {
                spawn(extract_task(
                    permit,
                    args.root_directory.clone(),
                    asset_path,
                    asset_bytes,
                    args.meta.then_some(asset_meta_bytes),
                ));
            }

            prefix.clear();
            asset_bytes = vec![];
            asset_meta_bytes = vec![];
        }
    }

    // wait all file extraction
    drop(parent_permit);
    let _ = extract_gate
        .acquire_many_owned((args.max_concurrency + 1) as u32)
        .await?;

    Ok(())
}

async fn extract_task(
    permit: OwnedSemaphorePermit,
    base_path: PathBuf,
    asset_path: String,
    asset_bytes: Vec<u8>,
    asset_meta_bytes: Option<Vec<u8>>,
) -> Result<()> {
    let asset_fullpath = base_path.join(&asset_path);
    let asset_dir = asset_fullpath.parent().context("invalid root path")?;

    create_dir_all(asset_dir).await?;
    write(asset_fullpath, &asset_bytes).await?;

    if let Some(meta_bytes) = asset_meta_bytes {
        let meta_path = base_path.join(format!("{asset_path}.meta"));
        write(meta_path, &meta_bytes).await?;
    }

    drop(permit);
    Ok(())
}
