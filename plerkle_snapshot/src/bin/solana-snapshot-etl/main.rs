use crate::geyser::GeyserDumper;
use clap::Parser;
use indicatif::{ProgressBar, ProgressBarIter, ProgressStyle};
use log::{error, info};
use plerkle::geyser_plugin_nft::Plerkle;
use plerkle_snapshot::archived::ArchiveSnapshotExtractor;
use plerkle_snapshot::parallel::AppendVecConsumer;
use plerkle_snapshot::unpacked::UnpackedSnapshotExtractor;
use plerkle_snapshot::{AppendVecIterator, ReadProgressTracking, SnapshotExtractor};
use reqwest::blocking::Response;
use serde::Deserialize;
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin;
use std::fs::File;
use std::io::{IoSliceMut, Read};
use std::path::Path;
use std::sync::Arc;

mod geyser;
mod mpl_metadata;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(help = "Snapshot source (unpacked snapshot, archive file, or HTTP link)")]
    source: String,
    #[clap(long, help = "Load Geyser plugin from given config file")]
    geyser: String,
}

fn main() {
    plerkle::config::init_logger();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("solana-snapshot-etl")
        .build()
        .expect("create tokio runtime");
    if let Err(e) = run(runtime) {
        error!("{}", e);
        std::process::exit(1);
    }
}

fn run(runtime: tokio::runtime::Runtime) -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let mut loader = SupportedLoader::new(&args.source, Box::new(LoadProgressTracking {}))?;
    info!("Dumping to Geyser plugin: {}", &args.geyser);

    let cfg = Config::read(&args.geyser)
        .map_err(|e| format!("Config error: {}", e.to_string()))
        .unwrap();
    let mut plugin = Plerkle::new_for_etl(Arc::new(runtime));
    plugin.on_load(&args.geyser, false).expect("setup plugin");

    assert!(
        plugin.account_data_notifications_enabled(),
        "Geyser plugin does not accept account data notifications"
    );

    let mut dumper = GeyserDumper::new(plugin, cfg.throttle_nanos);
    for append_vec in loader.iter() {
        match append_vec {
            Ok(v) => {
                dumper.on_append_vec(v).unwrap_or_else(|error| {
                    error!("on_append_vec: {:?}", error);
                });
            }
            Err(error) => error!("append_vec: {:?}", error),
        };
    }

    dumper.finish();
    info!("Done!");

    Ok(())
}

#[derive(Deserialize)]
pub struct Config {
    // path to the built Geyser binary
    pub libpath: String,
    // path to the Geyser config file
    pub geyser_conf_path: String,
    pub throttle_nanos: u64,
}

impl Config {
    pub fn read(path: &str) -> Result<Self, std::io::Error> {
        let data = std::fs::read_to_string(path)?;
        let c: Config = serde_json::from_str(data.as_str())?;

        Ok(c)
    }
}

struct LoadProgressTracking {}

impl ReadProgressTracking for LoadProgressTracking {
    fn new_read_progress_tracker(
        &self,
        _: &Path,
        rd: Box<dyn Read>,
        file_len: u64,
    ) -> Box<dyn Read> {
        let progress_bar = ProgressBar::new(file_len).with_style(
            ProgressStyle::with_template(
                "{prefix:>10.bold.dim} {spinner:.green} [{bar:.cyan/blue}] {bytes}/{total_bytes} ({percent}%)",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        progress_bar.set_prefix("manifest");
        Box::new(LoadProgressTracker {
            rd: progress_bar.wrap_read(rd),
            progress_bar,
        })
    }
}

struct LoadProgressTracker {
    progress_bar: ProgressBar,
    rd: ProgressBarIter<Box<dyn Read>>,
}

impl Drop for LoadProgressTracker {
    fn drop(&mut self) {
        self.progress_bar.finish()
    }
}

impl Read for LoadProgressTracker {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.rd.read(buf)
    }

    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> std::io::Result<usize> {
        self.rd.read_vectored(bufs)
    }

    fn read_to_string(&mut self, buf: &mut String) -> std::io::Result<usize> {
        self.rd.read_to_string(buf)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        self.rd.read_exact(buf)
    }
}

pub enum SupportedLoader {
    Unpacked(UnpackedSnapshotExtractor),
    ArchiveFile(ArchiveSnapshotExtractor<File>),
    ArchiveDownload(ArchiveSnapshotExtractor<Response>),
}

impl SupportedLoader {
    fn new(
        source: &str,
        progress_tracking: Box<dyn ReadProgressTracking>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if source.starts_with("http://") || source.starts_with("https://") {
            Self::new_download(source)
        } else {
            Self::new_file(source.as_ref(), progress_tracking).map_err(Into::into)
        }
    }

    fn new_download(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let resp = reqwest::blocking::get(url)?;
        let loader = ArchiveSnapshotExtractor::from_reader(resp)?;
        info!("Streaming snapshot from HTTP");
        Ok(Self::ArchiveDownload(loader))
    }

    fn new_file(
        path: &Path,
        progress_tracking: Box<dyn ReadProgressTracking>,
    ) -> plerkle_snapshot::Result<Self> {
        Ok(if path.is_dir() {
            info!("Reading unpacked snapshot");
            Self::Unpacked(UnpackedSnapshotExtractor::open(path, progress_tracking)?)
        } else {
            info!("Reading snapshot archive");
            Self::ArchiveFile(ArchiveSnapshotExtractor::open(path)?)
        })
    }
}

impl SnapshotExtractor for SupportedLoader {
    fn iter(&mut self) -> AppendVecIterator<'_> {
        match self {
            SupportedLoader::Unpacked(loader) => Box::new(loader.iter()),
            SupportedLoader::ArchiveFile(loader) => Box::new(loader.iter()),
            SupportedLoader::ArchiveDownload(loader) => Box::new(loader.iter()),
        }
    }
}
