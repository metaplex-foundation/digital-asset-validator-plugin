mod accounts_selector;
mod geyser;
mod mpl_metadata;

use crate::geyser::GeyserDumper;
use clap::Parser;
use indicatif::{ProgressBar, ProgressBarIter, ProgressStyle};
use plerkle_snapshot::archived::ArchiveSnapshotExtractor;
use plerkle_snapshot::unpacked::UnpackedSnapshotExtractor;
use plerkle_snapshot::{
    append_vec_iter, AppendVecIterator, ReadProgressTracking, SnapshotExtractor,
};
use reqwest::blocking::Response;
use std::fs::File;
use std::io::{IoSliceMut, Read};
use std::path::{Path, PathBuf};
use tracing::{info, warn};

use self::accounts_selector::{AccountsSelector, AccountsSelectorConfig};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(help = "Snapshot source (unpacked snapshot, archive file, or HTTP link)")]
    source: String,
    #[clap(
        long,
        default_value_t = 0,
        help = "Throttle nanoseconds between the dumping of individual accounts"
    )]
    throttle_nanos: u64,
    #[clap(long, help = "Path to accounts selector config (optional)")]
    accounts_selector_config: Option<PathBuf>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env_filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .event_format(tracing_subscriber::fmt::format::json())
        .init();
    if let Err(e) = dotenvy::dotenv() {
        warn!(error = %e, "Error initializing .env, make sure all required env is available...");
    }

    let args = Args::parse();
    let accounts_selector_config = args
        .accounts_selector_config
        .and_then(|path| {
            std::fs::read(path).ok().map(|slice| {
                serde_json::from_slice::<AccountsSelectorConfig>(&slice)
                    .expect("could not decode accounts selector config!")
            })
        })
        .unwrap_or_default();
    let accounts_selector = AccountsSelector::new(accounts_selector_config);

    let mut loader = SupportedLoader::new(&args.source, Box::new(LoadProgressTracking {}))?;

    let mut dumper = GeyserDumper::new(args.throttle_nanos, accounts_selector).await;
    for append_vec in loader.iter() {
        let append_vec = append_vec.unwrap();
        let slot = append_vec.get_slot();

        for account in append_vec_iter(append_vec) {
            dumper
                .dump_account(account, slot)
                .await
                .expect("failed to dump account");
        }
    }

    info!("Done! Accounts: {}", dumper.accounts_count);

    dumper.force_flush().await;

    Ok(())
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
