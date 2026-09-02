//! veilid-vc — a latency harness for Veilid's `app_message`.
//!
//! Before designing a call protocol on top of Veilid it is worth knowing what
//! the transport actually does under a media-shaped load: 50 small packets a
//! second, sustained, through a private route. Nobody has published those
//! numbers. This measures them.
//!
//!   Terminal one:   cargo run --release -- listen
//!   Terminal two:   cargo run --release -- probe --connect <blob>
//!
//! The same route machinery moves files, which is the other thing you want to
//! know before building on this transport:
//!
//!   Terminal one:   cargo run --release -- recv --out ./inbox
//!   Terminal two:   cargo run --release -- send --connect <blob> photo.jpg
//!
//! Run it against a private dev network first (see the veilid repo's
//! dev-setup/dev-network-setup.md); the public network is a moving target and
//! you want a baseline you control.

// veilid-core's async call stack nests deeply enough that auto-trait resolution
// runs past the default limit when we spawn an `app_message` future.
#![recursion_limit = "256"]

/// The single page the local server hands the browser. It renders itself
/// from the JSON API; the node never generates markup.
pub const APP_HTML: &str = include_str!("app.html");

mod proto;
mod roles;
mod social;
mod node;
mod stats;
mod web;
mod transfer;

use clap::{Parser, Subcommand};
use roles::{ProbeConfig, RouteParams, Stamped};
use transfer::{RecvConfig, RouteSource, SendConfig};
use std::sync::Arc;
use tokio::sync::mpsc;
use veilid_core::*;

#[derive(Parser)]
#[command(version, about = "Measure what Veilid does to a media-shaped packet stream")]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Logging verbosity: -d info, -dd debug, -ddd trace
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    debug: u8,

    /// Private route hop count. Lower is faster and less private; omit to use
    /// the node default.
    #[arg(long, global = true)]
    hops: Option<usize>,

    /// Prefer a low-latency route over a long-lived one.
    #[arg(long, global = true, default_value = "low-latency")]
    stability: StabilityArg,

    /// Ordering preference. Media wants unordered — a late packet is worthless.
    #[arg(long, global = true, default_value = "prefer-unordered")]
    sequencing: SequencingArg,

    /// Drop the safety route. Lowest latency Veilid can offer, at the cost of
    /// revealing this node to the far end.
    #[arg(long = "unsafe", global = true)]
    unsafe_routing: bool,
}

#[derive(Subcommand)]
enum Command {
    /// Create a private route, print its blob, and echo back whatever arrives.
    Listen,
    /// Send timestamped packets at a listener and report what came back.
    Probe {
        /// Base64 route blob printed by the listener.
        #[arg(long)]
        connect: String,

        /// Packets per second. 50 matches 20 ms Opus frames.
        #[arg(long, default_value_t = 50)]
        rate: u64,

        /// Total bytes per packet, padded. Clamped to Veilid's 32768 cap.
        /// 60-ish is a real Opus voice frame; 1200 is a video packet.
        #[arg(long, default_value_t = 64)]
        size: usize,

        /// Seconds to run. 0 runs until Ctrl-C.
        #[arg(long, default_value_t = 60)]
        duration: u64,

        /// Write one row per echoed packet here for later analysis.
        #[arg(long)]
        csv: Option<std::path::PathBuf>,
    },
    /// Create a private route, print its blob, and write whatever files arrive.
    Recv {
        /// Directory for completed files. Created if it does not exist.
        #[arg(long, default_value = "inbox")]
        out: std::path::PathBuf,

        /// Refuse any single transfer larger than this. The whole file is held
        /// in memory, and anyone holding the blob can open a transfer.
        #[arg(long, default_value_t = 256 * 1024 * 1024)]
        max_bytes: u64,

        /// How many private routes to keep published at once. Spares let a
        /// sender fail over locally instead of going back to the DHT.
        #[arg(long, default_value_t = 3)]
        pool: usize,

        /// Roughly how many seconds an idle route may live before it is
        /// replaced ahead of failing. Off by default: rotation churns the
        /// spare routes that sender failover depends on, and measured worse
        /// than leaving the pool alone. Kept for experimentation.
        #[arg(long, default_value_t = 0)]
        rotate_secs: u64,
    },
    /// Run a social node: your wall in the DHT, and a local page to drive it.
    Serve {
        /// Port for the local page. Bound to loopback only.
        #[arg(long, default_value_t = 8080)]
        port: u16,

        /// How many private routes to publish for people to message you on.
        #[arg(long, default_value_t = 2)]
        pool: usize,
    },
    /// Push files at a receiver over its private route.
    Send {
        /// Base64 route blob printed by the receiver. Names one route, so the
        /// run ends if that route dies. Prefer --rendezvous.
        #[arg(long, conflicts_with = "rendezvous", required_unless_present = "rendezvous")]
        connect: Option<String>,

        /// DHT record key printed by the receiver. The receiver rewrites it on
        /// every route rotation, so a dead route is re-read rather than fatal.
        #[arg(long)]
        rendezvous: Option<String>,

        /// Files to send, in order.
        #[arg(required = true)]
        files: Vec<std::path::PathBuf>,

        /// Payload bytes per chunk. Bigger is fewer round trips; smaller
        /// survives a lossy path better. Clamped to what one app_message holds.
        #[arg(long, default_value_t = 16384)]
        chunk: usize,

        /// Chunks per second. This times --chunk is the offered throughput.
        #[arg(long, default_value_t = 20)]
        rate: u64,

        /// How long to wait after a pass before asking what is missing.
        /// Wants to be a couple of round trips.
        #[arg(long, default_value_t = 1500)]
        settle_ms: u64,

        /// Give up after this many repair passes.
        #[arg(long, default_value_t = 20)]
        rounds: u32,
    },
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum StabilityArg {
    LowLatency,
    Reliable,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum SequencingArg {
    PreferUnordered,
    PreferOrdered,
    EnsureOrdered,
}

impl From<StabilityArg> for Stability {
    fn from(a: StabilityArg) -> Self {
        match a {
            StabilityArg::LowLatency => Stability::LowLatency,
            StabilityArg::Reliable => Stability::Reliable,
        }
    }
}

impl From<SequencingArg> for Sequencing {
    fn from(a: SequencingArg) -> Self {
        match a {
            SequencingArg::PreferUnordered => Sequencing::PreferUnordered,
            SequencingArg::PreferOrdered => Sequencing::PreferOrdered,
            SequencingArg::EnsureOrdered => Sequencing::EnsureOrdered,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let (done_tx, done_rx) = mpsc::channel(1);
    ctrlc::set_handler(move || {
        let _ = done_tx.try_send(());
    })?;

    let logs = VeilidTracing::stderr();
    logs.try_apply_default_env()?;
    logs.try_apply_facility_level(
        "#common",
        match cli.debug {
            1 => VeilidConfigLogLevel::Info,
            2 => VeilidConfigLogLevel::Debug,
            3.. => VeilidConfigLogLevel::Trace,
            _ => VeilidConfigLogLevel::Warn,
        },
    )?;

    let params = RouteParams {
        hops: cli.hops,
        stability: cli.stability.into(),
        sequencing: cli.sequencing.into(),
        unsafe_routing: cli.unsafe_routing,
    };

    // The two roles get separate namespaces so both can run on one machine
    // against the same state directory without fighting over it.
    let namespace = match &cli.command {
        Command::Listen => "listen",
        Command::Probe { .. } => "probe",
        Command::Recv { .. } => "recv",
        Command::Serve { .. } => "serve",
        Command::Send { .. } => "send",
    };

    // Updates arrive on a callback with no async context, so stamp each one on
    // arrival and hand it to the role over a channel. Log updates are dropped
    // here rather than forwarded — at trace level they would swamp the queue
    // the measurements travel on.
    let (update_tx, update_rx) = mpsc::unbounded_channel::<Stamped>();
    let callback = move |update: VeilidUpdate| {
        if matches!(update, VeilidUpdate::Log(_)) {
            return;
        }
        let _ = update_tx.send(Stamped { at_us: proto::now_us(), update });
    };

    let api = start_node(Arc::new(callback), config(namespace)?, namespace).await?;
    api.attach().await?;

    let result = match cli.command {
        Command::Listen => roles::listen(api.clone(), update_rx, params, done_rx).await,
        Command::Probe { connect, rate, size, duration, csv } => {
            let blob = decode_blob(&connect)?;
            let cfg = ProbeConfig {
                rate: rate.max(1),
                size: roles::clamp_size(size),
                duration_secs: duration,
                csv,
            };
            roles::probe(api.clone(), update_rx, params, cfg, blob, done_rx).await
        }
        Command::Recv { out, max_bytes, pool, rotate_secs } => {
            let cfg = RecvConfig {
                out_dir: out,
                max_bytes,
                rendezvous_file: state_dir().join(".veilid/recv/rendezvous"),
                pool: pool.clamp(1, 8),
                rotate_secs,
            };
            transfer::recv(api.clone(), update_rx, params, cfg, done_rx).await
        }
        Command::Serve { port, pool } => {
            let cfg = node::ServeConfig {
                port,
                identity_file: state_dir().join(".veilid/serve/identity"),
                follows_file: state_dir().join(".veilid/serve/follows"),
                pool: pool.clamp(1, 8),
            };
            node::serve(api.clone(), update_rx, params, cfg, done_rx).await
        }
        Command::Send { connect, rendezvous, files, chunk, rate, settle_ms, rounds } => {
            let source = match (connect, rendezvous) {
                (_, Some(key)) => RouteSource::Rendezvous(key),
                (Some(blob), None) => RouteSource::Blob(decode_blob(&blob)?),
                (None, None) => return Err("need --connect or --rendezvous".into()),
            };
            let cfg = SendConfig {
                files,
                chunk,
                rate: rate.max(1),
                settle_ms,
                max_rounds: rounds,
            };
            transfer::send(api.clone(), update_rx, params, cfg, source, done_rx).await
        }
    };

    api.shutdown().await;
    result
}

/// Start the node, waiting out a protected store that has not been released yet.
///
/// The insecure keyring is one file per namespace, and a node that was just
/// stopped keeps it until its shutdown finishes -- which outlives the process
/// vanishing from `ps`. Restarting promptly therefore fails with "failed to
/// create insecure keyring", which says nothing about the actual cause: either
/// another `veilid-vc <namespace>` is running, or the last one is still letting
/// go. The first is worth reporting plainly; the second is worth waiting for.
async fn start_node(
    callback: UpdateCallback,
    config: VeilidConfig,
    namespace: &str,
) -> Result<VeilidAPI, Box<dyn std::error::Error>> {
    const ATTEMPTS: u32 = 15;
    let mut waited = false;
    for attempt in 1..=ATTEMPTS {
        let err = match Box::pin(api_startup(callback.clone(), config.clone())).await {
            Ok(api) => {
                if waited {
                    eprintln!(" released.");
                }
                return Ok(api);
            }
            Err(e) => e,
        };
        if !err.to_string().contains("keyring") {
            return Err(err.into());
        }
        if attempt == ATTEMPTS {
            return Err(format!(
                "could not open the protected store for `{namespace}`. Another \
                 `veilid-vc {namespace}` is probably still running -- each subcommand keeps its \
                 own keyring, so two of the same one collide. Find it with \
                 `ps -eo pid,args | grep veilid-vc` and stop it with `kill -INT <pid>`."
            )
            .into());
        }
        if !waited {
            eprint!("Waiting for the previous {namespace} node to release its keyring...");
            waited = true;
        } else {
            eprint!(".");
        }
        let _ = std::io::Write::flush(&mut std::io::stderr());
        tokio::time::sleep(std::time::Duration::from_millis(700)).await;
    }
    unreachable!()
}

/// Where per-namespace state lives: beside the executable, like the veilid
/// stores below.
fn state_dir() -> std::path::PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_owned()))
        .unwrap_or_else(|| ".".into())
}

fn decode_blob(connect: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    data_encoding::BASE64
        .decode(connect.trim().as_bytes())
        .map_err(|e| format!("that does not look like a route blob: {e}").into())
}

fn config(namespace: &str) -> Result<VeilidConfig, Box<dyn std::error::Error>> {
    let dir = state_dir();

    Ok(VeilidConfig {
        program_name: "veilid-vc".into(),
        namespace: namespace.to_owned(),
        protected_store: VeilidConfigProtectedStore {
            // Fine for a measurement tool that holds nothing worth protecting.
            // Do not carry this into anything that stores a real identity.
            always_use_insecure_storage: true,
            directory: dir
                .join(format!(".veilid/{namespace}/protected_store"))
                .to_string_lossy()
                .to_string(),
            ..Default::default()
        },
        table_store: VeilidConfigTableStore {
            directory: dir
                .join(format!(".veilid/{namespace}/table_store"))
                .to_string_lossy()
                .to_string(),
            ..Default::default()
        },
        ..Default::default()
    })
}
