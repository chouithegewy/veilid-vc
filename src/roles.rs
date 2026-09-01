//! The two ends of the measurement.
//!
//! `listen` stands up a private route, prints its blob, and echoes back
//! everything it receives. `probe` imports that blob, stands up a return route
//! of its own, and paces timestamped packets at it.
//!
//! The echo is what makes the numbers honest. `app_message` is one-way and
//! unacknowledged, so a bare send tells you nothing about whether it landed;
//! bouncing each packet back off the far end gives an exact delivery count and
//! a round-trip time that does not depend on the two machines agreeing about
//! what time it is.

use crate::proto::{now_us, Packet, MAX_APP_MESSAGE, PROBE_LEN};
use crate::stats::Run;
use std::future::Future;
use std::io::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Semaphore};
use veilid_core::*;

/// A Veilid update, stamped the moment it reached us.
///
/// The stamp is taken inside the update callback rather than after the channel
/// hop, so queueing inside our own process does not get counted as network
/// delay. At 50 packets a second that difference is not academic.
pub struct Stamped {
    pub at_us: u64,
    pub update: VeilidUpdate,
}

pub type Updates = mpsc::UnboundedReceiver<Stamped>;

/// How the private routes and the safety route should be built.
#[derive(Clone)]
pub struct RouteParams {
    pub hops: Option<usize>,
    pub stability: Stability,
    pub sequencing: Sequencing,
    pub unsafe_routing: bool,
}

impl RouteParams {
    pub fn describe(&self) -> String {
        let safety = if self.unsafe_routing {
            "unsafe (no safety route; the far end learns this node)".to_string()
        } else {
            format!("safe, {} hop(s)", self.hops.unwrap_or(1))
        };
        format!(
            "  safety                     {safety}\n  \
             private route              {}\n  \
             stability / sequencing     {:?} / {:?}",
            self.hops
                .map(|h| format!("{h} hop(s), custom"))
                .unwrap_or_else(|| "node default".to_string()),
            self.stability,
            self.sequencing,
        )
    }
}

/// Everything the prober needs that the responder does not.
pub struct ProbeConfig {
    pub rate: u64,
    pub size: usize,
    pub duration_secs: u64,
    pub csv: Option<std::path::PathBuf>,
}

/// Retry past `TryAgain`, which is what the API returns while the node is still
/// finding its feet on the network. Everything else is a real error.
pub async fn try_again_loop<R, F: Future<Output = VeilidAPIResult<R>>>(
    what: &str,
    f: impl Fn() -> F,
) -> VeilidAPIResult<R> {
    let mut waiting = false;
    loop {
        match f().await {
            Ok(v) => {
                if waiting {
                    eprintln!(" ready.");
                }
                return Ok(v);
            }
            Err(VeilidAPIError::TryAgain { message: _ }) => {
                if !waiting {
                    eprint!("Waiting for network ({what})...");
                    waiting = true;
                } else {
                    eprint!(".");
                }
                let _ = std::io::stderr().flush();
                tools::sleep(1000).await;
            }
            Err(e) => {
                if waiting {
                    eprintln!();
                }
                return Err(e);
            }
        }
    }
}

pub async fn create_route(api: &VeilidAPI, p: &RouteParams) -> VeilidAPIResult<RouteBlob> {
    match p.hops {
        // A custom route is the only way to pin the hop count, which is the
        // single biggest lever on latency here.
        Some(hop_count) => {
            api.new_custom_private_route(PrivateSpec {
                crypto_kinds: VALID_CRYPTO_KINDS.to_vec(),
                hop_count,
                stability: p.stability,
                sequencing: p.sequencing,
            })
            .await
        }
        None => api.new_private_route().await,
    }
}

pub fn routing_context(api: &VeilidAPI, p: &RouteParams) -> VeilidAPIResult<RoutingContext> {
    let rc = api.routing_context()?;
    if p.unsafe_routing {
        rc.with_safety(SafetySelection::Unsafe(p.sequencing))
    } else {
        rc.with_safety(SafetySelection::Safe(SafetySpec {
            preferred_route: None,
            hop_count: p.hops.unwrap_or(1),
            stability: p.stability,
            sequencing: p.sequencing,
        }))
    }
}

/// Whether one spawned send made it out of the node.
type SendResult = bool;

// ---------------------------------------------------------------------------
// responder
// ---------------------------------------------------------------------------

pub async fn listen(
    api: VeilidAPI,
    mut updates: Updates,
    params: RouteParams,
    mut done: mpsc::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let rc = routing_context(&api, &params)?;

    let RouteBlob { route_id, blob } =
        try_again_loop("creating route", || async { create_route(&api, &params).await }).await?;

    println!("Route ready: {route_id}\n");
    println!("Point a prober at it with:\n");
    println!(
        "  cargo run --release -- probe --connect {}\n",
        data_encoding::BASE64.encode(&blob)
    );
    println!("Echoing probes. Ctrl-C for the report.\n");

    let mut run = Run::default();
    let started = Instant::now();
    // Bounded so a stalled network cannot grow an unbounded pile of tasks; a
    // refused permit is recorded rather than silently awaited.
    let inflight = Arc::new(Semaphore::new(256));
    let (send_tx, mut send_rx) = mpsc::unbounded_channel::<SendResult>();

    // Where to echo to. Set by the prober's Hello, replaced on every rotation.
    let mut return_route: Option<RouteId> = None;
    let mut report_tick = tokio::time::interval(Duration::from_secs(5));
    report_tick.tick().await;

    loop {
        tokio::select! {
            Some(Stamped { at_us, update }) = updates.recv() => {
                match update {
                    VeilidUpdate::AppMessage(msg) => {
                        match Packet::decode(msg.message()) {
                            Some(Packet::Hello { return_route: blob }) => {
                                match api.import_remote_private_route(blob) {
                                    Ok(id) => {
                                        println!("Prober return route: {id}");
                                        return_route = Some(id);
                                    }
                                    Err(e) => eprintln!("could not import the prober's route: {e}"),
                                }
                            }
                            Some(Packet::Probe { seq, t0 }) => {
                                let fresh = run.arrivals.record(seq);
                                // Transit crosses two clocks, so it is only
                                // meaningful as a series; jitter reads the
                                // differences, which cancel the offset.
                                let transit = at_us as i64 - t0 as i64;
                                if fresh {
                                    run.forward_jitter.update(transit);
                                    run.forward.push(transit.max(0) as u64);
                                }
                                if let Some(target) = return_route.clone() {
                                    // Echo at the same size as the probe, so the
                                    // return leg carries the same load as the
                                    // forward one.
                                    let pad = msg.message().len();
                                    let rc = rc.clone();
                                    let tx = send_tx.clone();
                                    let Ok(permit) = inflight.clone().try_acquire_owned() else {
                                        run.send_dropped += 1;
                                        continue;
                                    };
                                    tokio::spawn(async move {
                                        let _permit = permit;
                                        let pkt = Packet::Echo { seq, t0, t1: at_us, t2: now_us() };
                                        let ok = rc
                                            .app_message(Target::RouteId(target), pkt.encode(pad))
                                            .await
                                            .is_ok();
                                        let _ = tx.send(ok);
                                    });
                                }
                            }
                            _ => {}
                        }
                    }
                    VeilidUpdate::RouteChange(change) => {
                        note_route_change(&mut run, started, &change, &route_id, return_route.as_ref());
                        if change.dead_routes.contains(&route_id) {
                            println!("\nOur route died. The prober's blob is now stale; \
                                      restart to hand out a fresh one.");
                            break;
                        }
                    }
                    VeilidUpdate::Shutdown => break,
                    _ => {}
                }
            }
            Some(ok) = send_rx.recv() => {
                run.sent += 1;
                if !ok { run.send_errors += 1; }
            }
            _ = report_tick.tick() => {
                let (lost, pct) = run.arrivals.inferred_loss();
                println!(
                    "[{:>6.1}s] recv {:>6}  lost~{lost} ({pct:.1}%)  echoed {:>6}  fwd jitter {:>6.1}ms",
                    started.elapsed().as_secs_f64(),
                    run.arrivals.unique(),
                    run.sent,
                    run.forward_jitter.ms(),
                );
            }
            _ = done.recv() => break,
        }
    }

    println!(
        "{}",
        run.responder_report(&params.describe(), started.elapsed().as_secs_f64())
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// prober
// ---------------------------------------------------------------------------

pub async fn probe(
    api: VeilidAPI,
    mut updates: Updates,
    params: RouteParams,
    cfg: ProbeConfig,
    remote_blob: Vec<u8>,
    mut done: mpsc::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let rc = routing_context(&api, &params)?;

    // A route of our own for the far end to echo back on.
    let RouteBlob { route_id: mut my_route, blob: my_blob } =
        try_again_loop("creating return route", || async { create_route(&api, &params).await })
            .await?;

    let remote = try_again_loop("importing remote route", || async {
        api.import_remote_private_route(remote_blob.clone())
    })
    .await?;

    println!("Return route: {my_route}");
    println!("Remote route: {remote}");

    // Tell the far end where to echo. Retried, because this one message has to
    // land or the whole run measures nothing.
    let hello = Packet::Hello { return_route: my_blob }.encode(0);
    try_again_loop("sending hello", || async {
        rc.app_message(Target::RouteId(remote.clone()), hello.clone()).await
    })
    .await?;
    println!("Hello delivered. Probing at {}/s, {} byte packets.\n", cfg.rate, cfg.size);

    let mut csv = match &cfg.csv {
        Some(path) => {
            let mut f = std::io::BufWriter::new(std::fs::File::create(path)?);
            writeln!(f, "seq,t0_us,t1_us,t2_us,t3_us,rtt_us,fwd_us,rev_us")?;
            Some(f)
        }
        None => None,
    };

    let mut run = Run::default();
    let started = Instant::now();
    let inflight = Arc::new(Semaphore::new(256));
    let (send_tx, mut send_rx) = mpsc::unbounded_channel::<SendResult>();

    let mut seq: u64 = 0;
    let period = Duration::from_micros(1_000_000 / cfg.rate.max(1));
    let mut send_tick = tokio::time::interval(period);
    // Skip rather than burst: if the node cannot keep up we want the shortfall
    // to show in the report, not a catch-up flood that distorts the timings.
    send_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut report_tick = tokio::time::interval(Duration::from_secs(1));
    report_tick.tick().await;

    let deadline = (cfg.duration_secs > 0)
        .then(|| tokio::time::Instant::now() + Duration::from_secs(cfg.duration_secs));

    loop {
        tokio::select! {
            _ = send_tick.tick() => {
                let s = seq;
                seq += 1;
                let rc = rc.clone();
                let target = remote.clone();
                let tx = send_tx.clone();
                let size = cfg.size;
                let Ok(permit) = inflight.clone().try_acquire_owned() else {
                    run.send_dropped += 1;
                    continue;
                };
                tokio::spawn(async move {
                    let _permit = permit;
                    // Stamped inside the task, immediately before the call, so
                    // the number on the wire is as close to the real departure
                    // as we can get it.
                    let pkt = Packet::Probe { seq: s, t0: now_us() };
                    let ok = rc.app_message(Target::RouteId(target), pkt.encode(size)).await.is_ok();
                    let _ = tx.send(ok);
                });
            }
            Some(ok) = send_rx.recv() => {
                if ok { run.sent += 1; } else { run.send_errors += 1; }
            }
            Some(Stamped { at_us: t3, update }) = updates.recv() => {
                match update {
                    VeilidUpdate::AppMessage(msg) => {
                        if let Some(Packet::Echo { seq, t0, t1, t2 }) = Packet::decode(msg.message()) {
                            if run.arrivals.record(seq) {
                                // Both subtractions stay on one clock, so the
                                // offset between the hosts cancels exactly.
                                let elapsed = t3.saturating_sub(t0);
                                let dwell = t2.saturating_sub(t1);
                                let rtt = elapsed.saturating_sub(dwell);
                                let fwd = t1 as i64 - t0 as i64;
                                let rev = t3 as i64 - t2 as i64;

                                run.rtt.push(rtt);
                                run.forward.push(fwd.max(0) as u64);
                                run.reverse.push(rev.max(0) as u64);
                                run.forward_jitter.update(fwd);
                                run.reverse_jitter.update(rev);

                                if let Some(f) = csv.as_mut() {
                                    let _ = writeln!(
                                        f, "{seq},{t0},{t1},{t2},{t3},{rtt},{fwd},{rev}"
                                    );
                                }
                            }
                        }
                    }
                    VeilidUpdate::RouteChange(change) => {
                        note_route_change(&mut run, started, &change, &my_route, Some(&remote));
                        if change.dead_remote_routes.contains(&remote) {
                            println!(
                                "\nThe remote route died at {:.1}s. There is no way to get a fresh \
                                 blob in-band, so the run ends here — that time is the measurement.",
                                started.elapsed().as_secs_f64()
                            );
                            break;
                        }
                        if change.dead_routes.contains(&my_route) {
                            // Our own return route we can replace: build a new
                            // one and re-Hello. This is exactly the recovery a
                            // real call would need.
                            println!("Return route died; rebuilding.");
                            let _ = api.release_private_route(my_route.clone());
                            match create_route(&api, &params).await {
                                Ok(RouteBlob { route_id, blob }) => {
                                    my_route = route_id;
                                    let hello = Packet::Hello { return_route: blob }.encode(0);
                                    if rc.app_message(Target::RouteId(remote.clone()), hello)
                                        .await.is_ok()
                                    {
                                        run.note_route_event(
                                            started.elapsed().as_secs_f64(),
                                            format!("return route rebuilt as {my_route}"),
                                        );
                                    }
                                }
                                Err(e) => {
                                    println!("could not rebuild the return route: {e}");
                                    break;
                                }
                            }
                        }
                    }
                    VeilidUpdate::Shutdown => break,
                    _ => {}
                }
            }
            _ = report_tick.tick() => {
                println!("{}", run.tick_line(started.elapsed().as_secs_f64()));
            }
            _ = async {
                match deadline {
                    Some(d) => tokio::time::sleep_until(d).await,
                    None => std::future::pending::<()>().await,
                }
            } => break,
            _ = done.recv() => break,
        }
    }

    // Give the last few echoes a chance to land before we call them lost.
    let drain = tokio::time::Instant::now() + Duration::from_secs(3);
    println!("\nDraining for 3s...");
    loop {
        tokio::select! {
            Some(Stamped { at_us: t3, update }) = updates.recv() => {
                if let VeilidUpdate::AppMessage(msg) = update
                    && let Some(Packet::Echo { seq, t0, t1, t2 }) = Packet::decode(msg.message())
                    && run.arrivals.record(seq)
                {
                    let rtt = t3.saturating_sub(t0).saturating_sub(t2.saturating_sub(t1));
                    run.rtt.push(rtt);
                    run.forward.push((t1 as i64 - t0 as i64).max(0) as u64);
                    run.reverse.push((t3 as i64 - t2 as i64).max(0) as u64);
                }
            }
            Some(ok) = send_rx.recv() => {
                if ok { run.sent += 1; } else { run.send_errors += 1; }
            }
            _ = tokio::time::sleep_until(drain) => break,
        }
    }

    let elapsed = started.elapsed().as_secs_f64();
    run.scheduled = (elapsed * cfg.rate as f64) as u64;

    let mut desc = params.describe();
    desc.push_str(&format!(
        "\n  rate / packet size         {}/s / {} bytes",
        cfg.rate, cfg.size
    ));
    println!("{}", run.prober_report(&desc, elapsed));

    if let Some(mut f) = csv {
        let _ = f.flush();
        if let Some(p) = &cfg.csv {
            println!("\nPer-packet samples written to {}", p.display());
        }
    }
    Ok(())
}

/// Record route churn, and remember when the first death happened — the number
/// that decides whether a call could have survived this long.
fn note_route_change(
    run: &mut Run,
    started: Instant,
    change: &VeilidRouteChange,
    mine: &RouteId,
    remote: Option<&RouteId>,
) {
    let at = started.elapsed().as_secs_f64();
    let mut relevant = false;
    if change.dead_routes.contains(mine) {
        run.note_route_event(at, format!("our route {mine} died"));
        relevant = true;
    }
    if let Some(r) = remote
        && change.dead_remote_routes.contains(r)
    {
        run.note_route_event(at, format!("remote route {r} died"));
        relevant = true;
    }
    if relevant && run.first_route_death.is_none() {
        run.first_route_death = Some(at);
    }
}

/// Clamp `--size` into what a probe needs and what Veilid will carry.
pub fn clamp_size(requested: usize) -> usize {
    requested.clamp(PROBE_LEN, MAX_APP_MESSAGE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_is_clamped_to_the_protocol() {
        assert_eq!(clamp_size(0), PROBE_LEN);
        assert_eq!(clamp_size(1_000_000), MAX_APP_MESSAGE);
        assert_eq!(clamp_size(1200), 1200);
    }
}
