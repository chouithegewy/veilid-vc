//! Moving whole files over a private route.
//!
//! The measurement side of this tool answers "how fast is the path?". This
//! answers the next question: can you actually get a file across it, and what
//! does the path cost you while you do?
//!
//! Veilid gives you two primitives and neither is a file transfer. `app_message`
//! is fire-and-forget and capped at 32,768 bytes, so a file is a pile of chunks
//! and some of them will not arrive. `app_call` is request/response, so it is
//! reliable, but it pays a full round trip per call and the whole point of a
//! private route is that the round trip is long.
//!
//! So this uses both, for what each is good at:
//!
//! 1. **Manifest** over `app_call`. The sender does not send a byte until the
//!    receiver has acknowledged the name, the length, the chunking and the
//!    CRC-32 of the file. One round trip, and afterwards both ends agree on
//!    what "complete" means.
//! 2. **Chunks** over `app_message`, paced, many in flight. This is the part
//!    that has to be fast, and it is the part that loses packets.
//! 3. **Status** over `app_call`. "What are you still missing?" The receiver
//!    answers with the indices it has not got. The sender resends exactly
//!    those and asks again.
//!
//! Step 3 repeats until the answer is "nothing", so the transfer is reliable
//! without an ack per chunk, and the size of the first answer is a direct
//! measurement of what `app_message` loss looks like at this chunk size.
//!
//! The receiver never needs a route back to the sender: an `app_call` answer
//! returns down the path the question arrived on, so the sender can always be
//! heard even though it never published a route of its own.

use crate::proto::{
    crc32, Packet, MAX_CHUNK_DATA, MAX_MISSING_LISTED, XFER_COMPLETE, XFER_IN_PROGRESS,
    XFER_UNKNOWN,
};
use crate::roles::{create_route, routing_context, try_again_loop, RouteParams, Stamped, Updates};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use veilid_core::*;

/// How many chunk sends may be in the node at once. Past this the pacing loop
/// waits, so a stalled network applies backpressure instead of growing a pile
/// of tasks.
const MAX_INFLIGHT: usize = 32;

/// How many times to go back to the rendezvous record after burning through
/// every route it named. Without a bound a permanently dead receiver spins
/// forever: the record still holds its last, now-useless list, so each refresh
/// hands back the same dead routes.
const MAX_POOL_REFRESHES: u32 = 3;

/// How long a rotated-out route is kept alive after its replacement has been
/// published. This is the "make" half of make-before-break: a sender that read
/// the record just before the rotation still holds the old route, and dropping
/// it immediately would strand exactly the sender the rotation was meant to
/// protect.
const ROTATION_GRACE: Duration = Duration::from_secs(90);

pub struct RecvConfig {
    pub out_dir: PathBuf,
    /// Refuse any transfer claiming to be larger than this. The receiver
    /// allocates the whole file up front, and anyone holding the blob can open
    /// a transfer.
    pub max_bytes: u64,
    /// Where the rendezvous record's key and owner keypair are kept, so the
    /// same DHT key survives restarts and only has to be shared once.
    pub rendezvous_file: PathBuf,
    /// How many private routes to keep published at once.
    pub pool: usize,
    /// Roughly how long any one route is allowed to live before it is replaced.
    /// Zero disables rotation, leaving the pool purely reactive.
    pub rotate_secs: u64,
}

/// Where the sender gets the receiver's route from.
pub enum RouteSource {
    /// A blob pasted on the command line. Perishable: it names one route, and
    /// when that route dies there is no way to learn its replacement.
    Blob(Vec<u8>),
    /// A DHT record the receiver rewrites on every rotation. The key is stable,
    /// so a dead route becomes a re-read rather than the end of the run.
    Rendezvous(String),
}

/// The receiver's side of the rendezvous: a DHT record whose subkey 0 always
/// holds the blob for its current route.
///
/// A route blob names exactly one route, and routes die on their own schedule.
/// Publishing the blob into a DHT record under a stable key breaks that
/// coupling: the key is what you share, and the receiver rewrites the value
/// behind it every time it rebuilds. `veilid-core` gives us the pieces
/// (`create_dht_record`, `set_dht_value`, `get_dht_value`); the rendezvous
/// itself is application-level, because Veilid has no session that outlives a
/// route.
struct Rendezvous {
    rc: RoutingContext,
    key: RecordKey,
}

impl Rendezvous {
    /// Reuse the saved record if there is one, so the key stays constant across
    /// restarts; otherwise mint a new one and remember its owner keypair.
    async fn open(
        rc: &RoutingContext,
        path: &Path,
    ) -> Result<Rendezvous, Box<dyn std::error::Error>> {
        if let Ok(txt) = std::fs::read_to_string(path) {
            let mut lines = txt.lines();
            if let (Some(k), Some(kp)) = (lines.next(), lines.next())
                && let (Ok(key), Ok(keypair)) = (RecordKey::from_str(k), KeyPair::from_str(kp))
            {
                match rc.open_dht_record(key.clone(), Some(keypair)).await {
                    Ok(_) => return Ok(Rendezvous { rc: rc.clone(), key }),
                    // Not fatal: mint a fresh record below and overwrite the
                    // saved one. The old key simply stops being answered.
                    Err(e) => eprintln!("could not reopen the saved rendezvous record: {e}"),
                }
            }
        }
        let desc = rc
            .create_dht_record(VALID_CRYPTO_KINDS[0], DHTSchema::dflt(1)?, None)
            .await?;
        let key = desc.key();
        if let Some(kp) = desc.owner_keypair() {
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            if let Err(e) = std::fs::write(path, format!("{key}\n{kp}\n")) {
                eprintln!("could not save the rendezvous key ({e}); it will change on restart");
            }
        }
        Ok(Rendezvous { rc: rc.clone(), key })
    }

    /// Publish the whole pool, most-preferred first. A sender that holds the
    /// list can fail over locally instead of coming back to the DHT.
    async fn publish(&self, blobs: &[Vec<u8>]) -> VeilidAPIResult<()> {
        let value = Packet::Routes { blobs: blobs.to_vec() }.encode(0);
        self.rc.set_dht_value(self.key.clone(), 0, value, None).await?;
        Ok(())
    }
}

/// The receiver's live routes. Every one of them reaches this node, so the pool
/// is purely about how many ways in are published at once; nothing extra has to
/// be done to serve them.
struct Entry {
    id: RouteId,
    blob: Vec<u8>,
    created: Instant,
}

struct Pool {
    routes: Vec<Entry>,
    /// Routes replaced by rotation, kept alive until their grace expires.
    retiring: Vec<(RouteId, Instant)>,
    want: usize,
}

impl Pool {
    async fn build(
        api: &VeilidAPI,
        params: &RouteParams,
        want: usize,
    ) -> Result<Pool, Box<dyn std::error::Error>> {
        let mut routes = Vec::new();
        // The first has to succeed or there is nothing to publish; the rest are
        // best-effort, because a smaller pool still works.
        let first = try_again_loop("creating route", || async { create_route(api, params).await })
            .await?;
        routes.push(Entry { id: first.route_id, blob: first.blob, created: Instant::now() });
        for _ in 1..want.max(1) {
            match create_route(api, params).await {
                Ok(RouteBlob { route_id, blob }) => {
                    routes.push(Entry { id: route_id, blob, created: Instant::now() })
                }
                Err(e) => {
                    eprintln!("could only build {} of {want} routes: {e}", routes.len());
                    break;
                }
            }
        }
        Ok(Pool { routes, retiring: Vec::new(), want: want.max(1) })
    }

    fn blobs(&self) -> Vec<Vec<u8>> {
        self.routes.iter().map(|e| e.blob.clone()).collect()
    }

    fn holds(&self, id: &RouteId) -> bool {
        self.routes.iter().any(|e| &e.id == id)
    }

    fn oldest_age(&self) -> Duration {
        self.routes.iter().map(|e| e.created.elapsed()).max().unwrap_or_default()
    }

    /// Build a replacement, then stand the oldest route down. In that order:
    /// the new route is published before the old one stops being answered, so
    /// there is never a moment with fewer live routes than advertised.
    async fn rotate(&mut self, api: &VeilidAPI, params: &RouteParams) -> Option<(RouteId, RouteId)> {
        let RouteBlob { route_id, blob } = match create_route(api, params).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("rotation could not build a replacement route: {e}");
                return None;
            }
        };
        self.routes.push(Entry { id: route_id.clone(), blob, created: Instant::now() });
        // Oldest first, so age is bounded rather than arbitrary.
        let oldest = self
            .routes
            .iter()
            .enumerate()
            .min_by_key(|(_, e)| e.created)
            .map(|(i, _)| i)?;
        let retired = self.routes.remove(oldest);
        self.retiring.push((retired.id.clone(), Instant::now()));
        Some((retired.id, route_id))
    }

    /// Release routes whose grace period has run out.
    fn reap(&mut self, api: &VeilidAPI) {
        self.retiring.retain(|(id, since)| {
            if since.elapsed() < ROTATION_GRACE {
                return true;
            }
            let _ = api.release_private_route(id.clone());
            false
        });
    }

    /// Drop the routes that just died and build replacements. Returns how many
    /// were lost, so the caller can decide whether the record needs rewriting.
    async fn replace_dead(
        &mut self,
        api: &VeilidAPI,
        params: &RouteParams,
        dead: &[RouteId],
    ) -> usize {
        let before = self.routes.len();
        self.routes.retain(|e| !dead.contains(&e.id));
        let lost = before - self.routes.len();
        self.retiring.retain(|(id, _)| !dead.contains(id));
        for id in dead {
            let _ = api.release_private_route(id.clone());
        }
        while self.routes.len() < self.want {
            match create_route(api, params).await {
                Ok(RouteBlob { route_id, blob }) => {
                    self.routes.push(Entry { id: route_id, blob, created: Instant::now() })
                }
                Err(e) => {
                    eprintln!("could not top the route pool back up: {e}");
                    break;
                }
            }
        }
        lost
    }
}

pub struct SendConfig {
    pub files: Vec<PathBuf>,
    pub chunk: usize,
    pub rate: u64,
    pub settle_ms: u64,
    pub max_rounds: u32,
}

// ---------------------------------------------------------------------------
// receiver
// ---------------------------------------------------------------------------

/// A transfer in progress: the file being assembled and what is still missing.
struct Incoming {
    name: String,
    total_len: u64,
    chunk_size: u32,
    chunk_count: u32,
    crc: u32,
    data: Vec<u8>,
    have: Vec<bool>,
    have_count: u32,
    started: Instant,
    duplicates: u32,
    /// How many chunks were still missing the first time the sender asked.
    /// That is what the first pass actually lost, before any repair.
    first_pass_missing: Option<u32>,
    repair_rounds: u32,
}

impl Incoming {
    fn new(name: String, total_len: u64, chunk_size: u32, chunk_count: u32, crc: u32) -> Self {
        Self {
            name,
            total_len,
            chunk_size,
            chunk_count,
            crc,
            data: vec![0u8; total_len as usize],
            have: vec![false; chunk_count as usize],
            have_count: 0,
            started: Instant::now(),
            duplicates: 0,
            first_pass_missing: None,
            repair_rounds: 0,
        }
    }

    fn complete(&self) -> bool {
        self.have_count == self.chunk_count
    }

    /// Where chunk `index` belongs, or None if it is not a chunk of this file.
    fn span(&self, index: u32) -> Option<(usize, usize)> {
        if index >= self.chunk_count {
            return None;
        }
        let start = index as u64 * self.chunk_size as u64;
        let end = (start + self.chunk_size as u64).min(self.total_len);
        Some((start as usize, end as usize))
    }

    /// Returns true if this chunk was new. A chunk whose length disagrees with
    /// the manifest is dropped: the sender promised a shape and this is not it.
    fn store(&mut self, index: u32, data: &[u8]) -> bool {
        let Some((start, end)) = self.span(index) else {
            return false;
        };
        if data.len() != end - start {
            return false;
        }
        if self.have[index as usize] {
            self.duplicates += 1;
            return false;
        }
        self.data[start..end].copy_from_slice(data);
        self.have[index as usize] = true;
        self.have_count += 1;
        true
    }

    /// The chunks still wanted: the honest total, and as many indices as one
    /// answer can name.
    fn missing(&self) -> (u32, Vec<u32>) {
        let total = self.chunk_count - self.have_count;
        let listed = self
            .have
            .iter()
            .enumerate()
            .filter(|(_, got)| !**got)
            .map(|(i, _)| i as u32)
            .take(MAX_MISSING_LISTED)
            .collect();
        (total, listed)
    }
}

pub async fn recv(
    api: VeilidAPI,
    mut updates: Updates,
    params: RouteParams,
    cfg: RecvConfig,
    mut done: mpsc::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(&cfg.out_dir)
        .map_err(|e| format!("{}: {e}", cfg.out_dir.display()))?;

    let rc = routing_context(&api, &params)?;

    let mut pool = Pool::build(&api, &params, cfg.pool).await?;

    // Publish the pool under a stable DHT key so a dead route is recoverable.
    // Failing to do so is not fatal -- the printed blob still works for a
    // single route's lifetime -- so the run continues without it.
    let rendezvous = match Rendezvous::open(&rc, &cfg.rendezvous_file).await {
        Ok(r) => match r.publish(&pool.blobs()).await {
            Ok(()) => Some(r),
            Err(e) => {
                eprintln!("could not publish the routes to the rendezvous record: {e}");
                None
            }
        },
        Err(e) => {
            eprintln!("no rendezvous record ({e}); routes will not be recoverable");
            None
        }
    };

    let blob = pool.blobs().into_iter().next().unwrap_or_default();
    println!("Routes ready ({}):", pool.routes.len());
    for e in &pool.routes {
        println!("  {}", e.id);
    }
    println!();
    println!("Writing arrivals to {}\n", cfg.out_dir.display());
    match &rendezvous {
        Some(r) => {
            println!("Send files to it with:\n");
            println!("  veilid-vc send --rendezvous {} <file>...\n", r.key);
            println!("That key is stable: it survives route death and restarts, so it only");
            println!("has to be shared once. The one-shot equivalent, good until this route");
            println!("dies, is:\n");
        }
        None => println!("Send files to it with:\n"),
    }
    println!(
        "  veilid-vc send --connect {} <file>...\n",
        data_encoding::BASE64.encode(&blob)
    );
    println!("Waiting. Ctrl-C to stop.\n");

    let mut transfers: HashMap<u64, Incoming> = HashMap::new();
    // Transfer ids that are already written out. A `complete` answer can be
    // lost like anything else, and the sender will ask again; without this the
    // second question answers "never heard of it" and it sends the whole file
    // a second time.
    let mut completed: HashSet<u64> = HashSet::new();
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    tick.tick().await;

    // One route is replaced every rotate_secs/pool, so each lives about
    // rotate_secs and the replacements stagger themselves out over time
    // instead of the whole pool ageing out together.
    let rotate_every = if cfg.rotate_secs == 0 {
        None
    } else {
        Some(Duration::from_secs((cfg.rotate_secs / cfg.pool.max(1) as u64).max(30)))
    };
    let mut rotate_tick = tokio::time::interval(
        rotate_every.unwrap_or(Duration::from_secs(3600)),
    );
    rotate_tick.tick().await;
    if let Some(d) = rotate_every {
        println!(
            "Rotating one route every {}s, so none lives much past {}s.\n",
            d.as_secs(),
            cfg.rotate_secs
        );
    }

    loop {
        tokio::select! {
            Some(Stamped { update, .. }) = updates.recv() => {
                match update {
                    // Control traffic. The answer goes back down the route the
                    // question came in on, so no return route is needed.
                    VeilidUpdate::AppCall(call) => {
                        let Some(reply) = answer(&mut transfers, &completed, &cfg, call.message())
                        else {
                            // Not ours. Leaving it unanswered lets the caller's
                            // own timeout deal with it.
                            continue;
                        };
                        if let Err(e) = api.app_call_reply(call.id(), reply).await {
                            eprintln!("could not answer a call: {e}");
                        }
                        // Answering a status query is the moment a transfer can
                        // turn out to be finished, because the last chunk may
                        // have landed while we were idle.
                        finish_completed(&cfg, &mut transfers, &mut completed);
                    }
                    VeilidUpdate::AppMessage(msg) => {
                        if let Some(Packet::Chunk { xfer, index, data }) =
                            Packet::decode(msg.message())
                            && let Some(inc) = transfers.get_mut(&xfer)
                        {
                            inc.store(index, &data);
                        }
                    }
                    VeilidUpdate::RouteChange(change) => {
                        let dead: Vec<RouteId> = change
                            .dead_routes
                            .iter()
                            .filter(|id| pool.holds(id))
                            .cloned()
                            .collect();
                        if dead.is_empty() {
                            continue;
                        }
                        // Survivable while any route in the pool is still up:
                        // senders holding the published list fail over to one of
                        // the others with no round trip at all. Transfers in
                        // flight are keyed by transfer id, not by route, so they
                        // carry on rather than restart.
                        let lost = pool.replace_dead(&api, &params, &dead).await;
                        println!("\n{lost} route(s) died; pool is now {}.", pool.routes.len());
                        if pool.routes.is_empty() {
                            println!("No routes left and none could be rebuilt. Restarting is the \
                                      only way forward.");
                            break;
                        }
                        let Some(r) = &rendezvous else {
                            println!("No rendezvous record, so the blob already handed out is \
                                      stale. Restart to publish a fresh one.");
                            break;
                        };
                        match r.publish(&pool.blobs()).await {
                            Ok(()) => println!("Republished {} route(s).", pool.routes.len()),
                            Err(e) => println!("could not republish the pool: {e}"),
                        }
                    }
                    VeilidUpdate::Shutdown => break,
                    _ => {}
                }
            }
            _ = rotate_tick.tick(), if rotate_every.is_some() => {
                pool.reap(&api);
                if let Some((retired, fresh)) = pool.rotate(&api, &params).await {
                    match &rendezvous {
                        Some(r) => match r.publish(&pool.blobs()).await {
                            Ok(()) => println!(
                                "Rotated {retired} out for {fresh} (oldest route now {}s).",
                                pool.oldest_age().as_secs()
                            ),
                            Err(e) => println!("rotated but could not republish: {e}"),
                        },
                        // Without a record nobody can learn the new route, so
                        // rotating would only throw away the one they have.
                        None => println!("no rendezvous record; skipping rotation"),
                    }
                }
            }
            _ = tick.tick() => {
                for inc in transfers.values() {
                    println!(
                        "  {} — {}/{} chunks ({:.0}%)",
                        inc.name,
                        inc.have_count,
                        inc.chunk_count,
                        inc.have_count as f64 * 100.0 / inc.chunk_count.max(1) as f64,
                    );
                }
            }
            _ = done.recv() => break,
        }
    }

    if !transfers.is_empty() {
        println!("\n{} transfer(s) unfinished:", transfers.len());
        for inc in transfers.values() {
            println!("  {} — {}/{} chunks", inc.name, inc.have_count, inc.chunk_count);
        }
    }
    Ok(())
}

/// Build the answer to one `app_call`, or None if it was not for us.
fn answer(
    transfers: &mut HashMap<u64, Incoming>,
    completed: &HashSet<u64>,
    cfg: &RecvConfig,
    message: &[u8],
) -> Option<Vec<u8>> {
    match Packet::decode(message)? {
        Packet::Manifest { xfer, total_len, chunk_size, chunk_count, crc32: crc, name } => {
            let refuse = |why: &str| {
                Some(Packet::ManifestOk { xfer, accepted: false, message: why.into() }.encode(0))
            };
            if completed.contains(&xfer) {
                // Already written. Accepting costs the sender one wasted pass,
                // which the next status query cuts short; refusing would look
                // like a failure for a file that arrived intact.
                return Some(Packet::ManifestOk { xfer, accepted: true, message: String::new() }.encode(0));
            }
            if let Some(existing) = transfers.get(&xfer) {
                // A retried manifest, because our first answer did not make it
                // back. Keep the progress we already have.
                println!("Resuming {} ({}/{} chunks)", existing.name, existing.have_count, existing.chunk_count);
                return Some(Packet::ManifestOk { xfer, accepted: true, message: String::new() }.encode(0));
            }
            if total_len > cfg.max_bytes {
                println!("Refused {name}: {total_len} bytes is over the --max-bytes limit");
                return refuse("over the receiver's --max-bytes limit");
            }
            if chunk_size == 0 || chunk_size as usize > MAX_CHUNK_DATA {
                return refuse("chunk size will not fit an app_message");
            }
            // The three numbers have to agree, or `span` and the manifest are
            // describing different files.
            if total_len.div_ceil(chunk_size as u64) != chunk_count as u64 {
                return refuse("chunk count does not match the length and chunk size");
            }
            println!(
                "Incoming: {name} — {total_len} bytes, {chunk_count} chunk(s) of {chunk_size}, crc32 {crc:08x}"
            );
            transfers.insert(xfer, Incoming::new(name, total_len, chunk_size, chunk_count, crc));
            Some(Packet::ManifestOk { xfer, accepted: true, message: String::new() }.encode(0))
        }
        Packet::Status { xfer } => {
            let Some(inc) = transfers.get_mut(&xfer) else {
                let state = if completed.contains(&xfer) { XFER_COMPLETE } else { XFER_UNKNOWN };
                return Some(
                    Packet::StatusReply { xfer, state, missing_total: 0, missing: Vec::new() }
                        .encode(0),
                );
            };
            let (missing_total, missing) = inc.missing();
            if inc.first_pass_missing.is_none() {
                inc.first_pass_missing = Some(missing_total);
            } else {
                inc.repair_rounds += 1;
            }
            let state = if inc.complete() { XFER_COMPLETE } else { XFER_IN_PROGRESS };
            Some(Packet::StatusReply { xfer, state, missing_total, missing }.encode(0))
        }
        _ => None,
    }
}

/// Write out and report every transfer that has all of its chunks.
fn finish_completed(
    cfg: &RecvConfig,
    transfers: &mut HashMap<u64, Incoming>,
    completed: &mut HashSet<u64>,
) {
    let done: Vec<u64> = transfers
        .iter()
        .filter(|(_, inc)| inc.complete())
        .map(|(id, _)| *id)
        .collect();
    for id in done {
        if let Some(inc) = transfers.remove(&id) {
            write_out(cfg, inc);
            completed.insert(id);
        }
    }
}

fn write_out(cfg: &RecvConfig, inc: Incoming) {
    let elapsed = inc.started.elapsed().as_secs_f64().max(0.001);
    let got = crc32(&inc.data);
    let path = unique_path(&cfg.out_dir, &safe_name(&inc.name));

    println!("\n-- received {} --", inc.name);
    match std::fs::write(&path, &inc.data) {
        Ok(()) => println!("  written to             {}", path.display()),
        Err(e) => println!("  COULD NOT WRITE        {}: {e}", path.display()),
    }
    println!("  bytes                  {}", inc.total_len);
    println!(
        "  elapsed                {elapsed:.1}s  ({:.1} KiB/s)",
        inc.total_len as f64 / elapsed / 1024.0
    );
    println!("  chunks                 {} of {}", inc.chunk_count, inc.chunk_size);
    if let Some(missed) = inc.first_pass_missing {
        println!(
            "  lost on the first pass {missed}  ({:.1}% of chunks)",
            missed as f64 * 100.0 / inc.chunk_count.max(1) as f64
        );
    }
    println!("  repair rounds          {}", inc.repair_rounds);
    println!("  duplicate chunks       {}", inc.duplicates);
    if got == inc.crc {
        println!("  crc32                  {got:08x}  matches the sender");
    } else {
        println!("  crc32                  {got:08x}  DOES NOT MATCH sender's {:08x}", inc.crc);
    }
    println!();
}

/// Reduce a name from the wire to something that can only land inside the
/// output directory. The sender is whoever holds the blob, so this treats the
/// name as hostile: directory components are dropped, and so is anything that
/// could make the result start with a dot.
fn safe_name(raw: &str) -> String {
    let base = raw.rsplit(['/', '\\']).next().unwrap_or("");
    let cleaned: String = base
        .chars()
        .filter(|c| !c.is_control() && *c != '\u{7f}')
        .collect();
    let cleaned = cleaned.trim().trim_start_matches('.').trim();
    if cleaned.is_empty() {
        "transfer.bin".to_string()
    } else {
        cleaned.to_string()
    }
}

/// Never overwrite. A second `cat.png` lands as `cat-1.png`.
fn unique_path(dir: &Path, name: &str) -> PathBuf {
    let candidate = dir.join(name);
    if !candidate.exists() {
        return candidate;
    }
    let (stem, ext) = match name.rsplit_once('.') {
        Some((s, e)) if !s.is_empty() => (s.to_string(), format!(".{e}")),
        _ => (name.to_string(), String::new()),
    };
    for n in 1..10_000 {
        let candidate = dir.join(format!("{stem}-{n}{ext}"));
        if !candidate.exists() {
            return candidate;
        }
    }
    dir.join(format!("{stem}-{}{ext}", crate::proto::now_us()))
}

// ---------------------------------------------------------------------------
// sender
// ---------------------------------------------------------------------------

pub async fn send(
    api: VeilidAPI,
    mut updates: Updates,
    params: RouteParams,
    cfg: SendConfig,
    source: RouteSource,
    mut done: mpsc::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let rc = routing_context(&api, &params)?;

    // With a rendezvous key the record stays open for the whole run, so a
    // later re-read costs one DHT fetch rather than a fresh open.
    let record = match &source {
        RouteSource::Blob(_) => None,
        RouteSource::Rendezvous(k) => {
            let key = RecordKey::from_str(k)
                .map_err(|e| format!("that does not look like a rendezvous key: {e}"))?;
            let _ = try_again_loop("opening rendezvous record", || async {
                rc.open_dht_record(key.clone(), None).await
            })
            .await?;
            println!("Rendezvous: {key}");
            Some(key)
        }
    };

    let mut routes = import_routes(&api, &rc, &source, record.as_ref()).await?;
    let mut current = 0usize;
    println!("Remote route(s): {}", routes.len());
    for (i, r) in routes.iter().enumerate() {
        println!("  {}{r}", if i == 0 { "* " } else { "  " });
    }
    println!("{}\n", params.describe());

    for path in &cfg.files {
        // The transfer id is fixed per file rather than per attempt, so if the
        // route dies and we come back on a new one the receiver recognises the
        // transfer and we resume from the chunks it is missing.
        let name = file_name_of(path);
        let xfer = xfer_id(&name);
        let mut refreshes = 0u32;
        let mut stats = FileStats::default();
        let started = Instant::now();
        loop {
            let remote = routes[current].clone();
            let outcome = tokio::select! {
                r = send_one(&rc, &remote, path, &cfg, xfer, &mut stats) => match r {
                    Ok(()) => Outcome::Done,
                    Err(SendError::RouteUnusable(why)) => {
                        println!("  {why}");
                        Outcome::RemoteRouteDied
                    }
                    Err(SendError::Fatal(e)) => return Err(e),
                },
                () = watch_remote(&mut updates, &remote) => Outcome::RemoteRouteDied,
                _ = done.recv() => {
                    println!("\nInterrupted.");
                    return Ok(());
                }
            };
            match outcome {
                Outcome::Done => {
                    report_file(path, &stats, started);
                    break;
                }
                Outcome::RemoteRouteDied => {
                    // A spare from the published pool costs nothing to try: we
                    // already hold it, so this is a local retry with no round
                    // trip and no dependency on the receiver being reachable.
                    current += 1;
                    if current < routes.len() {
                        println!(
                            "  route died; failing over to spare {}/{}: {}",
                            current + 1,
                            routes.len(),
                            routes[current]
                        );
                        continue;
                    }
                    // The pool is exhausted. Now it is worth a DHT round trip.
                    if record.is_none() {
                        return Err("the remote route died mid-transfer and there is no rendezvous \
                                    record, so there is no way to learn its replacement. Restart \
                                    the receiver for a fresh blob, or use --rendezvous."
                            .into());
                    }
                    refreshes += 1;
                    if refreshes > MAX_POOL_REFRESHES {
                        return Err(format!(
                            "{name}: every route in the rendezvous record has failed \
                             {MAX_POOL_REFRESHES} refreshes running. The receiver is most likely \
                             gone -- it would have republished by now if it were alive."
                        )
                        .into());
                    }
                    println!(
                        "  every published route is dead; re-reading the rendezvous record \
                         ({refreshes}/{MAX_POOL_REFRESHES})"
                    );
                    routes = import_routes(&api, &rc, &source, record.as_ref()).await?;
                    current = 0;
                    println!("  resuming on {}", routes[current]);
                }
            }
        }
    }
    Ok(())
}

enum Outcome {
    Done,
    RemoteRouteDied,
}

/// Why a transfer attempt stopped.
///
/// The distinction is what makes a pool useful. Veilid only reports a route as
/// dead when it decides to; long before that, control calls on it start failing
/// with `could not get remote private route`. Treating that as fatal throws
/// away perfectly good spares, so it is reported separately and retried on the
/// next route.
/// Per-file totals, accumulated across however many attempts and routes it
/// took. Keeping these in the caller is the point: a retry after a failover is
/// still the same file, and timing it from the retry's start reports a
/// throughput that never happened.
#[derive(Default)]
struct FileStats {
    chunk_sends: u64,
    send_errors: u64,
    repairs: u32,
    first_pass_missing: Option<u32>,
    attempts: u32,
    total_chunks: u32,
    bytes: u64,
}

enum SendError {
    RouteUnusable(String),
    Fatal(Box<dyn std::error::Error>),
}

/// Resolve the receiver's current route: straight from the pasted blob, or from
/// the rendezvous record.
///
/// The DHT read always forces a refresh. A cached value is by definition the
/// route we were told about last time, and the only reason to consult the
/// record at all is that it may since have changed -- reading the cache would
/// hand back exactly the stale blob we are trying to get away from.
async fn import_routes(
    api: &VeilidAPI,
    rc: &RoutingContext,
    source: &RouteSource,
    record: Option<&RecordKey>,
) -> Result<Vec<RouteId>, Box<dyn std::error::Error>> {
    let blobs = match (source, record) {
        (RouteSource::Blob(b), _) => vec![b.clone()],
        (RouteSource::Rendezvous(_), Some(key)) => {
            let value = try_again_loop("reading rendezvous record", || async {
                rc.get_dht_value(key.clone(), 0, true).await
            })
            .await?
            .ok_or("the rendezvous record exists but has no route published in it yet")?;
            let raw = value.data();
            match Packet::decode(raw) {
                Some(Packet::Routes { blobs }) => blobs,
                // A receiver before the pool existed published a bare blob.
                // It carries no magic, so it cannot be confused for a list.
                _ => vec![raw.to_vec()],
            }
        }
        (RouteSource::Rendezvous(_), None) => return Err("rendezvous record was never opened".into()),
    };

    let mut routes = Vec::new();
    let mut last_err = None;
    for blob in &blobs {
        match api.import_remote_private_route(blob.clone()) {
            Ok(id) => routes.push(id),
            Err(e) => last_err = Some(e),
        }
    }
    if routes.is_empty() {
        // Nothing imported cleanly. Retry the first one patiently, since the
        // usual cause is the node not being ready rather than a bad blob.
        let first = blobs.into_iter().next().ok_or_else(|| {
            format!("no routes published{}",
                    last_err.map(|e| format!(" and none importable: {e}")).unwrap_or_default())
        })?;
        routes.push(
            try_again_loop("importing remote route", || async {
                api.import_remote_private_route(first.clone())
            })
            .await?,
        );
    }
    Ok(routes)
}

fn file_name_of(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "transfer.bin".to_string())
}

/// Returns when the route we are sending down is gone, or the node is stopping.
async fn watch_remote(updates: &mut Updates, remote: &RouteId) {
    while let Some(Stamped { update, .. }) = updates.recv().await {
        match update {
            VeilidUpdate::RouteChange(change) if change.dead_remote_routes.contains(remote) => {
                return
            }
            VeilidUpdate::Shutdown => return,
            _ => {}
        }
    }
}

async fn send_one(
    rc: &RoutingContext,
    remote: &RouteId,
    path: &Path,
    cfg: &SendConfig,
    xfer: u64,
    stats: &mut FileStats,
) -> Result<(), SendError> {
    let bytes = std::fs::read(path)
        .map_err(|e| SendError::Fatal(format!("{}: {e}", path.display()).into()))?;
    let name = file_name_of(path);

    let chunk_size = cfg.chunk.clamp(1, MAX_CHUNK_DATA);
    let chunk_count = bytes.len().div_ceil(chunk_size);
    if chunk_count > u32::MAX as usize {
        return Err(SendError::Fatal(
            format!("{name}: too many chunks; use a larger --chunk").into(),
        ));
    }
    let crc = crc32(&bytes);

    println!(
        "\n{name}: {} bytes, {chunk_count} chunk(s) of {chunk_size}, crc32 {crc:08x}",
        bytes.len()
    );

    let manifest = Packet::Manifest {
        xfer,
        total_len: bytes.len() as u64,
        chunk_size: chunk_size as u32,
        chunk_count: chunk_count as u32,
        crc32: crc,
        name: name.clone(),
    }
    .encode(0);

    open_transfer(rc, remote, &manifest, &name).await?;

    stats.attempts += 1;
    let mut round = 0u32;

    // Ask before sending, every time round. On a fresh transfer the answer is
    // "all of them" and this costs one round trip; on a resumed one -- after a
    // failover, or a second run of the same file -- it is what makes the resume
    // real. Assuming a full send and letting the receiver discard duplicates
    // gets the right file, but re-ships every byte already delivered.
    loop {
        let reply = call(rc, remote, Packet::Status { xfer }.encode(0), "status")
            .await
            .map_err(|e| SendError::RouteUnusable(format!("status calls are failing ({e})")))?;
        let Some(Packet::StatusReply { state, missing_total, missing, .. }) = Packet::decode(&reply)
        else {
            return Err(SendError::Fatal(
                format!("{name}: the receiver answered a status query with something else").into(),
            ));
        };
        // Round 0's answer is the starting position, not a loss measurement.
        // The first pass's loss is what is still missing after it.
        if round == 1 && stats.first_pass_missing.is_none() {
            stats.first_pass_missing = Some(missing_total);
        }

        match state {
            XFER_COMPLETE => break,
            XFER_UNKNOWN => {
                // The receiver restarted, or never got the manifest at all.
                println!("  the receiver does not know this transfer; opening it again");
                open_transfer(rc, remote, &manifest, &name).await?;
                round += 1;
                if round > cfg.max_rounds {
                    return Err(SendError::Fatal(
                        format!("{name}: the receiver keeps forgetting this transfer").into(),
                    ));
                }
                continue;
            }
            _ => {}
        }

        if missing.is_empty() {
            return Err(SendError::Fatal(
                format!("{name}: the receiver is {missing_total} chunk(s) short but named none of them")
                    .into(),
            ));
        }

        if round == 0 {
            let already = chunk_count as u32 - missing_total;
            if already > 0 {
                println!("  resuming: {already} of {chunk_count} chunk(s) already there");
            }
            println!("  sending {} chunk(s) at {}/s", missing.len(), cfg.rate);
        } else {
            println!("  repair pass {round}: resending {} chunk(s)", missing.len());
        }
        let (ok, failed) = blast(rc, remote, xfer, &bytes, chunk_size, &missing, cfg.rate).await;
        stats.chunk_sends += ok + failed;
        stats.send_errors += failed;
        if round > 0 {
            stats.repairs += 1;
        }

        // Let whatever is still in flight land before calling it missing.
        tokio::time::sleep(Duration::from_millis(cfg.settle_ms)).await;

        round += 1;
        if round > cfg.max_rounds {
            return Err(SendError::Fatal(
                format!(
                    "{name}: still {missing_total} chunk(s) short after {} repair pass(es); \
                     try a lower --rate or a smaller --chunk",
                    cfg.max_rounds
                )
                .into(),
            ));
        }
    }

    stats.total_chunks = chunk_count as u32;
    stats.bytes = bytes.len() as u64;
    Ok(())
}

/// The per-file summary, printed once the file is actually complete -- timed
/// from when the file was first attempted, not from the attempt that happened
/// to finish it.
fn report_file(path: &Path, stats: &FileStats, started: Instant) {
    let elapsed = started.elapsed().as_secs_f64().max(0.001);
    let lost = stats.first_pass_missing.unwrap_or(0);
    println!(
        "  delivered in {elapsed:.1}s  ({:.1} KiB/s)",
        stats.bytes as f64 / elapsed / 1024.0
    );
    println!(
        "  {} chunk send(s) for {} chunk(s); first pass lost {lost} ({:.1}%), {} repair pass(es)",
        stats.chunk_sends,
        stats.total_chunks,
        lost as f64 * 100.0 / stats.total_chunks.max(1) as f64,
        stats.repairs,
    );
    if stats.attempts > 1 {
        println!(
            "  took {} attempts across {} route(s)",
            stats.attempts, stats.attempts
        );
    }
    if stats.send_errors > 0 {
        println!("  {} chunk(s) never left the node", stats.send_errors);
    }
    let _ = path;
}

/// Get the receiver to agree to a transfer before sending any of it.
async fn open_transfer(
    rc: &RoutingContext,
    remote: &RouteId,
    manifest: &[u8],
    name: &str,
) -> Result<(), SendError> {
    let reply = call(rc, remote, manifest.to_vec(), "manifest")
        .await
        .map_err(|e| SendError::RouteUnusable(format!("manifest calls are failing ({e})")))?;
    match Packet::decode(&reply) {
        Some(Packet::ManifestOk { accepted: true, .. }) => Ok(()),
        // A refusal is the receiver's considered answer, not a broken path:
        // another route would be refused in exactly the same way.
        Some(Packet::ManifestOk { message, .. }) => Err(SendError::Fatal(
            format!("{name}: the receiver refused it — {message}").into(),
        )),
        _ => Err(SendError::Fatal(
            format!("{name}: the receiver answered the manifest with something else").into(),
        )),
    }
}

/// One `app_call`, retried past the failures that are worth retrying.
///
/// Two kinds of "not yet" are worth telling apart. `TryAgain` is the node
/// still finding its feet — a freshly started sender has not established its
/// network class yet and cannot allocate a route at all, which is normal for
/// the first several seconds and not a failure. That gets waited out. A
/// timeout or a dropped route is a real miss, and gets a bounded number of
/// attempts with a growing pause.
async fn call(
    rc: &RoutingContext,
    remote: &RouteId,
    message: Vec<u8>,
    what: &str,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    const ATTEMPTS: u32 = 5;
    let mut last = String::new();
    for attempt in 1..=ATTEMPTS {
        let sent = try_again_loop(what, || async {
            rc.app_call(Target::RouteId(remote.clone()), message.clone()).await
        })
        .await;
        match sent {
            Ok(reply) => return Ok(reply),
            Err(e) => {
                last = e.to_string();
                eprintln!("  {what} attempt {attempt}/{ATTEMPTS} failed: {e}");
                tokio::time::sleep(Duration::from_millis(500 * attempt as u64)).await;
            }
        }
    }
    Err(format!("{what} failed after {ATTEMPTS} attempts: {last}").into())
}

/// Push the named chunks at the receiver, paced, several in flight. Returns
/// (accepted by the node, refused by the node).
async fn blast(
    rc: &RoutingContext,
    remote: &RouteId,
    xfer: u64,
    bytes: &[u8],
    chunk_size: usize,
    indices: &[u32],
    rate: u64,
) -> (u64, u64) {
    let mut ticker = tokio::time::interval(Duration::from_micros(1_000_000 / rate.max(1)));
    // Waiting for in-flight room must not be repaid as a burst at full speed;
    // --rate is a spacing, not an average to catch up to.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut set: JoinSet<bool> = JoinSet::new();
    let (mut ok, mut failed) = (0u64, 0u64);
    let mut tally = |sent: bool| {
        if sent {
            ok += 1;
        } else {
            failed += 1;
        }
    };

    for &i in indices {
        ticker.tick().await;
        while set.len() >= MAX_INFLIGHT {
            match set.join_next().await {
                Some(Ok(sent)) => tally(sent),
                Some(Err(_)) => tally(false),
                None => break,
            }
        }
        let start = i as usize * chunk_size;
        let end = (start + chunk_size).min(bytes.len());
        let payload = Packet::Chunk { xfer, index: i, data: bytes[start..end].to_vec() }.encode(0);
        let rc = rc.clone();
        let target = remote.clone();
        set.spawn(async move {
            rc.app_message(Target::RouteId(target), payload).await.is_ok()
        });
    }
    while let Some(r) = set.join_next().await {
        tally(matches!(r, Ok(true)));
    }
    (ok, failed)
}

/// A per-transfer id, so two runs against the same receiver never collide and
/// a restarted sender does not resume into a stale half-file.
fn xfer_id(name: &str) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for b in name.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h ^ crate::proto::now_us().rotate_left(17)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn incoming(total_len: u64, chunk_size: u32) -> Incoming {
        let count = total_len.div_ceil(chunk_size as u64) as u32;
        Incoming::new("x.bin".into(), total_len, chunk_size, count, 0)
    }

    #[test]
    fn a_file_reassembles_out_of_order() {
        let mut inc = incoming(2500, 1000);
        assert_eq!(inc.chunk_count, 3);
        assert!(inc.store(2, &[3u8; 500])); // the short last chunk
        assert!(inc.store(0, &[1u8; 1000]));
        assert!(!inc.complete());
        assert!(inc.store(1, &[2u8; 1000]));
        assert!(inc.complete());
        assert_eq!(inc.data[0], 1);
        assert_eq!(inc.data[1000], 2);
        assert_eq!(inc.data[2000], 3);
    }

    #[test]
    fn a_chunk_of_the_wrong_length_is_dropped() {
        let mut inc = incoming(2500, 1000);
        // The last chunk is 500 bytes; a full-size one there would run past
        // the end of the file.
        assert!(!inc.store(2, &[9u8; 1000]));
        assert!(!inc.store(0, &[9u8; 999]));
        assert_eq!(inc.have_count, 0);
    }

    #[test]
    fn a_chunk_past_the_end_is_dropped() {
        let mut inc = incoming(2500, 1000);
        assert!(!inc.store(3, &[9u8; 1000]));
        assert!(!inc.store(u32::MAX, &[9u8; 1000]));
        assert_eq!(inc.have_count, 0);
    }

    #[test]
    fn duplicates_are_counted_not_stored_twice() {
        let mut inc = incoming(2000, 1000);
        assert!(inc.store(0, &[1u8; 1000]));
        assert!(!inc.store(0, &[2u8; 1000]));
        assert_eq!(inc.duplicates, 1);
        assert_eq!(inc.have_count, 1);
        assert_eq!(inc.data[0], 1, "the duplicate must not overwrite what arrived first");
    }

    #[test]
    fn missing_names_the_gaps() {
        let mut inc = incoming(5000, 1000);
        inc.store(0, &[0u8; 1000]);
        inc.store(2, &[0u8; 1000]);
        inc.store(4, &[0u8; 1000]);
        assert_eq!(inc.missing(), (2, vec![1, 3]));
    }

    #[test]
    fn missing_lists_no_more_than_one_answer_holds() {
        let count = MAX_MISSING_LISTED as u64 + 100;
        let inc = incoming(count, 1);
        let (total, listed) = inc.missing();
        assert_eq!(total as u64, count);
        assert_eq!(listed.len(), MAX_MISSING_LISTED);
    }

    #[test]
    fn an_empty_file_is_complete_on_arrival() {
        let inc = Incoming::new("empty".into(), 0, 1000, 0, crc32(b""));
        assert!(inc.complete());
        assert_eq!(inc.missing(), (0, vec![]));
    }

    #[test]
    fn a_hostile_name_cannot_escape_the_output_directory() {
        assert_eq!(safe_name("../../etc/passwd"), "passwd");
        assert_eq!(safe_name("/etc/shadow"), "shadow");
        assert_eq!(safe_name(r"..\..\windows\system32\cmd.exe"), "cmd.exe");
        assert_eq!(safe_name(".."), "transfer.bin");
        assert_eq!(safe_name(""), "transfer.bin");
        assert_eq!(safe_name("   "), "transfer.bin");
        assert_eq!(safe_name(".bashrc"), "bashrc");
        assert_eq!(safe_name("ok\u{0}nul.png"), "oknul.png");
        assert_eq!(safe_name("cat.png"), "cat.png");
    }

    #[test]
    fn the_manifest_and_the_chunking_have_to_agree() {
        let mut transfers = HashMap::new();
        let cfg = RecvConfig { out_dir: ".".into(), max_bytes: 1024, rendezvous_file: ".".into(), pool: 1, rotate_secs: 0 };
        let ask = |total_len: u64, chunk_size: u32, chunk_count: u32| {
            Packet::Manifest {
                xfer: 1,
                total_len,
                chunk_size,
                chunk_count,
                crc32: 0,
                name: "x".into(),
            }
            .encode(0)
        };
        let accepted = |msg: &[u8], transfers: &mut HashMap<u64, Incoming>| {
            match Packet::decode(&answer(transfers, &HashSet::new(), &cfg, msg).unwrap()) {
                Some(Packet::ManifestOk { accepted, .. }) => accepted,
                other => panic!("answered with {other:?}"),
            }
        };

        // 100 bytes in 10-byte chunks is 10 chunks, not 9.
        assert!(!accepted(&ask(100, 10, 9), &mut transfers));
        // Over --max-bytes.
        assert!(!accepted(&ask(2048, 10, 205), &mut transfers));
        // A chunk that could never fit one app_message.
        assert!(!accepted(&ask(100, u32::MAX, 1), &mut transfers));
        assert!(transfers.is_empty());
        // And the one that adds up.
        assert!(accepted(&ask(100, 10, 10), &mut transfers));
        assert_eq!(transfers.len(), 1);
    }

    #[test]
    fn an_unknown_transfer_says_so_rather_than_lying_about_progress() {
        let mut transfers = HashMap::new();
        let cfg = RecvConfig { out_dir: ".".into(), max_bytes: 1024, rendezvous_file: ".".into(), pool: 1, rotate_secs: 0 };
        let reply = answer(&mut transfers, &HashSet::new(), &cfg, &Packet::Status { xfer: 7 }.encode(0)).unwrap();
        match Packet::decode(&reply) {
            Some(Packet::StatusReply { state, .. }) => assert_eq!(state, XFER_UNKNOWN),
            other => panic!("answered with {other:?}"),
        }
    }

    #[test]
    fn a_retried_manifest_keeps_the_progress_already_made() {
        let mut transfers = HashMap::new();
        let cfg = RecvConfig { out_dir: ".".into(), max_bytes: 1024, rendezvous_file: ".".into(), pool: 1, rotate_secs: 0 };
        let manifest =
            Packet::Manifest { xfer: 1, total_len: 100, chunk_size: 10, chunk_count: 10, crc32: 0, name: "x".into() }
                .encode(0);
        answer(&mut transfers, &HashSet::new(), &cfg, &manifest).unwrap();
        transfers.get_mut(&1).unwrap().store(0, &[1u8; 10]);
        answer(&mut transfers, &HashSet::new(), &cfg, &manifest).unwrap();
        assert_eq!(transfers.get(&1).unwrap().have_count, 1);
    }

    #[test]
    fn status_records_the_first_pass_loss_once() {
        let mut transfers = HashMap::new();
        let cfg = RecvConfig { out_dir: ".".into(), max_bytes: 1024, rendezvous_file: ".".into(), pool: 1, rotate_secs: 0 };
        let manifest =
            Packet::Manifest { xfer: 1, total_len: 100, chunk_size: 10, chunk_count: 10, crc32: 0, name: "x".into() }
                .encode(0);
        answer(&mut transfers, &HashSet::new(), &cfg, &manifest).unwrap();
        for i in 0..8 {
            transfers.get_mut(&1).unwrap().store(i, &[0u8; 10]);
        }
        let status = Packet::Status { xfer: 1 }.encode(0);
        answer(&mut transfers, &HashSet::new(), &cfg, &status).unwrap();
        // Two more arrive, and the sender asks again.
        transfers.get_mut(&1).unwrap().store(8, &[0u8; 10]);
        transfers.get_mut(&1).unwrap().store(9, &[0u8; 10]);
        let reply = answer(&mut transfers, &HashSet::new(), &cfg, &status).unwrap();
        match Packet::decode(&reply) {
            Some(Packet::StatusReply { state, missing_total, .. }) => {
                assert_eq!(state, XFER_COMPLETE);
                assert_eq!(missing_total, 0);
            }
            other => panic!("answered with {other:?}"),
        }
        let inc = transfers.get(&1).unwrap();
        assert_eq!(inc.first_pass_missing, Some(2), "the first answer is what the first pass lost");
        assert_eq!(inc.repair_rounds, 1);
    }

    #[test]
    fn a_finished_transfer_is_not_forgotten_the_moment_it_is_written() {
        // The sender's status call can time out after we have answered it. It
        // asks again; if that answer were "never heard of it" the sender would
        // send the whole file a second time.
        let mut transfers = HashMap::new();
        let mut completed = HashSet::new();
        completed.insert(1u64);
        let cfg = RecvConfig { out_dir: ".".into(), max_bytes: 1024, rendezvous_file: ".".into(), pool: 1, rotate_secs: 0 };
        let reply =
            answer(&mut transfers, &completed, &cfg, &Packet::Status { xfer: 1 }.encode(0)).unwrap();
        match Packet::decode(&reply) {
            Some(Packet::StatusReply { state, missing_total, .. }) => {
                assert_eq!(state, XFER_COMPLETE);
                assert_eq!(missing_total, 0);
            }
            other => panic!("answered with {other:?}"),
        }
        // And a re-opened manifest for it does not start the file again.
        let manifest = Packet::Manifest {
            xfer: 1,
            total_len: 100,
            chunk_size: 10,
            chunk_count: 10,
            crc32: 0,
            name: "x".into(),
        }
        .encode(0);
        answer(&mut transfers, &completed, &cfg, &manifest).unwrap();
        assert!(transfers.is_empty());
    }

    #[test]
    fn xfer_ids_differ_between_runs_of_the_same_file() {
        assert_ne!(xfer_id("cat.png"), xfer_id("cat.png"));
        assert_ne!(xfer_id("cat.png"), xfer_id("dog.png"));
    }
}
