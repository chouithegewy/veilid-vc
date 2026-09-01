# veilid-vc

A latency harness for Veilid's `app_message`.

Veilid has no streaming primitive. `app_message` (fire-and-forget) and `app_call`
(request/response) are the only ways to move bytes to a peer, both capped at
32,768 bytes, and every message crosses your safety route and then the peer's
private route. Whether a voice call can live on that is an empirical question,
and nobody has published the answer.

This measures it: 50 small packets a second through a private route, timestamped
and echoed, reported as a latency distribution.

It also moves files across the same routes, because the second thing you want to
know about a transport is whether a few hundred kilobytes survive it. See
[Sending files](#sending-files).

## Getting it onto a machine

Each release carries a prebuilt `veilid-vc-x86_64-linux`. It is built inside an
Ubuntu 20.04 container and needs **glibc 2.30 or newer** — Ubuntu 20.04+,
Debian 11+, RHEL/Rocky 9, Amazon Linux 2023. It links nothing but libc, so
there is no Rust toolchain and nothing to install on the far end: drop the file
somewhere on `$PATH`, `chmod +x`, run it.

Two platforms it will *not* run on: RHEL/CentOS 8 and its rebuilds (glibc 2.28,
below the floor) and Alpine or anything else on musl. Both need a build from
source.

Check before you copy it anywhere:

```
ldd --version | head -1        # needs 2.30 or newer
```

```
curl -fsSL -o veilid-vc https://github.com/chouithegewy/veilid-vc/releases/latest/download/veilid-vc-x86_64-linux
chmod +x veilid-vc
./veilid-vc --help
```

That URL always points at the newest release. To pin a version, swap
`latest/download` for `download/v0.1.0`. Each release also publishes a
`.sha256` next to the binary:

```
curl -fsSL -O https://github.com/chouithegewy/veilid-vc/releases/latest/download/veilid-vc-x86_64-linux.sha256
echo "$(cat veilid-vc-x86_64-linux.sha256)  veilid-vc" | sha256sum -c -
```

Building from source needs Rust 1.85 or newer (the crate is edition 2024):
`cargo build --release`.

## Running it

Two terminals. The listener creates a private route and prints its blob:

```
cargo run --release -- listen
```

The prober imports that blob, stands up a return route of its own, and paces
packets at it:

```
cargo run --release -- probe --connect <blob> --duration 60
```

Both ends print a report on exit. Ctrl-C ends a run early.

Attaching to the public network takes 30–60 seconds before a route can be built;
`Waiting for network...` is normal.

## Sending files

The receiver publishes a route the same way the listener does:

```
cargo run --release -- recv --out ./inbox
```

It prints two ways to reach it. Prefer the first:

```
cargo run --release -- send --rendezvous <dht-key> photo.jpg screenshot.png
cargo run --release -- send --connect    <blob>    photo.jpg screenshot.png
```

`--connect` names one private route. Routes die on their own schedule, and when
that one does the run ends — there is no way to learn its replacement, because
the path you would ask over is the path that just died.

`--rendezvous` takes a DHT record key instead. The receiver writes its current
route blob into that record and rewrites it every time it rebuilds, so the key
is stable: share it once and it keeps working across route deaths and receiver
restarts. The key is kept with its owner keypair in `.veilid/recv/rendezvous`
beside the binary, so restarting the receiver reuses the same key rather than
minting a new one.

The receiver keeps a **pool** of routes (`--pool`, default 3) and publishes all
of them at once, so the sender already holds the alternatives. Failover is then
a local decision — try the next one — with no round trip and no dependence on
the receiver being reachable at that instant. Only when every published route
has failed is a DHT re-read worth the latency.

Routes are also replaced **before** they fail. One is rotated out every
`--rotate-secs / --pool` seconds, so no route lives much past `--rotate-secs`
(default 600) and they stagger rather than ageing out together. The order is
make-before-break: the replacement is built and published first, and the
retired route is kept answering for a further 90 seconds, so a sender that read
the record just before a rotation is never stranded. A 2 MB transfer across
five rotations completed with no loss, no repair passes and no failover.

The escalation when one dies anyway, cheapest first:

1. **Spare from the pool.** Instant, local, cannot fail on its own.
2. **Re-read the record.** One DHT fetch, for when the whole pool is stale.
3. **Give up**, after three fruitless refreshes — a live receiver would have
   republished by then, so the diagnosis is that it is gone.

Two things count as a route failing. Veilid reporting it dead is the obvious
one, but it decides that lazily: long before the event fires, control calls on
that route start returning `could not get remote private route`. Both trigger
failover. A manifest the receiver *refuses* does not — another route would be
refused identically.

Whatever the sender comes back on, it **resumes the same transfer** rather than
restarting: transfers are keyed by a random id rather than by route, and the
receiver answers a repeated manifest by keeping the chunks it already has.
Nothing already delivered is sent twice.

Completed files are written to `--out` (default `./inbox`), never overwriting:
a second `photo.jpg` lands as `photo-1.jpg`. Both ends print what the path cost
them — throughput, how many chunks the first pass lost, how many repair passes
it took — and the receiver checks the file's CRC-32 against the sender's.

### How it gets there

Veilid has no file transfer and neither primitive is one on its own.
`app_message` is fire-and-forget, so some chunks will not arrive. `app_call` is
reliable but spends a full round trip, and a round trip through two private
routes is the expensive thing here. So each is used for what it is good at:

1. **Manifest**, over `app_call`. Name, length, chunking and CRC-32. Nothing is
   sent until the receiver has acknowledged it, so afterwards both ends agree
   on what "complete" means.
2. **Chunks**, over `app_message`, paced and several in flight. This is the part
   that has to be fast and the part that loses packets. Each chunk carries its
   index, so arrival order does not matter.
3. **Status**, over `app_call`. "What are you still missing?" The receiver names
   the indices it does not have; the sender resends exactly those and asks
   again, until the answer is "nothing".

That makes the transfer reliable without an acknowledgement per chunk — and the
size of the *first* answer is a direct measurement of what `app_message` loss
looks like at that chunk size, which is the number worth writing down.

The receiver never needs a route back: an `app_call` answer returns down the
path the question arrived on. Only the receiver has to publish a blob.

### Options

| Flag | Default | What it does |
|---|---|---|
| `--out <dir>` | `inbox` | *(recv)* Where completed files are written. Created if missing. |
| `--max-bytes` | 268435456 | *(recv)* Refuse a transfer larger than this. The file is assembled in memory, and anyone holding the blob can open one. |
| `--pool <n>` | 3 | *(recv)* How many routes to keep published at once. Spares let a sender fail over locally instead of going back to the DHT. Clamped to 1–8. |
| `--rotate-secs` | 600 | *(recv)* Roughly how long any one route may live before being replaced ahead of failing. 0 disables rotation. |
| `--chunk` | 16384 | *(send)* Payload bytes per chunk. Larger is fewer round trips; smaller survives a lossy path better. Capped at 32751, what one `app_message` holds after the header. |
| `--rate` | 20 | *(send)* Chunks per second. `--rate` × `--chunk` is the offered throughput; the default asks for about 320 KiB/s. |
| `--settle-ms` | 1500 | *(send)* How long to wait after a pass before asking what is missing. Wants to be a couple of round trips of the measured RTT. |
| `--rounds` | 20 | *(send)* Give up after this many repair passes. |
| `--rendezvous <key>` | — | *(send)* DHT record key printed by `recv`. Survives route death; mutually exclusive with `--connect`. |

The routing flags below (`--hops`, `--unsafe`, `--stability`, `--sequencing`)
apply to `recv` and `send` too.

If a transfer stalls in repair passes that never shrink, the path is losing
chunks faster than they can be replaced: drop `--rate`, then `--chunk`.

## What the numbers mean

**Round-trip time is trustworthy.** Each echo carries four timestamps, and
`rtt = (t3 - t0) - (t2 - t1)` subtracts the responder's own dwell time. Both
subtractions stay on a single clock, so the offset between the two machines
cancels exactly. This is the number to design against.

**One-way delay is not.** `t1 - t0` crosses two unsynchronised clocks and carries
whatever they disagree by — possibly hours. It is reported anyway because its
*variation* is still real: skew is constant over a short run, so it drops out of
the jitter figure and out of the spread between percentiles. Read the shape, not
the absolute value.

**Loss is counted twice.** The prober knows exactly how many probes it sent, so
its unanswered count is exact — but it conflates a lost probe with a lost echo.
The responder infers loss from gaps in the sequence numbers that arrived, which
isolates the forward path but cannot see anything dropped after the last packet
to land. Compare the two.

**Route death is a result, not a failure.** Private routes die on their own
schedule. The prober rebuilds its own return route and carries on; when the
*remote* route dies there is no in-band way to get a fresh blob, so the run ends
and the elapsed time is reported. For a call application that number is the whole
problem, which is why it is measured rather than worked around.

## Options

| Flag | Default | What it does |
|---|---|---|
| `--rate` | 50 | Packets per second. 50 matches 20 ms Opus frames. |
| `--size` | 64 | Bytes per packet, padded. ~64 is a real Opus voice frame; 1200 is a video packet. Clamped to 32768. |
| `--duration` | 60 | Seconds. 0 runs until Ctrl-C. |
| `--csv <path>` | — | One row per echoed packet: `seq,t0,t1,t2,t3,rtt,fwd,rev`, microseconds. |
| `--hops <n>` | node default | Private route hop count. The biggest single lever on latency. |
| `--unsafe` | off | Drop the safety route. The lowest latency Veilid can offer, at the cost of revealing this node to the far end. |
| `--stability` | low-latency | `low-latency` or `reliable`. |
| `--sequencing` | prefer-unordered | Media wants unordered — a late packet is worthless. |
| `-d` | | `-d` info, `-dd` debug, `-ddd` trace. |

These are global: `--hops`, `--unsafe`, `--stability` and `--sequencing` apply
to `recv` and `send` as well.

## Sweeps worth running

The point is comparison, not a single number:

```
# The floor: no safety route, one hop.
cargo run --release -- probe --connect <blob> --unsafe --hops 1

# The cost of each hop.
for h in 1 2 3; do cargo run --release -- probe --connect <blob> --hops $h --duration 60; done

# Voice-sized versus video-sized packets.
cargo run --release -- probe --connect <blob> --size 64
cargo run --release -- probe --connect <blob> --size 1200

# Can the node hold a real frame rate at all?
cargo run --release -- probe --connect <blob> --rate 50 --duration 300
```

Run against a private dev network first — see `dev-setup/dev-network-setup.md`
in the veilid repo. The public network is a moving target and you want a
baseline you control. Then run against the public network at several times of
day, because that is what your users will get.

## Layout

- `src/proto.rs` — wire format for both protocols, and CRC-32. Hello carries a
  return-route blob; Probe and Echo carry sequence numbers and timestamps,
  padded to `--size`; Manifest, Chunk and Status carry a file.
- `src/stats.rs` — percentiles, RFC 3550 interarrival jitter, sequence
  bookkeeping, and the report.
- `src/roles.rs` — the two ends of the measurement, route construction, and
  route-death handling.
- `src/transfer.rs` — the two ends of a file transfer: chunking, reassembly,
  the missing-chunk repair loop, and the DHT rendezvous that lets a transfer
  outlive the route it started on.
- `src/main.rs` — CLI and Veilid node lifecycle.

`cargo test` covers the encoding, the percentiles, the loss/reorder/duplicate
accounting, that jitter is immune to a constant clock offset, that a file
reassembles out of order, and that a hostile file name cannot write outside
`--out`.

## Caveats

`always_use_insecure_storage` is set, because this tool holds nothing worth
protecting. Do not carry that setting into anything that stores a real identity.

A route blob is a capability. Anyone who has one can open a transfer against
that receiver, so `recv` treats what arrives as hostile: `--max-bytes` bounds
the allocation, chunk lengths are checked against the manifest, and the file
name is stripped to a bare basename before anything is written. It is still not
an authenticated channel — hand the blob to one peer, and restart when you are
done with it.
