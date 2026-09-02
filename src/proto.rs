//! Wire format for harness packets.
//!
//! Everything is little-endian at fixed offsets. There is no framing: Veilid
//! delivers a whole `app_message` or nothing at all, so a packet is a message.
//!
//! Timestamps are microseconds since the Unix epoch, taken as close to the
//! actual send/receive as the API allows. The two ends do not share a clock, so
//! only differences taken on a single host are trustworthy on their own; see
//! `stats` for how the round trip cancels the skew back out.
//!
//! Two protocols share this format and this magic. The latency probe
//! (`Hello`/`Probe`/`Echo`) measures the path; the file transfer
//! (`Manifest`/`Chunk`/`Status`) moves bytes over it. They never run at the
//! same time, but sharing the header means a packet from the wrong one is
//! recognised and ignored rather than parsed into nonsense.

use std::sync::OnceLock;

/// Magic prefix, so a stray message from something else is dropped rather than
/// parsed into nonsense.
pub const MAGIC: [u8; 4] = *b"VVC0";

pub const KIND_HELLO: u8 = 1;
pub const KIND_PROBE: u8 = 2;
pub const KIND_ECHO: u8 = 3;
pub const KIND_MANIFEST: u8 = 4;
pub const KIND_MANIFEST_OK: u8 = 5;
pub const KIND_CHUNK: u8 = 6;
pub const KIND_STATUS: u8 = 7;
pub const KIND_STATUS_REPLY: u8 = 8;
pub const KIND_ROUTES: u8 = 9;
pub const KIND_SOCIAL: u8 = 10;

/// magic + kind
pub const HEADER_LEN: usize = 5;
/// header + seq + t0
pub const PROBE_LEN: usize = HEADER_LEN + 8 + 8;
/// header + seq + t0 + t1 + t2
pub const ECHO_LEN: usize = HEADER_LEN + 8 + 8 + 8 + 8;

/// Veilid's own cap on an `app_message` payload, from
/// `veilid-core/src/rpc_processor/coders/operations/operation_app_message.rs`.
/// Sending more than this fails at the coder, not on the wire. `app_call`
/// applies the same cap to both the question and the answer.
pub const MAX_APP_MESSAGE: usize = 32768;

/// header + transfer id + chunk index; everything after this is payload.
pub const CHUNK_HEADER_LEN: usize = HEADER_LEN + 8 + 4;

/// The most payload one chunk can carry and still fit a single `app_message`.
pub const MAX_CHUNK_DATA: usize = MAX_APP_MESSAGE - CHUNK_HEADER_LEN;

/// What the responder knows about a transfer id it was asked about.
pub const XFER_UNKNOWN: u8 = 0;
pub const XFER_IN_PROGRESS: u8 = 1;
pub const XFER_COMPLETE: u8 = 2;

/// header + xfer + state + missing_total + listed
const STATUS_REPLY_HEADER_LEN: usize = HEADER_LEN + 8 + 1 + 4 + 4;

/// How many missing indices one `StatusReply` can name. Anything past this is
/// still counted in `missing_total` and picked up on the next round.
pub const MAX_MISSING_LISTED: usize = (MAX_APP_MESSAGE - STATUS_REPLY_HEADER_LEN) / 4;

/// Microseconds since the Unix epoch.
pub fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// CRC-32 (IEEE, the one gzip and zip use), so a received file can be checked
/// against what was actually read off the sender's disk. A wrong chunk that
/// somehow passed length validation shows up here.
pub fn crc32(data: &[u8]) -> u32 {
    static TABLE: OnceLock<[u32; 256]> = OnceLock::new();
    let table = TABLE.get_or_init(|| {
        let mut t = [0u32; 256];
        for (i, e) in t.iter_mut().enumerate() {
            let mut c = i as u32;
            for _ in 0..8 {
                c = if c & 1 != 0 { 0xEDB8_8320 ^ (c >> 1) } else { c >> 1 };
            }
            *e = c;
        }
        t
    });
    let mut crc = !0u32;
    for &b in data {
        crc = (crc >> 8) ^ table[((crc ^ b as u32) & 0xff) as usize];
    }
    !crc
}

#[derive(Debug, Clone)]
pub enum Packet {
    /// Prober tells the responder which route to echo back on. Sent on every
    /// route rotation, so the responder always holds the live one.
    Hello { return_route: Vec<u8> },
    /// Prober to responder. `t0` is the prober's clock at send.
    Probe { seq: u64, t0: u64 },
    /// Responder back to prober. `t1` is receipt, `t2` is echo send, both on
    /// the responder's clock.
    Echo { seq: u64, t0: u64, t1: u64, t2: u64 },

    /// Opens a transfer. Sent as an `app_call`, so the sender knows the
    /// receiver has it before a single chunk goes out.
    Manifest {
        xfer: u64,
        total_len: u64,
        chunk_size: u32,
        chunk_count: u32,
        crc32: u32,
        name: String,
    },
    /// The answer to a `Manifest`. A refusal carries its reason.
    ManifestOk { xfer: u64, accepted: bool, message: String },
    /// One piece of the file, sent fire-and-forget. `index` is what puts it
    /// back in the right place, so arrival order does not matter.
    Chunk { xfer: u64, index: u32, data: Vec<u8> },
    /// "What are you still missing?", sent as an `app_call` between passes.
    Status { xfer: u64 },
    /// The answer to a `Status`. `missing` is truncated to what fits one
    /// message; `missing_total` is the honest count.
    StatusReply { xfer: u64, state: u8, missing_total: u32, missing: Vec<u32> },

    /// A social-layer message: JSON over a private route, used for anything
    /// that must not sit in the DHT. Private messages are the reason -- a
    /// record anyone can read is the wrong place for them.
    ///
    /// The payload is JSON rather than fixed offsets so the social protocol
    /// can gain fields without a wire-format change; `proto` stays the framing
    /// and does not need to know what a direct message is.
    Social { json: Vec<u8> },

    /// The value published in the rendezvous record: every route the receiver
    /// currently has, most-preferred first.
    ///
    /// This never travels over a route; it is what a DHT subkey holds. A list
    /// rather than one blob is what makes failover free: the sender already
    /// holds the alternatives, so a dead route costs a local retry instead of
    /// a DHT round trip.
    Routes { blobs: Vec<Vec<u8>> },
}

fn put_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn get_u64(buf: &[u8], off: usize) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&buf[off..off + 8]);
    u64::from_le_bytes(b)
}

fn put_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn get_u32(buf: &[u8], off: usize) -> u32 {
    let mut b = [0u8; 4];
    b.copy_from_slice(&buf[off..off + 4]);
    u32::from_le_bytes(b)
}

/// A length-prefixed string. Two bytes of length is plenty for a file name and
/// keeps a hostile prefix from asking for a large allocation.
fn put_str(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    let len = bytes.len().min(u16::MAX as usize);
    buf.extend_from_slice(&(len as u16).to_le_bytes());
    buf.extend_from_slice(&bytes[..len]);
}

fn get_str(buf: &[u8], off: usize) -> Option<(String, usize)> {
    if buf.len() < off + 2 {
        return None;
    }
    let len = u16::from_le_bytes([buf[off], buf[off + 1]]) as usize;
    let start = off + 2;
    let end = start.checked_add(len)?;
    if buf.len() < end {
        return None;
    }
    Some((String::from_utf8_lossy(&buf[start..end]).into_owned(), end))
}

impl Packet {
    /// Encode, padded with zeros to `pad_to` bytes. Padding is what lets a probe
    /// stand in for a real media frame; the trailing bytes are never read.
    ///
    /// Only `Probe` and `Echo` are padded. Everything else carries a payload
    /// whose length is the message length, so padding would be indistinguishable
    /// from data.
    pub fn encode(&self, pad_to: usize) -> Vec<u8> {
        // Clamp once, up front: a caller-supplied size must never be able to
        // ask for an allocation, only for padding Veilid would actually carry.
        let pad_to = pad_to.min(MAX_APP_MESSAGE);
        let mut buf = Vec::with_capacity(pad_to.max(ECHO_LEN));
        buf.extend_from_slice(&MAGIC);
        match self {
            Packet::Hello { return_route } => {
                buf.push(KIND_HELLO);
                put_u64(&mut buf, return_route.len() as u64);
                buf.extend_from_slice(return_route);
                return buf; // never padded; the blob is the payload
            }
            Packet::Probe { seq, t0 } => {
                buf.push(KIND_PROBE);
                put_u64(&mut buf, *seq);
                put_u64(&mut buf, *t0);
            }
            Packet::Echo { seq, t0, t1, t2 } => {
                buf.push(KIND_ECHO);
                put_u64(&mut buf, *seq);
                put_u64(&mut buf, *t0);
                put_u64(&mut buf, *t1);
                put_u64(&mut buf, *t2);
            }
            Packet::Manifest { xfer, total_len, chunk_size, chunk_count, crc32, name } => {
                buf.push(KIND_MANIFEST);
                put_u64(&mut buf, *xfer);
                put_u64(&mut buf, *total_len);
                put_u32(&mut buf, *chunk_size);
                put_u32(&mut buf, *chunk_count);
                put_u32(&mut buf, *crc32);
                put_str(&mut buf, name);
                return buf;
            }
            Packet::ManifestOk { xfer, accepted, message } => {
                buf.push(KIND_MANIFEST_OK);
                put_u64(&mut buf, *xfer);
                buf.push(u8::from(*accepted));
                put_str(&mut buf, message);
                return buf;
            }
            Packet::Chunk { xfer, index, data } => {
                buf.push(KIND_CHUNK);
                put_u64(&mut buf, *xfer);
                put_u32(&mut buf, *index);
                // Truncated rather than rejected: a chunk longer than one
                // message could not be sent anyway, and the receiver checks
                // every chunk's length against the manifest.
                let take = data.len().min(MAX_CHUNK_DATA);
                buf.extend_from_slice(&data[..take]);
                return buf;
            }
            Packet::Status { xfer } => {
                buf.push(KIND_STATUS);
                put_u64(&mut buf, *xfer);
                return buf;
            }
            Packet::Social { json } => {
                buf.push(KIND_SOCIAL);
                let take = json.len().min(MAX_APP_MESSAGE - HEADER_LEN);
                buf.extend_from_slice(&json[..take]);
                return buf;
            }
            Packet::Routes { blobs } => {
                buf.push(KIND_ROUTES);
                let n = blobs.len().min(u16::MAX as usize);
                buf.extend_from_slice(&(n as u16).to_le_bytes());
                for b in &blobs[..n] {
                    let len = b.len().min(u16::MAX as usize);
                    buf.extend_from_slice(&(len as u16).to_le_bytes());
                    buf.extend_from_slice(&b[..len]);
                }
                return buf;
            }
            Packet::StatusReply { xfer, state, missing_total, missing } => {
                buf.push(KIND_STATUS_REPLY);
                put_u64(&mut buf, *xfer);
                buf.push(*state);
                put_u32(&mut buf, *missing_total);
                let listed = missing.len().min(MAX_MISSING_LISTED);
                put_u32(&mut buf, listed as u32);
                for &i in &missing[..listed] {
                    put_u32(&mut buf, i);
                }
                return buf;
            }
        }
        if buf.len() < pad_to {
            buf.resize(pad_to, 0);
        }
        buf
    }

    pub fn decode(buf: &[u8]) -> Option<Packet> {
        if buf.len() < HEADER_LEN || buf[..4] != MAGIC {
            return None;
        }
        match buf[4] {
            KIND_HELLO => {
                if buf.len() < HEADER_LEN + 8 {
                    return None;
                }
                let len = get_u64(buf, HEADER_LEN) as usize;
                let start = HEADER_LEN + 8;
                let end = start.checked_add(len)?;
                if buf.len() < end {
                    return None;
                }
                Some(Packet::Hello {
                    return_route: buf[start..end].to_vec(),
                })
            }
            KIND_PROBE if buf.len() >= PROBE_LEN => Some(Packet::Probe {
                seq: get_u64(buf, HEADER_LEN),
                t0: get_u64(buf, HEADER_LEN + 8),
            }),
            KIND_ECHO if buf.len() >= ECHO_LEN => Some(Packet::Echo {
                seq: get_u64(buf, HEADER_LEN),
                t0: get_u64(buf, HEADER_LEN + 8),
                t1: get_u64(buf, HEADER_LEN + 16),
                t2: get_u64(buf, HEADER_LEN + 24),
            }),
            KIND_MANIFEST if buf.len() >= HEADER_LEN + 28 => {
                let (name, _) = get_str(buf, HEADER_LEN + 28)?;
                Some(Packet::Manifest {
                    xfer: get_u64(buf, HEADER_LEN),
                    total_len: get_u64(buf, HEADER_LEN + 8),
                    chunk_size: get_u32(buf, HEADER_LEN + 16),
                    chunk_count: get_u32(buf, HEADER_LEN + 20),
                    crc32: get_u32(buf, HEADER_LEN + 24),
                    name,
                })
            }
            KIND_MANIFEST_OK if buf.len() >= HEADER_LEN + 9 => {
                let (message, _) = get_str(buf, HEADER_LEN + 9)?;
                Some(Packet::ManifestOk {
                    xfer: get_u64(buf, HEADER_LEN),
                    accepted: buf[HEADER_LEN + 8] != 0,
                    message,
                })
            }
            KIND_CHUNK if buf.len() >= CHUNK_HEADER_LEN => Some(Packet::Chunk {
                xfer: get_u64(buf, HEADER_LEN),
                index: get_u32(buf, HEADER_LEN + 8),
                data: buf[CHUNK_HEADER_LEN..].to_vec(),
            }),
            KIND_STATUS if buf.len() >= HEADER_LEN + 8 => Some(Packet::Status {
                xfer: get_u64(buf, HEADER_LEN),
            }),
            KIND_SOCIAL => Some(Packet::Social { json: buf[HEADER_LEN..].to_vec() }),
            KIND_ROUTES if buf.len() >= HEADER_LEN + 2 => {
                let n = u16::from_le_bytes([buf[HEADER_LEN], buf[HEADER_LEN + 1]]) as usize;
                let mut blobs = Vec::with_capacity(n.min(64));
                let mut off = HEADER_LEN + 2;
                for _ in 0..n {
                    if buf.len() < off + 2 {
                        return None;
                    }
                    let len = u16::from_le_bytes([buf[off], buf[off + 1]]) as usize;
                    let start = off + 2;
                    let end = start.checked_add(len)?;
                    if buf.len() < end {
                        return None;
                    }
                    blobs.push(buf[start..end].to_vec());
                    off = end;
                }
                Some(Packet::Routes { blobs })
            }
            KIND_STATUS_REPLY if buf.len() >= STATUS_REPLY_HEADER_LEN => {
                let listed = get_u32(buf, HEADER_LEN + 13) as usize;
                let start = STATUS_REPLY_HEADER_LEN;
                let end = start.checked_add(listed.checked_mul(4)?)?;
                if buf.len() < end {
                    return None;
                }
                Some(Packet::StatusReply {
                    xfer: get_u64(buf, HEADER_LEN),
                    state: buf[HEADER_LEN + 8],
                    missing_total: get_u32(buf, HEADER_LEN + 9),
                    missing: (0..listed).map(|i| get_u32(buf, start + i * 4)).collect(),
                })
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_survives_padding() {
        let p = Packet::Probe { seq: 42, t0: 1234567 };
        let enc = p.encode(1200);
        assert_eq!(enc.len(), 1200);
        match Packet::decode(&enc) {
            Some(Packet::Probe { seq, t0 }) => {
                assert_eq!(seq, 42);
                assert_eq!(t0, 1234567);
            }
            other => panic!("decoded as {other:?}"),
        }
    }

    #[test]
    fn echo_roundtrips() {
        let p = Packet::Echo { seq: 7, t0: 1, t1: 2, t2: 3 };
        match Packet::decode(&p.encode(0)) {
            Some(Packet::Echo { seq, t0, t1, t2 }) => {
                assert_eq!((seq, t0, t1, t2), (7, 1, 2, 3));
            }
            other => panic!("decoded as {other:?}"),
        }
    }

    #[test]
    fn hello_carries_blob() {
        let blob = vec![9u8; 300];
        let p = Packet::Hello { return_route: blob.clone() };
        match Packet::decode(&p.encode(0)) {
            Some(Packet::Hello { return_route }) => assert_eq!(return_route, blob),
            other => panic!("decoded as {other:?}"),
        }
    }

    #[test]
    fn padding_never_exceeds_the_app_message_cap() {
        let enc = Packet::Probe { seq: 1, t0: 1 }.encode(usize::MAX);
        assert_eq!(enc.len(), MAX_APP_MESSAGE);
    }

    #[test]
    fn foreign_and_truncated_messages_are_rejected() {
        assert!(Packet::decode(b"hello there").is_none());
        assert!(Packet::decode(&[]).is_none());
        let short = &Packet::Probe { seq: 1, t0: 1 }.encode(0)[..10];
        assert!(Packet::decode(short).is_none());
    }

    #[test]
    fn manifest_roundtrips() {
        let p = Packet::Manifest {
            xfer: 0xdead_beef_cafe_f00d,
            total_len: 1_048_576,
            chunk_size: 16384,
            chunk_count: 64,
            crc32: 0x1234_5678,
            name: "cat.png".into(),
        };
        match Packet::decode(&p.encode(0)) {
            Some(Packet::Manifest { xfer, total_len, chunk_size, chunk_count, crc32, name }) => {
                assert_eq!(xfer, 0xdead_beef_cafe_f00d);
                assert_eq!(total_len, 1_048_576);
                assert_eq!((chunk_size, chunk_count, crc32), (16384, 64, 0x1234_5678));
                assert_eq!(name, "cat.png");
            }
            other => panic!("decoded as {other:?}"),
        }
    }

    #[test]
    fn manifest_refusal_carries_its_reason() {
        let p = Packet::ManifestOk { xfer: 1, accepted: false, message: "too big".into() };
        match Packet::decode(&p.encode(0)) {
            Some(Packet::ManifestOk { accepted, message, .. }) => {
                assert!(!accepted);
                assert_eq!(message, "too big");
            }
            other => panic!("decoded as {other:?}"),
        }
    }

    #[test]
    fn a_full_chunk_fits_one_app_message() {
        let data = vec![7u8; MAX_CHUNK_DATA];
        let enc = Packet::Chunk { xfer: 5, index: 9, data: data.clone() }.encode(0);
        assert_eq!(enc.len(), MAX_APP_MESSAGE);
        match Packet::decode(&enc) {
            Some(Packet::Chunk { xfer, index, data: got }) => {
                assert_eq!((xfer, index), (5, 9));
                assert_eq!(got, data);
            }
            other => panic!("decoded as {other:?}"),
        }
    }

    #[test]
    fn an_empty_chunk_is_still_a_chunk() {
        // The last chunk of a file whose length is an exact multiple of the
        // chunk size is never sent, but a zero-length one must not decode as
        // garbage if it is.
        match Packet::decode(&Packet::Chunk { xfer: 1, index: 0, data: vec![] }.encode(0)) {
            Some(Packet::Chunk { data, .. }) => assert!(data.is_empty()),
            other => panic!("decoded as {other:?}"),
        }
    }

    #[test]
    fn status_reply_roundtrips_and_truncates() {
        let missing: Vec<u32> = (0..MAX_MISSING_LISTED as u32 + 500).collect();
        let p = Packet::StatusReply {
            xfer: 3,
            state: XFER_IN_PROGRESS,
            missing_total: missing.len() as u32,
            missing,
        };
        let enc = p.encode(0);
        assert!(enc.len() <= MAX_APP_MESSAGE);
        match Packet::decode(&enc) {
            Some(Packet::StatusReply { state, missing_total, missing, .. }) => {
                assert_eq!(state, XFER_IN_PROGRESS);
                // The honest count survives even though the list did not.
                assert_eq!(missing_total, MAX_MISSING_LISTED as u32 + 500);
                assert_eq!(missing.len(), MAX_MISSING_LISTED);
                assert_eq!(missing[0], 0);
            }
            other => panic!("decoded as {other:?}"),
        }
    }

    #[test]
    fn status_reply_with_a_lying_length_is_rejected() {
        let mut enc = Packet::StatusReply {
            xfer: 1,
            state: XFER_IN_PROGRESS,
            missing_total: 2,
            missing: vec![1, 2],
        }
        .encode(0);
        // Claim far more indices than the message actually carries.
        enc[HEADER_LEN + 13..HEADER_LEN + 17].copy_from_slice(&9999u32.to_le_bytes());
        assert!(Packet::decode(&enc).is_none());
    }

    #[test]
    fn routes_list_roundtrips() {
        let blobs = vec![vec![1u8; 900], vec![2u8; 1100], vec![3u8; 5]];
        match Packet::decode(&Packet::Routes { blobs: blobs.clone() }.encode(0)) {
            Some(Packet::Routes { blobs: got }) => assert_eq!(got, blobs),
            other => panic!("decoded as {other:?}"),
        }
    }

    #[test]
    fn an_empty_routes_list_is_valid() {
        match Packet::decode(&Packet::Routes { blobs: vec![] }.encode(0)) {
            Some(Packet::Routes { blobs }) => assert!(blobs.is_empty()),
            other => panic!("decoded as {other:?}"),
        }
    }

    #[test]
    fn a_routes_list_with_a_lying_length_is_rejected() {
        let mut enc = Packet::Routes { blobs: vec![vec![7u8; 10]] }.encode(0);
        // Claim a blob far longer than the message actually carries.
        enc[HEADER_LEN + 2..HEADER_LEN + 4].copy_from_slice(&9999u16.to_le_bytes());
        assert!(Packet::decode(&enc).is_none());
    }

    #[test]
    fn a_raw_blob_does_not_decode_as_a_routes_list() {
        // v0.2.0 receivers published a bare route blob. It has no magic, so it
        // must not be mistaken for a list -- the sender falls back on it.
        assert!(Packet::decode(&[0x41, 0x52, 0x10, 0x64, 0x50]).is_none());
    }

    #[test]
    fn a_social_payload_roundtrips() {
        let json = br#"{"t":"dm","text":"hello"}"#.to_vec();
        match Packet::decode(&Packet::Social { json: json.clone() }.encode(0)) {
            Some(Packet::Social { json: got }) => assert_eq!(got, json),
            other => panic!("decoded as {other:?}"),
        }
    }

    #[test]
    fn an_oversized_social_payload_is_truncated_not_rejected_by_the_coder() {
        let enc = Packet::Social { json: vec![b'x'; MAX_APP_MESSAGE * 2] }.encode(0);
        assert_eq!(enc.len(), MAX_APP_MESSAGE, "must still fit one app_message");
    }

    #[test]
    fn crc32_matches_the_standard_check_value() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
    }
}
