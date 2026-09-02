//! A social node: one DHT record is your wall, and everything else reads it.
//!
//! The shape is deliberately boring. A record has subkeys; subkey 0 holds the
//! profile and subkeys 1.. hold posts in a ring. That is the whole storage
//! layer. Following someone is remembering their record key; reading their
//! feed is reading their subkeys. There is no server anywhere holding a copy.
//!
//! The record key is the identity. Share it and people can follow you; it
//! survives restarts because the owner keypair is kept on disk, the same trick
//! the file-transfer rendezvous uses. That matters for more than convenience:
//! the profile carries the node's current route blobs, so a private message
//! can find you even after the route it was sent on has died.
//!
//! Nothing here renders HTML. The local server hands the browser JSON and the
//! page builds itself, so what a reader sees is a function of what is in the
//! DHT rather than of what some server chose to say.

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::str::FromStr;
use veilid_core::*;

/// Subkey 0 is the profile; the rest are posts.
pub const PROFILE_SUBKEY: u32 = 0;
/// How many posts a wall keeps. Older ones fall out of the ring.
pub const POST_SLOTS: u32 = 63;
/// Profile + posts.
pub const WALL_SUBKEYS: u16 = (POST_SLOTS + 1) as u16;

/// Values are capped at 32768 bytes, and a post has to fit in one with room
/// for its JSON envelope.
pub const MAX_POST_CHARS: usize = 8000;
pub const MAX_NAME_CHARS: usize = 64;
pub const MAX_BIO_CHARS: usize = 500;

/// Subkey 0. Carries the routes as well as the display fields, so a reader of
/// your wall already knows how to send you a private message.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Profile {
    pub name: String,
    pub bio: String,
    /// Base64 route blobs, most-preferred first. Same list the file transfer
    /// publishes, for the same reason: a spare costs nothing and saves a round
    /// trip when one dies.
    #[serde(default)]
    pub routes: Vec<String>,
    /// Sequence number of the next post, so a reader knows which ring slots
    /// are live and in what order.
    #[serde(default)]
    pub next_seq: u64,
    #[serde(default)]
    pub updated_us: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Post {
    pub seq: u64,
    pub ts_us: u64,
    pub text: String,
    /// Set when the post announces a file the author will send on request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attachment: Option<Attachment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attachment {
    pub name: String,
    pub bytes: u64,
    pub crc32: u32,
}

/// A post as shown in a feed: the post plus who wrote it.
#[derive(Debug, Clone, Serialize)]
pub struct FeedItem {
    pub author_key: String,
    pub author_name: String,
    #[serde(flatten)]
    pub post: Post,
}

/// A private message. Never touches the DHT -- it goes over a private route as
/// an `app_call`, so it is not sitting in a distributed hash table for anyone
/// who learns the key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dm {
    pub from_key: String,
    pub from_name: String,
    pub ts_us: u64,
    pub text: String,
}

/// Which ring slot a post sequence number lives in.
pub fn slot_for(seq: u64) -> u32 {
    PROFILE_SUBKEY + 1 + (seq % POST_SLOTS as u64) as u32
}

/// The sequence numbers still readable on a wall, newest first.
pub fn live_seqs(next_seq: u64) -> Vec<u64> {
    let count = next_seq.min(POST_SLOTS as u64);
    (0..count).map(|i| next_seq - 1 - i).collect()
}

pub fn truncate_chars(s: &str, max: usize) -> String {
    s.chars().take(max).collect()
}

/// Your own wall: the record, its owner keypair, and the profile in it.
pub struct Wall {
    pub rc: RoutingContext,
    pub key: RecordKey,
}

impl Wall {
    /// Reuse the saved identity if there is one so the key -- and therefore who
    /// you are to everyone following you -- survives a restart.
    pub async fn open(
        rc: &RoutingContext,
        path: &Path,
    ) -> Result<Wall, Box<dyn std::error::Error>> {
        if let Ok(txt) = std::fs::read_to_string(path) {
            let mut lines = txt.lines();
            if let (Some(k), Some(kp)) = (lines.next(), lines.next())
                && let (Ok(key), Ok(keypair)) = (RecordKey::from_str(k), KeyPair::from_str(kp))
            {
                match rc.open_dht_record(key.clone(), Some(keypair)).await {
                    Ok(_) => return Ok(Wall { rc: rc.clone(), key }),
                    Err(e) => eprintln!("could not reopen the saved identity: {e}"),
                }
            }
        }
        let desc = rc
            .create_dht_record(VALID_CRYPTO_KINDS[0], DHTSchema::dflt(WALL_SUBKEYS)?, None)
            .await?;
        let key = desc.key();
        if let Some(kp) = desc.owner_keypair() {
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            if let Err(e) = std::fs::write(path, format!("{key}\n{kp}\n")) {
                eprintln!("could not save the identity ({e}); your key will change on restart");
            }
        }
        Ok(Wall { rc: rc.clone(), key })
    }

    pub async fn put_profile(&self, p: &Profile) -> Result<(), Box<dyn std::error::Error>> {
        let json = serde_json::to_vec(p)?;
        if json.len() > ValueData::MAX_LEN {
            return Err("profile is too large for one DHT value".into());
        }
        self.rc.set_dht_value(self.key.clone(), PROFILE_SUBKEY, json, None).await?;
        Ok(())
    }

    pub async fn put_post(&self, post: &Post) -> Result<(), Box<dyn std::error::Error>> {
        let json = serde_json::to_vec(post)?;
        if json.len() > ValueData::MAX_LEN {
            return Err("post is too large for one DHT value".into());
        }
        self.rc.set_dht_value(self.key.clone(), slot_for(post.seq), json, None).await?;
        Ok(())
    }
}

/// Read someone else's wall. `force` skips the local cache, which is what you
/// want when refreshing a feed and not what you want when paging through one.
pub async fn read_profile(
    rc: &RoutingContext,
    key: &RecordKey,
    force: bool,
) -> Result<Profile, Box<dyn std::error::Error>> {
    let value = rc
        .get_dht_value(key.clone(), PROFILE_SUBKEY, force)
        .await?
        .ok_or("that key has no profile published on it")?;
    Ok(serde_json::from_slice(value.data())?)
}

pub async fn read_post(
    rc: &RoutingContext,
    key: &RecordKey,
    seq: u64,
    force: bool,
) -> Option<Post> {
    let value = rc.get_dht_value(key.clone(), slot_for(seq), force).await.ok()??;
    let post: Post = serde_json::from_slice(value.data()).ok()?;
    // The ring means a slot may hold a newer post than the one asked for; that
    // is not corruption, it is the old one having aged out.
    (post.seq == seq).then_some(post)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn posts_land_in_distinct_ring_slots() {
        let slots: Vec<u32> = (0..POST_SLOTS as u64).map(slot_for).collect();
        let unique: std::collections::HashSet<_> = slots.iter().collect();
        assert_eq!(unique.len(), POST_SLOTS as usize, "a full ring uses every slot once");
        assert!(slots.iter().all(|s| *s != PROFILE_SUBKEY), "posts never overwrite the profile");
        assert!(slots.iter().all(|s| (*s as u16) < WALL_SUBKEYS), "slots stay inside the schema");
    }

    #[test]
    fn the_ring_wraps_onto_itself() {
        assert_eq!(slot_for(0), slot_for(POST_SLOTS as u64));
        assert_eq!(slot_for(1), slot_for(POST_SLOTS as u64 + 1));
    }

    #[test]
    fn live_seqs_are_newest_first_and_bounded_by_the_ring() {
        assert_eq!(live_seqs(0), Vec::<u64>::new());
        assert_eq!(live_seqs(3), vec![2, 1, 0]);
        let many = live_seqs(1000);
        assert_eq!(many.len(), POST_SLOTS as usize, "only a ring's worth survives");
        assert_eq!(many[0], 999, "newest first");
        assert_eq!(*many.last().unwrap(), 1000 - POST_SLOTS as u64);
    }

    #[test]
    fn truncation_counts_characters_not_bytes() {
        // A byte-based cut would split these and produce invalid UTF-8.
        assert_eq!(truncate_chars("héllo wörld", 5).chars().count(), 5);
        assert_eq!(truncate_chars("abc", 99), "abc");
        assert_eq!(truncate_chars("", 5), "");
    }

    #[test]
    fn a_profile_roundtrips_through_json() {
        let p = Profile {
            name: "ada".into(),
            bio: "counts things".into(),
            routes: vec!["AAAA".into(), "BBBB".into()],
            next_seq: 7,
            updated_us: 123,
        };
        let back: Profile = serde_json::from_slice(&serde_json::to_vec(&p).unwrap()).unwrap();
        assert_eq!(back.name, "ada");
        assert_eq!(back.routes.len(), 2);
        assert_eq!(back.next_seq, 7);
    }

    #[test]
    fn an_old_profile_without_routes_still_parses() {
        // Fields are additive: a peer running an older build must not break
        // our feed just because its profile lacks a key we added later.
        let p: Profile = serde_json::from_str(r#"{"name":"bob","bio":""}"#).unwrap();
        assert_eq!(p.name, "bob");
        assert!(p.routes.is_empty());
        assert_eq!(p.next_seq, 0);
    }

    #[test]
    fn a_post_without_an_attachment_omits_the_field() {
        let p = Post { seq: 1, ts_us: 2, text: "hi".into(), attachment: None };
        let json = serde_json::to_string(&p).unwrap();
        assert!(!json.contains("attachment"), "absent means absent, not null: {json}");
    }
}
