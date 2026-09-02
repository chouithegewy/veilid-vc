//! The running social node: a wall in the DHT, a private route for messages,
//! and a local page to drive both.
//!
//! Two loops share the node. One services Veilid updates -- private messages
//! arriving over a route, routes dying and being replaced. The other answers
//! the browser. They meet at a mutex around a small pile of state, which is
//! enough because everything here is human-paced.

use crate::proto::{now_us, Packet};
use crate::roles::{create_route, routing_context, try_again_loop, RouteParams, Stamped, Updates};
use crate::social::*;
use crate::web::{self, Request, Response};
use serde_json::json;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use veilid_core::*;

pub struct ServeConfig {
    pub port: u16,
    pub identity_file: PathBuf,
    pub follows_file: PathBuf,
    pub pool: usize,
}

struct State {
    profile: Profile,
    /// Our own posts, mirrored locally so the feed does not have to read our
    /// own record back out of the DHT.
    posts: Vec<Post>,
    follows: Vec<String>,
    inbox: Vec<Dm>,
    routes: Vec<RouteId>,
}

struct Node {
    api: VeilidAPI,
    rc: RoutingContext,
    wall: Wall,
    state: Mutex<State>,
}

impl Node {
    fn key_string(&self) -> String {
        self.wall.key.to_string()
    }

    /// Republish the profile. Called whenever anything in it changes, because
    /// it carries the routes as well as the display fields -- a stale profile
    /// means nobody can send you a message.
    async fn publish_profile(&self) -> Result<(), Box<dyn std::error::Error>> {
        let profile = {
            let mut st = self.state.lock().await;
            st.profile.updated_us = now_us();
            st.profile.clone()
        };
        self.wall.put_profile(&profile).await
    }

    async fn set_routes(&self, blobs: Vec<String>, ids: Vec<RouteId>) {
        let mut st = self.state.lock().await;
        st.profile.routes = blobs;
        st.routes = ids;
    }

    /// Send a private message over the peer's published route. Never touches
    /// the DHT: a record anyone can read is the wrong place for private mail.
    async fn send_dm(&self, to: &str, text: &str) -> Result<(), Box<dyn std::error::Error>> {
        let key = RecordKey::from_str(to).map_err(|_| "that does not look like a node key")?;
        // A syntactically valid key can still be nonsense, and the error the
        // DHT gives for that talks about byte lengths, which helps nobody.
        self.rc
            .open_dht_record(key.clone(), None)
            .await
            .map_err(|_| "no node is published under that key")?;
        let peer = read_profile(&self.rc, &key, true).await?;
        if peer.routes.is_empty() {
            return Err("that node has published no route to receive messages on".into());
        }

        let (me, my_name) = {
            let st = self.state.lock().await;
            (self.key_string(), st.profile.name.clone())
        };
        let payload = Packet::Social {
            json: serde_json::to_vec(&json!({
                "t": "dm",
                "from": me,
                "name": my_name,
                "text": text,
                "ts": now_us(),
            }))?,
        }
        .encode(0);

        // Try each published route before giving up: the peer publishes spares
        // precisely so a dead one costs nothing.
        let mut last = String::from("no routes could be imported");
        for blob in &peer.routes {
            let Ok(raw) = data_encoding::BASE64.decode(blob.as_bytes()) else { continue };
            let Ok(target) = self.api.import_remote_private_route(raw) else { continue };
            match self.rc.app_call(Target::RouteId(target), payload.clone()).await {
                Ok(_) => return Ok(()),
                Err(e) => last = e.to_string(),
            }
        }
        Err(format!("could not reach that node on any of its routes: {last}").into())
    }

    /// Everything we can see, newest first: our own posts and those of the
    /// people we follow.
    async fn feed(&self) -> Vec<FeedItem> {
        let (follows, mine, my_name, my_key) = {
            let st = self.state.lock().await;
            (st.follows.clone(), st.posts.clone(), st.profile.name.clone(), self.key_string())
        };

        let mut items: Vec<FeedItem> = mine
            .into_iter()
            .map(|post| FeedItem {
                author_key: my_key.clone(),
                author_name: my_name.clone(),
                post,
            })
            .collect();

        for key_str in follows {
            let Ok(key) = RecordKey::from_str(&key_str) else { continue };
            let _ = self.rc.open_dht_record(key.clone(), None).await;
            let Ok(profile) = read_profile(&self.rc, &key, true).await else { continue };
            let name = if profile.name.is_empty() {
                key_str.chars().take(12).collect()
            } else {
                profile.name.clone()
            };
            for seq in live_seqs(profile.next_seq).into_iter().take(20) {
                if let Some(post) = read_post(&self.rc, &key, seq, false).await {
                    items.push(FeedItem {
                        author_key: key_str.clone(),
                        author_name: name.clone(),
                        post,
                    });
                }
            }
        }
        items.sort_by(|a, b| b.post.ts_us.cmp(&a.post.ts_us));
        items
    }

    fn save_follows(&self, cfg: &ServeConfig, follows: &[String]) {
        if let Some(parent) = cfg.follows_file.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = std::fs::write(&cfg.follows_file, follows.join("\n"));
    }
}

fn load_follows(path: &std::path::Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .map(|t| t.lines().map(str::trim).filter(|l| !l.is_empty()).map(String::from).collect())
        .unwrap_or_default()
}

pub async fn serve(
    api: VeilidAPI,
    mut updates: Updates,
    params: RouteParams,
    cfg: ServeConfig,
    mut done: mpsc::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let rc = routing_context(&api, &params)?;
    let wall = Wall::open(&rc, &cfg.identity_file).await?;

    // Routes first: the profile we are about to publish has to carry them.
    let mut blobs = Vec::new();
    let mut ids = Vec::new();
    let first = try_again_loop("creating route", || async { create_route(&api, &params).await })
        .await?;
    blobs.push(data_encoding::BASE64.encode(&first.blob));
    ids.push(first.route_id);
    for _ in 1..cfg.pool.max(1) {
        match create_route(&api, &params).await {
            Ok(RouteBlob { route_id, blob }) => {
                blobs.push(data_encoding::BASE64.encode(&blob));
                ids.push(route_id);
            }
            Err(_) => break,
        }
    }

    let existing = read_profile(&rc, &wall.key, false).await.ok();
    let profile = Profile {
        name: existing.as_ref().map(|p| p.name.clone()).unwrap_or_default(),
        bio: existing.as_ref().map(|p| p.bio.clone()).unwrap_or_default(),
        next_seq: existing.as_ref().map(|p| p.next_seq).unwrap_or(0),
        routes: blobs.clone(),
        updated_us: now_us(),
    };

    let node = Arc::new(Node {
        api: api.clone(),
        rc: rc.clone(),
        wall,
        state: Mutex::new(State {
            profile,
            posts: Vec::new(),
            follows: load_follows(&cfg.follows_file),
            inbox: Vec::new(),
            routes: ids,
        }),
    });
    node.publish_profile().await?;

    println!("Your key (share this so others can follow you):\n");
    println!("  {}\n", node.key_string());

    let cfg = Arc::new(cfg);
    let handler = {
        let node = node.clone();
        let cfg = cfg.clone();
        Arc::new(move |req: Request| {
            let node = node.clone();
            let cfg = cfg.clone();
            async move { route_request(node, cfg, req).await }
        })
    };

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(web::serve(cfg.port, handler, async {
        let _ = stop_rx.await;
    }));

    // Veilid side: private messages arriving, routes dying.
    loop {
        tokio::select! {
            Some(Stamped { update, .. }) = updates.recv() => {
                match update {
                    VeilidUpdate::AppCall(call) => {
                        let reply = handle_social(&node, call.message()).await;
                        let _ = api.app_call_reply(call.id(), reply).await;
                    }
                    VeilidUpdate::RouteChange(change) => {
                        let dead: Vec<RouteId> = {
                            let st = node.state.lock().await;
                            change.dead_routes.iter()
                                .filter(|id| st.routes.contains(id))
                                .cloned().collect()
                        };
                        if dead.is_empty() { continue; }
                        println!("{} route(s) died; rebuilding and republishing.", dead.len());
                        let mut blobs = Vec::new();
                        let mut ids = Vec::new();
                        {
                            let st = node.state.lock().await;
                            for (id, blob) in st.routes.iter().zip(st.profile.routes.iter()) {
                                if !dead.contains(id) {
                                    ids.push(id.clone());
                                    blobs.push(blob.clone());
                                }
                            }
                        }
                        for id in &dead {
                            let _ = api.release_private_route(id.clone());
                        }
                        while ids.len() < cfg.pool.max(1) {
                            match create_route(&api, &params).await {
                                Ok(RouteBlob { route_id, blob }) => {
                                    blobs.push(data_encoding::BASE64.encode(&blob));
                                    ids.push(route_id);
                                }
                                Err(_) => break,
                            }
                        }
                        node.set_routes(blobs, ids).await;
                        if let Err(e) = node.publish_profile().await {
                            println!("could not republish the profile: {e}");
                        }
                    }
                    VeilidUpdate::Shutdown => break,
                    _ => {}
                }
            }
            _ = done.recv() => break,
        }
    }

    let _ = stop_tx.send(());
    let _ = server.await;
    Ok(())
}

/// Answer an inbound social message. Only direct messages for now; the kind is
/// carried in the JSON so more can be added without a wire change.
async fn handle_social(node: &Arc<Node>, message: &[u8]) -> Vec<u8> {
    let Some(Packet::Social { json }) = Packet::decode(message) else {
        return Packet::Social { json: b"{\"t\":\"unknown\"}".to_vec() }.encode(0);
    };
    let value: serde_json::Value = serde_json::from_slice(&json).unwrap_or(serde_json::Value::Null);
    if value.get("t").and_then(|v| v.as_str()) != Some("dm") {
        return Packet::Social { json: b"{\"t\":\"unknown\"}".to_vec() }.encode(0);
    }

    let dm = Dm {
        from_key: value.get("from").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        from_name: value.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        ts_us: value.get("ts").and_then(|v| v.as_u64()).unwrap_or_else(now_us),
        text: truncate_chars(value.get("text").and_then(|v| v.as_str()).unwrap_or(""), MAX_POST_CHARS),
    };
    println!("message from {}: {}", if dm.from_name.is_empty() { &dm.from_key } else { &dm.from_name }, dm.text);
    node.state.lock().await.inbox.push(dm);
    Packet::Social { json: b"{\"t\":\"ok\"}".to_vec() }.encode(0)
}

async fn route_request(node: Arc<Node>, cfg: Arc<ServeConfig>, req: Request) -> Response {
    match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/") => Response::asset("text/html; charset=utf-8", crate::APP_HTML),

        ("GET", "/api/me") => {
            let st = node.state.lock().await;
            Response::json(json!({
                "key": node.key_string(),
                "name": st.profile.name,
                "bio": st.profile.bio,
                "posts": st.profile.next_seq,
                "routes": st.profile.routes.len(),
                "follows": st.follows,
                "unread": st.inbox.len(),
            }))
        }

        ("POST", "/api/profile") => {
            {
                let mut st = node.state.lock().await;
                st.profile.name = truncate_chars(&req.field("name"), MAX_NAME_CHARS);
                st.profile.bio = truncate_chars(&req.field("bio"), MAX_BIO_CHARS);
            }
            match node.publish_profile().await {
                Ok(()) => Response::json(json!({ "ok": true })),
                Err(e) => Response::error(500, &e.to_string()),
            }
        }

        ("POST", "/api/post") => {
            let text = truncate_chars(&req.field("text"), MAX_POST_CHARS);
            if text.is_empty() {
                return Response::error(400, "a post needs some text");
            }
            let post = {
                let mut st = node.state.lock().await;
                let post = Post {
                    seq: st.profile.next_seq,
                    ts_us: now_us(),
                    text,
                    attachment: None,
                };
                st.profile.next_seq += 1;
                st.posts.insert(0, post.clone());
                post
            };
            // The post goes in first: a profile advertising a post that is not
            // there yet reads as a gap to anyone refreshing at that moment.
            if let Err(e) = node.wall.put_post(&post).await {
                return Response::error(500, &e.to_string());
            }
            match node.publish_profile().await {
                Ok(()) => Response::json(json!({ "ok": true, "seq": post.seq })),
                Err(e) => Response::error(500, &e.to_string()),
            }
        }

        ("GET", "/api/feed") => Response::json(json!({ "items": node.feed().await })),

        ("GET", "/api/inbox") => {
            let st = node.state.lock().await;
            Response::json(json!({ "messages": st.inbox }))
        }

        ("POST", "/api/follow") => {
            let key = req.field("key");
            if RecordKey::from_str(&key).is_err() {
                return Response::error(400, "that does not look like a node key");
            }
            if key == node.key_string() {
                return Response::error(400, "you are already yourself");
            }
            let follows = {
                let mut st = node.state.lock().await;
                if !st.follows.contains(&key) {
                    st.follows.push(key);
                }
                st.follows.clone()
            };
            node.save_follows(&cfg, &follows);
            Response::json(json!({ "follows": follows }))
        }

        ("POST", "/api/unfollow") => {
            let key = req.field("key");
            let follows = {
                let mut st = node.state.lock().await;
                st.follows.retain(|k| k != &key);
                st.follows.clone()
            };
            node.save_follows(&cfg, &follows);
            Response::json(json!({ "follows": follows }))
        }

        ("POST", "/api/dm") => {
            let to = req.field("to");
            let text = truncate_chars(&req.field("text"), MAX_POST_CHARS);
            if text.is_empty() {
                return Response::error(400, "a message needs some text");
            }
            match node.send_dm(&to, &text).await {
                Ok(()) => Response::json(json!({ "ok": true })),
                Err(e) => Response::error(400, &e.to_string()),
            }
        }

        _ => Response::error(404, "no such endpoint"),
    }
}
