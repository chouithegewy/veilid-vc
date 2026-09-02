//! A very small HTTP/1.1 server, for one browser on one machine.
//!
//! This exists to hand a local page some JSON, so it implements only what that
//! needs: GET and POST, a fixed set of paths, no keep-alive, no chunked
//! encoding, no TLS. Pulling in a web framework to do that would have cost more
//! than writing it.
//!
//! It binds loopback only. The node holds your identity keypair and can post as
//! you, so the API is authority over your account with no authentication in
//! front of it -- which is safe exactly as long as nothing off the machine can
//! reach it, and not one moment longer.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Bodies are small JSON; anything larger is a mistake or an attack.
const MAX_BODY: usize = 1 << 20;
/// Headers likewise.
const MAX_HEADERS: usize = 16 * 1024;

pub struct Request {
    pub method: String,
    pub path: String,
    /// Parsed but unread so far: every current endpoint takes its arguments in
    /// a JSON body. Kept because paging a feed will want it and the parsing is
    /// the fiddly part.
    #[allow(dead_code)]
    pub query: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl Request {
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or(serde_json::Value::Null)
    }

    /// A string field from the JSON body, trimmed. Missing reads as empty,
    /// which every caller wants to treat the same as blank anyway.
    pub fn field(&self, name: &str) -> String {
        self.json().get(name).and_then(|v| v.as_str()).unwrap_or("").trim().to_string()
    }
}

pub struct Response {
    pub status: u16,
    pub content_type: &'static str,
    pub body: Vec<u8>,
}

impl Response {
    pub fn json(value: serde_json::Value) -> Response {
        Response {
            status: 200,
            content_type: "application/json; charset=utf-8",
            body: serde_json::to_vec(&value).unwrap_or_else(|_| b"{}".to_vec()),
        }
    }

    pub fn error(status: u16, message: &str) -> Response {
        Response {
            status,
            content_type: "application/json; charset=utf-8",
            body: serde_json::to_vec(&serde_json::json!({ "error": message }))
                .unwrap_or_else(|_| b"{}".to_vec()),
        }
    }

    pub fn asset(content_type: &'static str, body: &str) -> Response {
        Response { status: 200, content_type, body: body.as_bytes().to_vec() }
    }
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        413 => "Payload Too Large",
        500 => "Internal Server Error",
        _ => "OK",
    }
}

/// Percent-decoding, for query strings. Invalid escapes are left as written
/// rather than dropped, so a stray `%` in a search box is not silently eaten.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                match u8::from_str_radix(&s[i + 1..i + 3], 16) {
                    Ok(b) => {
                        out.push(b);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn parse_query(raw: &str) -> HashMap<String, String> {
    raw.split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| match pair.split_once('=') {
            Some((k, v)) => (percent_decode(k), percent_decode(v)),
            None => (percent_decode(pair), String::new()),
        })
        .collect()
}

async fn read_request(stream: &mut TcpStream) -> Option<Request> {
    let mut buf = Vec::with_capacity(2048);
    let mut head_end = None;
    // Read until the blank line that ends the headers.
    while head_end.is_none() {
        if buf.len() > MAX_HEADERS {
            return None;
        }
        let mut chunk = [0u8; 1024];
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
        head_end = buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4);
    }
    let head_end = head_end?;
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();

    let mut lines = head.lines();
    let mut request_line = lines.next()?.split_whitespace();
    let method = request_line.next()?.to_string();
    let target = request_line.next()?.to_string();

    let content_length = lines
        .find_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.eq_ignore_ascii_case("content-length").then(|| v.trim().parse::<usize>().ok())?
        })
        .unwrap_or(0);
    if content_length > MAX_BODY {
        return None;
    }

    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_string(), parse_query(q)),
        None => (target, HashMap::new()),
    };

    let mut body = buf[head_end..].to_vec();
    while body.len() < content_length {
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(content_length);

    Some(Request { method, path, query, body })
}

async fn write_response(stream: &mut TcpStream, res: Response) {
    let head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\
         Cache-Control: no-store\r\n\
         X-Content-Type-Options: nosniff\r\n\r\n",
        res.status,
        reason(res.status),
        res.content_type,
        res.body.len()
    );
    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(&res.body).await;
    let _ = stream.flush().await;
}

/// Serve until the future returned by `shutdown` completes.
pub async fn serve<S, F, Fut>(
    port: u16,
    handler: Arc<F>,
    shutdown: S,
) -> std::io::Result<()>
where
    S: std::future::Future<Output = ()>,
    F: Fn(Request) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Response> + Send + 'static,
{
    // Loopback only, deliberately. See the module note.
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    let actual = listener.local_addr()?;
    println!("Open http://{actual}/ in a browser.\n");

    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((mut stream, _)) = accepted else { continue };
                let handler = handler.clone();
                tokio::spawn(async move {
                    let res = match read_request(&mut stream).await {
                        Some(req) => handler(req).await,
                        None => Response::error(400, "malformed request"),
                    };
                    write_response(&mut stream, res).await;
                });
            }
            _ = &mut shutdown => return Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_strings_decode_escapes_and_plus() {
        let q = parse_query("key=VLD0%3Aabc&name=ada+lovelace&bare");
        assert_eq!(q.get("key").unwrap(), "VLD0:abc");
        assert_eq!(q.get("name").unwrap(), "ada lovelace");
        assert_eq!(q.get("bare").unwrap(), "");
    }

    #[test]
    fn a_stray_percent_is_kept_rather_than_swallowed() {
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("%zz"), "%zz");
    }

    #[test]
    fn percent_decoding_handles_multibyte_utf8() {
        assert_eq!(percent_decode("caf%C3%A9"), "café");
    }

    #[test]
    fn a_missing_json_field_reads_as_empty() {
        let req = Request {
            method: "POST".into(),
            path: "/api/post".into(),
            query: HashMap::new(),
            body: br#"{"text":"  hi  "}"#.to_vec(),
        };
        assert_eq!(req.field("text"), "hi", "fields are trimmed");
        assert_eq!(req.field("absent"), "");
    }

    #[test]
    fn a_non_json_body_does_not_panic() {
        let req = Request {
            method: "POST".into(),
            path: "/x".into(),
            query: HashMap::new(),
            body: b"not json at all".to_vec(),
        };
        assert_eq!(req.field("anything"), "");
    }
}
