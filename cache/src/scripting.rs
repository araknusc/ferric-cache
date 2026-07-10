//! Lua scripting (EVAL / EVALSHA) via `mlua`.
//!
//! First-pass scope:
//! - EVAL <script> <numkeys> [key...] [arg...] — compiles, exposes a
//!   `redis.call(cmd, ...)` bridge, runs, converts return to RESP.
//! - EVALSHA <sha1> <numkeys> [key...] [arg...] — looks up cached source.
//! - `redis.call` whitelists a small set of read+write commands.
//!
//! Write handling: each write performed by `redis.call` is (a) checked
//! against the caller's ACL *before* it runs — so a script cannot perform
//! writes the user's ACL would otherwise deny — and (b) recorded as a
//! `Command`. `run`/`run_sha` return the recorded writes so the caller can
//! push them through the same WAL + replication chokepoint as ordinary
//! writes. This closes the earlier privilege-escalation and
//! non-durability/non-replication gaps.
//!
//! No timeout enforcement, no `redis.pcall` (only `redis.call`), no
//! script flushing — these can be added incrementally. Cluster routing of
//! script keys is still local-only (documented limitation).

use std::collections::HashMap;
use std::sync::Arc;
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use sha1::{Sha1, Digest};

use crate::protocol::{Command, Response};
use crate::storage::CacheStorage;

/// Per-command authorization check, injected by the server from the
/// connection's authenticated user + ACL. `(command_name, first_key)` →
/// allowed?. When auth is disabled the server passes an allow-all checker.
pub type AclCheck = Arc<dyn Fn(&str, Option<&[u8]>) -> bool + Send + Sync>;

/// An allow-all checker, for contexts without authentication (and tests).
pub fn allow_all_acl() -> AclCheck {
    Arc::new(|_cmd: &str, _key: Option<&[u8]>| true)
}

pub struct LuaEngine {
    /// SHA1 hex (40 chars) -> script source. Populated by EVAL, consumed
    /// by EVALSHA.
    scripts: RwLock<HashMap<String, String>>,
}

impl LuaEngine {
    pub fn new() -> Self {
        Self { scripts: RwLock::new(HashMap::new()) }
    }
}

impl Default for LuaEngine {
    fn default() -> Self { Self::new() }
}

pub fn sha1_hex(data: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(data);
    let bytes = hasher.finalize();
    let mut s = String::with_capacity(40);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

impl LuaEngine {
    /// Run a script. Stores the source under its SHA1 so a later EVALSHA can
    /// re-use it without sending the script body again. Returns the reply plus
    /// the ordered list of write `Command`s the script performed, so the caller
    /// can WAL-log and replicate them.
    pub fn run(&self, script: &str, keys: Vec<Bytes>, argv: Vec<Bytes>,
               storage: &Arc<CacheStorage>, acl: AclCheck) -> (Response, Vec<Command>)
    {
        let sha = sha1_hex(script.as_bytes());
        self.scripts.write().insert(sha, script.to_string());
        run_script(script, keys, argv, storage, acl)
    }

    /// Run a script previously stored by `run` (EVAL).
    pub fn run_sha(&self, sha1_hex: &str, keys: Vec<Bytes>, argv: Vec<Bytes>,
                   storage: &Arc<CacheStorage>, acl: AclCheck) -> (Response, Vec<Command>)
    {
        let source = match self.scripts.read().get(&sha1_hex.to_ascii_lowercase()) {
            Some(s) => s.clone(),
            None => return (
                Response::Error("NOSCRIPT No matching script. Please use EVAL.".to_string()),
                Vec::new(),
            ),
        };
        run_script(&source, keys, argv, storage, acl)
    }
}

fn run_script(source: &str, keys: Vec<Bytes>, argv: Vec<Bytes>,
              storage: &Arc<CacheStorage>, acl: AclCheck) -> (Response, Vec<Command>)
{
    use mlua::{Lua, Value as LuaValue, Variadic};

    // Collects the write commands performed via redis.call, in order, so the
    // server can route them through the WAL/replication chokepoint.
    let writes: Arc<Mutex<Vec<Command>>> = Arc::new(Mutex::new(Vec::new()));

    let lua = match Lua::new_with(mlua::StdLib::ALL_SAFE, mlua::LuaOptions::new()) {
        Ok(l) => l,
        Err(e) => return (Response::Error(format!("ERR Lua init failed: {}", e)), Vec::new()),
    };

    let res = (|| -> mlua::Result<Response> {
        let keys_tbl = lua.create_table()?;
        for (i, k) in keys.iter().enumerate() {
            keys_tbl.set(i + 1, lua.create_string(k.as_ref())?)?;
        }
        let argv_tbl = lua.create_table()?;
        for (i, a) in argv.iter().enumerate() {
            argv_tbl.set(i + 1, lua.create_string(a.as_ref())?)?;
        }
        lua.globals().set("KEYS", keys_tbl)?;
        lua.globals().set("ARGV", argv_tbl)?;

        // redis.call(cmd, ...args) - sync bridge into storage.
        let storage_for_call = Arc::clone(storage);
        let acl_for_call = Arc::clone(&acl);
        let writes_for_call = Arc::clone(&writes);
        let call_fn = lua.create_function(move |lua, args: Variadic<LuaValue>| {
            let cmd = lua_string_arg(&args, 0)
                .ok_or_else(|| mlua::Error::external("redis.call needs a command name"))?;
            let rest: Vec<Vec<u8>> = (1..args.len())
                .filter_map(|i| lua_bytes_arg(&args, i))
                .collect();
            let resp = redis_call_bridge(&cmd, &rest, &storage_for_call, &acl_for_call, &writes_for_call);
            response_to_lua(lua, resp)
        })?;
        let redis_tbl = lua.create_table()?;
        redis_tbl.set("call", call_fn)?;
        lua.globals().set("redis", redis_tbl)?;

        let chunk = lua.load(source);
        let value: LuaValue = chunk.eval()?;
        Ok(lua_to_response(value))
    })();

    let collected = std::mem::take(&mut *writes.lock());
    let response = match res {
        Ok(r) => r,
        Err(e) => Response::Error(format!("ERR script execution failed: {}", e)),
    };
    (response, collected)
}

fn lua_to_response(val: mlua::Value) -> Response {
    use mlua::Value as V;
    match val {
        V::Nil => Response::Null,
        V::Boolean(true) => Response::Ok,
        V::Boolean(false) => Response::Null,
        V::Integer(n) => Response::Integer(n as i64),
        V::Number(f) => Response::Integer(f as i64),
        V::String(s) => Response::Value(Bytes::copy_from_slice(s.as_bytes().as_ref())),
        V::Table(t) => {
            let mut out = Vec::new();
            let mut i = 1i64;
            while let Ok(v) = t.get::<mlua::Value>(i) {
                if matches!(v, V::Nil) { break; }
                match v {
                    V::String(s) => out.push(Bytes::copy_from_slice(s.as_bytes().as_ref())),
                    V::Integer(n) => out.push(Bytes::from(n.to_string())),
                    V::Number(f) => out.push(Bytes::from(f.to_string())),
                    _ => out.push(Bytes::new()),
                }
                i += 1;
            }
            Response::Array(out)
        }
        _ => Response::Null,
    }
}

fn response_to_lua(lua: &mlua::Lua, resp: Response) -> mlua::Result<mlua::Value> {
    use mlua::Value as V;
    match resp {
        Response::Ok => Ok(V::Boolean(true)),
        Response::NotFound | Response::Null => Ok(V::Boolean(false)),
        Response::Integer(n) => Ok(V::Integer(n as mlua::Integer)),
        Response::Value(b) => Ok(V::String(lua.create_string(b.as_ref())?)),
        Response::Array(items) => {
            let t = lua.create_table()?;
            for (i, b) in items.into_iter().enumerate() {
                t.set(i + 1, lua.create_string(b.as_ref())?)?;
            }
            Ok(V::Table(t))
        }
        Response::Error(msg) => Err(mlua::Error::external(msg)),
        Response::Raw(_) => Ok(V::Boolean(true)),
    }
}

fn lua_string_arg(v: &mlua::Variadic<mlua::Value>, idx: usize) -> Option<String> {
    match v.get(idx) {
        Some(mlua::Value::String(s)) => Some(s.to_str().ok()?.to_string()),
        _ => None,
    }
}

fn lua_bytes_arg(v: &mlua::Variadic<mlua::Value>, idx: usize) -> Option<Vec<u8>> {
    match v.get(idx) {
        Some(mlua::Value::String(s)) => Some(s.as_bytes().to_vec()),
        Some(mlua::Value::Integer(n)) => Some(n.to_string().into_bytes()),
        Some(mlua::Value::Number(f)) => Some(f.to_string().into_bytes()),
        _ => None,
    }
}

fn redis_call_bridge(
    cmd: &str,
    args: &[Vec<u8>],
    storage: &Arc<CacheStorage>,
    acl: &AclCheck,
    writes: &Arc<Mutex<Vec<Command>>>,
) -> Response {
    let upper = cmd.to_ascii_uppercase();

    // Enforce the caller's ACL for the command + first key *before* executing.
    // A script therefore cannot escalate past the user's permissions.
    let first_key = args.first().map(|a| a.as_slice());
    if !acl(&upper, first_key) {
        return Response::Error(format!(
            "NOPERM this user has no permissions to run the '{}' command",
            upper.to_lowercase()
        ));
    }

    // Record a write so the server can WAL-log + replicate it.
    let record = |writes: &Arc<Mutex<Vec<Command>>>, c: Command| writes.lock().push(c);

    match upper.as_str() {
        "GET" => {
            let Some(k) = args.first() else { return Response::Error("ERR GET key".into()); };
            match storage.get(k) {
                Some(v) => Response::Value(v),
                None => Response::Null,
            }
        }
        "SET" => {
            if args.len() < 2 { return Response::Error("ERR SET key value".into()); }
            let key = Bytes::copy_from_slice(&args[0]);
            let value = Bytes::copy_from_slice(&args[1]);
            storage.set(key.clone(), value.clone(), None);
            record(writes, Command::Set { key, value, ttl_secs: None });
            Response::Ok
        }
        "DEL" | "DELETE" => {
            let Some(k) = args.first() else { return Response::Error("ERR DEL key".into()); };
            let deleted = storage.delete(k);
            if deleted {
                record(writes, Command::Delete { key: Bytes::copy_from_slice(k) });
            }
            Response::Integer(if deleted { 1 } else { 0 })
        }
        "EXISTS" => {
            let bs: Vec<Bytes> = args.iter().map(|a| Bytes::copy_from_slice(a)).collect();
            Response::Integer(storage.exists(&bs) as i64)
        }
        "INCR" => match args.first() {
            Some(k) => match storage.incr(k) {
                Ok(n) => {
                    record(writes, Command::Incr { key: Bytes::copy_from_slice(k) });
                    Response::Integer(n)
                }
                Err(e) => Response::Error(e),
            },
            None => Response::Error("ERR INCR key".into()),
        },
        "DECR" => match args.first() {
            Some(k) => match storage.decr(k) {
                Ok(n) => {
                    record(writes, Command::Decr { key: Bytes::copy_from_slice(k) });
                    Response::Integer(n)
                }
                Err(e) => Response::Error(e),
            },
            None => Response::Error("ERR DECR key".into()),
        },
        "HGET" => {
            if args.len() < 2 { return Response::Error("ERR HGET key field".into()); }
            match storage.hget(&args[0], &args[1]) {
                Some(v) => Response::Value(v),
                None => Response::Null,
            }
        }
        "HSET" => {
            if args.len() < 3 { return Response::Error("ERR HSET key field value".into()); }
            let key = Bytes::copy_from_slice(&args[0]);
            let field = Bytes::copy_from_slice(&args[1]);
            let value = Bytes::copy_from_slice(&args[2]);
            match storage.hset(key.clone(), field.clone(), value.clone()) {
                Ok(_) => {
                    record(writes, Command::HSet { key, field, value });
                    Response::Integer(1)
                }
                Err(e) => Response::Error(e),
            }
        }
        "LPUSH" | "RPUSH" => {
            if args.len() < 2 { return Response::Error(format!("ERR {} key value", upper)); }
            let key = Bytes::copy_from_slice(&args[0]);
            let vals: Vec<Bytes> = args[1..].iter().map(|a| Bytes::copy_from_slice(a)).collect();
            let r = if upper == "LPUSH" {
                storage.lpush(key.clone(), vals.clone())
            } else {
                storage.rpush(key.clone(), vals.clone())
            };
            match r {
                Ok(n) => {
                    let c = if upper == "LPUSH" {
                        Command::LPush { key, values: vals }
                    } else {
                        Command::RPush { key, values: vals }
                    };
                    record(writes, c);
                    Response::Integer(n as i64)
                }
                Err(e) => Response::Error(e),
            }
        }
        "SADD" => {
            if args.len() < 2 { return Response::Error("ERR SADD key member".into()); }
            let key = Bytes::copy_from_slice(&args[0]);
            let members: Vec<Bytes> = args[1..].iter().map(|a| Bytes::copy_from_slice(a)).collect();
            match storage.sadd(key.clone(), members.clone()) {
                Ok(n) => {
                    record(writes, Command::SAdd { key, members });
                    Response::Integer(n as i64)
                }
                Err(e) => Response::Error(e),
            }
        }
        _ => Response::Error(format!(
            "ERR redis.call: '{}' not implemented in scripting bridge", cmd
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha1_hex_known_vector() {
        // SHA1("") = "da39a3ee5e6b4b0d3255bfef95601890afd80709"
        assert_eq!(sha1_hex(b""), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
    }

    #[test]
    fn lua_returns_integer() {
        let engine = LuaEngine::new();
        let storage = Arc::new(CacheStorage::new());
        let (r, _) = engine.run("return 42", vec![], vec![], &storage, allow_all_acl());
        assert!(matches!(r, Response::Integer(42)));
    }

    #[test]
    fn lua_keys_and_argv_visible() {
        let engine = LuaEngine::new();
        let storage = Arc::new(CacheStorage::new());
        let (r, _) = engine.run(
            "return KEYS[1] .. '/' .. ARGV[1]",
            vec![Bytes::from_static(b"foo")],
            vec![Bytes::from_static(b"bar")],
            &storage,
            allow_all_acl(),
        );
        match r {
            Response::Value(b) => assert_eq!(b.as_ref(), b"foo/bar"),
            other => panic!("expected Value, got {:?}", other),
        }
    }

    #[test]
    fn redis_call_set_then_get_lands_on_storage_and_records_write() {
        let engine = LuaEngine::new();
        let storage = Arc::new(CacheStorage::new());
        let (_, writes) = engine.run(
            "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])",
            vec![Bytes::from_static(b"k")],
            vec![Bytes::from_static(b"v")],
            &storage,
            allow_all_acl(),
        );
        assert_eq!(storage.get(b"k").as_deref(), Some(&b"v"[..]));
        // The SET must be captured for WAL/replication; the GET must not.
        assert_eq!(writes.len(), 1);
        assert!(matches!(&writes[0], Command::Set { .. }));
    }

    #[test]
    fn acl_denied_write_is_blocked_in_script() {
        let engine = LuaEngine::new();
        let storage = Arc::new(CacheStorage::new());
        // Deny SET, allow everything else.
        let acl: AclCheck = Arc::new(|cmd: &str, _k: Option<&[u8]>| cmd != "SET");
        let (r, writes) = engine.run(
            "return redis.call('SET', KEYS[1], ARGV[1])",
            vec![Bytes::from_static(b"k")],
            vec![Bytes::from_static(b"v")],
            &storage,
            acl,
        );
        match r {
            Response::Error(msg) => assert!(msg.contains("NOPERM"), "got {}", msg),
            other => panic!("expected NOPERM error, got {:?}", other),
        }
        assert!(storage.get(b"k").is_none(), "denied write must not land");
        assert!(writes.is_empty(), "denied write must not be recorded");
    }

    #[test]
    fn run_sha_uses_cached_source() {
        let engine = LuaEngine::new();
        let storage = Arc::new(CacheStorage::new());
        let script = "return ARGV[1]";
        let sha = sha1_hex(script.as_bytes());

        // Before EVAL -> NOSCRIPT.
        let (r, _) = engine.run_sha(&sha, vec![], vec![Bytes::from_static(b"x")], &storage, allow_all_acl());
        match r {
            Response::Error(msg) => assert!(msg.starts_with("NOSCRIPT"), "got {}", msg),
            other => panic!("expected NOSCRIPT, got {:?}", other),
        }

        engine.run(script, vec![], vec![Bytes::from_static(b"x")], &storage, allow_all_acl());
        let (r, _) = engine.run_sha(&sha, vec![], vec![Bytes::from_static(b"y")], &storage, allow_all_acl());
        match r {
            Response::Value(b) => assert_eq!(b.as_ref(), b"y"),
            other => panic!("expected Value, got {:?}", other),
        }
    }
}
