use crate::state::{StateMsg, msgpack_params_to_sqlite, sqlite_value_to_json};
use crate::store::{HashKey, PackageStore, detect_tar, package_name_from_toml};
use crate::types::ProcessId;
use anyhow::Result;
use bytes::Bytes;
use flate2::read::GzDecoder;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::Read;
use tar::Archive;
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// In-memory package store (no longer used — kept for reference)
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct InMemoryPackageStore {
    names: HashMap<String, HashKey>,
    packages: HashMap<HashKey, Bytes>,
    cache: HashMap<(HashKey, String), Bytes>,
}

impl InMemoryPackageStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl PackageStore for InMemoryPackageStore {
    fn resolve_name(&self, name: &str) -> Option<HashKey> {
        self.names.get(name).copied()
    }
    fn set_name(&mut self, name: &str, key: HashKey) {
        self.names.insert(name.to_string(), key);
    }
    fn get_package(&self, key: &HashKey) -> Option<Bytes> {
        self.packages.get(key).cloned()
    }
    fn add_package(&mut self, value: Bytes) -> Result<HashKey> {
        let key: HashKey = Sha256::digest(&value).into();
        let format = detect_tar(&value);

        if format.is_none() {
            anyhow::bail!("Invalid tarball")
        }
        if self.packages.contains_key(&key) {
            anyhow::bail!("Package already exists")
        }

        self.packages.insert(key, value.clone());

        let format = format.unwrap();
        let reader: Box<dyn Read> = if format == ".tar.gz" {
            Box::new(GzDecoder::new(&value[..]))
        } else {
            Box::new(std::io::Cursor::new(&value[..]))
        };
        let mut archive = Archive::new(reader);
        for entry in archive.entries().map_err(|e| anyhow::anyhow!("{}", e))? {
            let mut entry = entry.map_err(|e| anyhow::anyhow!("{}", e))?;
            let path = entry.path().map_err(|e| anyhow::anyhow!("{}", e))?;
            let path_str = path.to_string_lossy().into_owned();
            let mut contents = Vec::new();
            entry
                .read_to_end(&mut contents)
                .map_err(|e| anyhow::anyhow!("{}", e))?;
            self.cache.insert((key, path_str), contents.into());
        }

        // Auto-register name from arcanum.toml
        if let Some(name) = self
            .cache
            .get(&(key, "arcanum.toml".into()))
            .and_then(|d| package_name_from_toml(d))
        {
            self.names.insert(name, key);
        }

        Ok(key)
    }
    fn get_asset(&self, key: &HashKey, asset: &str) -> Option<Bytes> {
        self.cache.get(&(*key, asset.to_string())).cloned()
    }
    fn list_names(&self) -> Vec<String> {
        self.names.keys().cloned().collect()
    }
}

// ---------------------------------------------------------------------------
// In-memory KV state (no longer used — kept for reference)
// ---------------------------------------------------------------------------

pub trait KVState {
    fn set(&mut self, process: &ProcessId, key: &str, value: String);
    fn get(&self, process: &ProcessId, key: &str) -> Option<String>;
}

#[derive(Default)]
pub struct InMemoryKVState {
    kv: HashMap<ProcessId, HashMap<String, String>>,
}

impl InMemoryKVState {
    pub fn new() -> Self {
        Self::default()
    }
}

impl KVState for InMemoryKVState {
    fn set(&mut self, process: &ProcessId, key: &str, value: String) {
        self.kv
            .entry(process.clone())
            .or_default()
            .insert(key.into(), value);
    }
    fn get(&self, process: &ProcessId, key: &str) -> Option<String> {
        self.kv.get(process).and_then(|map| map.get(key).cloned())
    }
}

pub async fn run_state(mut rx: mpsc::UnboundedReceiver<StateMsg>, mut state: InMemoryKVState) {
    let mut sqlite_dbs: HashMap<ProcessId, rusqlite::Connection> = HashMap::new();

    while let Some(msg) = rx.recv().await {
        match msg {
            StateMsg::Set {
                process,
                key,
                value,
                resp,
            } => {
                state.set(&process, &key, value);
                let _ = resp.send(());
            }
            StateMsg::Get {
                process, key, resp, ..
            } => {
                let value = state.get(&process, &key);
                let _ = resp.send(value);
            }
            StateMsg::SqlExec {
                process,
                sql,
                params,
                resp,
            } => {
                let conn = sqlite_dbs
                    .entry(process)
                    .or_insert_with(|| rusqlite::Connection::open_in_memory().unwrap());
                let sqlite_params = msgpack_params_to_sqlite(&params);
                let result = conn.execute(&sql, rusqlite::params_from_iter(sqlite_params));
                let response = match result {
                    Ok(affected) => {
                        let map = serde_json::json!({"affected": affected});
                        rmp_serde::to_vec(&map).unwrap_or_default()
                    }
                    Err(e) => {
                        let map = serde_json::json!({"error": e.to_string()});
                        rmp_serde::to_vec(&map).unwrap_or_default()
                    }
                };
                let _ = resp.send(response);
            }
            StateMsg::SqlQuery {
                process,
                sql,
                params,
                resp,
            } => {
                let conn = sqlite_dbs
                    .entry(process)
                    .or_insert_with(|| rusqlite::Connection::open_in_memory().unwrap());
                let sqlite_params = msgpack_params_to_sqlite(&params);
                let response = match conn.prepare(&sql) {
                    Ok(mut stmt) => {
                        let col_count = stmt.column_count();
                        let col_names: Vec<String> = (0..col_count)
                            .map(|i| stmt.column_name(i).unwrap().to_string())
                            .collect();
                        match stmt.query_map(rusqlite::params_from_iter(sqlite_params), |row| {
                            let mut map = serde_json::Map::new();
                            for (i, name) in col_names.iter().enumerate() {
                                let val = row
                                    .get::<_, rusqlite::types::Value>(i)
                                    .unwrap_or(rusqlite::types::Value::Null);
                                map.insert(name.clone(), sqlite_value_to_json(val));
                            }
                            Ok(serde_json::Value::Object(map))
                        }) {
                            Ok(mapped_rows) => {
                                let rows: Vec<serde_json::Value> =
                                    mapped_rows.filter_map(|r| r.ok()).collect();
                                let result =
                                    serde_json::json!({"rows": rows, "columns": col_names});
                                rmp_serde::to_vec(&result).unwrap_or_default()
                            }
                            Err(e) => {
                                let map = serde_json::json!({"error": e.to_string()});
                                rmp_serde::to_vec(&map).unwrap_or_default()
                            }
                        }
                    }
                    Err(e) => {
                        let map = serde_json::json!({"error": e.to_string()});
                        rmp_serde::to_vec(&map).unwrap_or_default()
                    }
                };
                let _ = resp.send(response);
            }
        }
    }
}
