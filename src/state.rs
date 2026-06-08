use crate::types::ProcessId;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub enum StateMsg {
    Set {
        process: ProcessId,
        key: String,
        value: String,
        resp: tokio::sync::oneshot::Sender<()>,
    },
    Get {
        process: ProcessId,
        key: String,
        resp: tokio::sync::oneshot::Sender<Option<String>>,
    },
    SqlExec {
        process: ProcessId,
        sql: String,
        params: Vec<u8>,
        resp: tokio::sync::oneshot::Sender<Vec<u8>>,
    },
    SqlQuery {
        process: ProcessId,
        sql: String,
        params: Vec<u8>,
        resp: tokio::sync::oneshot::Sender<Vec<u8>>,
    },
}

#[derive(Debug, Clone)]
pub struct StateHandle {
    sender: mpsc::UnboundedSender<StateMsg>,
}

impl StateHandle {
    pub fn from_sender(sender: mpsc::UnboundedSender<StateMsg>) -> Self {
        Self { sender }
    }

    pub async fn set(&self, process: ProcessId, key: String, value: String) {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(StateMsg::Set {
                process,
                key,
                value,
                resp: resp_tx,
            })
            .expect("State task has been killed");
        resp_rx.await.expect("State task has been killed");
    }

    pub async fn get(&self, process: ProcessId, key: String) -> Option<String> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(StateMsg::Get {
                process,
                key,
                resp: resp_tx,
            })
            .expect("State task has been killed");
        resp_rx.await.expect("State task has been killed")
    }

    pub async fn sql_exec(&self, process: ProcessId, sql: String, params: Vec<u8>) -> Vec<u8> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(StateMsg::SqlExec {
                process,
                sql,
                params,
                resp: resp_tx,
            })
            .expect("State task has been killed");
        resp_rx.await.expect("State task has been killed")
    }

    pub async fn sql_query(&self, process: ProcessId, sql: String, params: Vec<u8>) -> Vec<u8> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(StateMsg::SqlQuery {
                process,
                sql,
                params,
                resp: resp_tx,
            })
            .expect("State task has been killed");
        resp_rx.await.expect("State task has been killed")
    }
}

pub(crate) fn sqlite_value_to_json(value: rusqlite::types::Value) -> serde_json::Value {
    match value {
        rusqlite::types::Value::Null => serde_json::Value::Null,
        rusqlite::types::Value::Integer(i) => {
            serde_json::Value::Number(serde_json::Number::from(i))
        }
        rusqlite::types::Value::Real(f) => serde_json::Number::from_f64(f)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        rusqlite::types::Value::Text(s) => serde_json::Value::String(s),
        rusqlite::types::Value::Blob(b) => serde_json::Value::Array(
            b.into_iter()
                .map(|byte| serde_json::Value::Number(serde_json::Number::from(byte as i64)))
                .collect(),
        ),
    }
}

pub(crate) fn msgpack_params_to_sqlite(params: &[u8]) -> Vec<rusqlite::types::Value> {
    if params.is_empty() {
        return Vec::new();
    }
    match rmp_serde::from_slice::<Vec<serde_json::Value>>(params) {
        Ok(json_params) => json_params.into_iter().map(json_to_sqlite_value).collect(),
        Err(_) => Vec::new(),
    }
}

pub(crate) fn json_to_sqlite_value(value: serde_json::Value) -> rusqlite::types::Value {
    match value {
        serde_json::Value::Null => rusqlite::types::Value::Null,
        serde_json::Value::Bool(b) => rusqlite::types::Value::Integer(b as i64),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                rusqlite::types::Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                rusqlite::types::Value::Real(f)
            } else {
                rusqlite::types::Value::Null
            }
        }
        serde_json::Value::String(s) => rusqlite::types::Value::Text(s),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            let bytes = rmp_serde::to_vec(&value).unwrap_or_default();
            rusqlite::types::Value::Blob(bytes)
        }
    }
}

fn db_path_for_process(state_dir: &Path, process: &ProcessId) -> PathBuf {
    state_dir
        .join(&process.namespace)
        .join(&process.app)
        .join(format!("{}.db", process.proc))
}

/// Spawn a per-process state actor with its own KV store and SQLite database.
/// The SQLite database is persisted at `{state_dir}/{namespace}/{app}/{proc}.db`.
pub fn spawn_per_process_state(process: &ProcessId, state_dir: &Path) -> StateHandle {
    let db_path = db_path_for_process(state_dir, process);
    if let Some(parent) = db_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let (sender, receiver) = mpsc::unbounded_channel();
    tokio::spawn(run_per_process_state(receiver, db_path));
    StateHandle { sender }
}

async fn run_per_process_state(mut rx: mpsc::UnboundedReceiver<StateMsg>, db_path: PathBuf) {
    let mut kv: HashMap<String, String> = HashMap::new();
    let conn = rusqlite::Connection::open(&db_path)
        .unwrap_or_else(|e| panic!("failed to open state database {}: {}", db_path.display(), e));
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS kv (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );",
    )
    .unwrap();

    while let Some(msg) = rx.recv().await {
        match msg {
            StateMsg::Set {
                key, value, resp, ..
            } => {
                let _ = conn.execute(
                    "INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)",
                    rusqlite::params![&key, &value],
                );
                kv.insert(key, value);
                let _ = resp.send(());
            }
            StateMsg::Get { key, resp, .. } => {
                let value = conn
                    .query_row(
                        "SELECT value FROM kv WHERE key = ?1",
                        rusqlite::params![&key],
                        |row| row.get::<_, String>(0),
                    )
                    .ok();
                let _ = resp.send(value);
            }
            StateMsg::SqlExec {
                sql, params, resp, ..
            } => {
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
                sql, params, resp, ..
            } => {
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


