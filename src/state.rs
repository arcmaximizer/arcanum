use crate::types::ProcessId;
use std::collections::HashMap;
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
        resp: tokio::sync::oneshot::Sender<Vec<u8>>,
    },
    SqlQuery {
        process: ProcessId,
        sql: String,
        resp: tokio::sync::oneshot::Sender<Vec<u8>>,
    },
}

#[derive(Clone)]
pub struct StateHandle {
    sender: mpsc::UnboundedSender<StateMsg>,
}

impl StateHandle {
    pub fn new(state: InMemoryKVState) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        tokio::spawn(run_state(receiver, state));
        Self { sender }
    }

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

    pub async fn sql_exec(&self, process: ProcessId, sql: String) -> Vec<u8> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(StateMsg::SqlExec {
                process,
                sql,
                resp: resp_tx,
            })
            .expect("State task has been killed");
        resp_rx.await.expect("State task has been killed")
    }

    pub async fn sql_query(&self, process: ProcessId, sql: String) -> Vec<u8> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.sender
            .send(StateMsg::SqlQuery {
                process,
                sql,
                resp: resp_tx,
            })
            .expect("State task has been killed");
        resp_rx.await.expect("State task has been killed")
    }
}

fn sqlite_value_to_json(value: rusqlite::types::Value) -> serde_json::Value {
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
                .map(|byte| {
                    serde_json::Value::Number(serde_json::Number::from(byte as i64))
                })
                .collect(),
        ),
    }
}

pub async fn run_state(
    mut rx: mpsc::UnboundedReceiver<StateMsg>,
    mut state: InMemoryKVState,
) {
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
            StateMsg::Get { process, key, resp } => {
                let value = state.get(&process, &key);
                let _ = resp.send(value);
            }
            StateMsg::SqlExec { process, sql, resp } => {
                let conn = sqlite_dbs
                    .entry(process)
                    .or_insert_with(|| rusqlite::Connection::open_in_memory().unwrap());
                let result = conn.execute(&sql, []);
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
            StateMsg::SqlQuery { process, sql, resp } => {
                let conn = sqlite_dbs
                    .entry(process)
                    .or_insert_with(|| rusqlite::Connection::open_in_memory().unwrap());
                let response = match conn.prepare(&sql) {
                    Ok(mut stmt) => {
                        let col_count = stmt.column_count();
                        let col_names: Vec<String> = (0..col_count)
                            .map(|i| stmt.column_name(i).unwrap().to_string())
                            .collect();
                        match stmt.query_map([], |row| {
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

pub trait KVState {
    fn set(&mut self, process: &ProcessId, key: &str, value: String);
    fn get(&self, process: &ProcessId, key: &str) -> Option<String>;
}

pub struct InMemoryKVState {
    kv: HashMap<ProcessId, HashMap<String, String>>,
}

impl InMemoryKVState {
    pub fn new() -> Self {
        Self { kv: HashMap::new() }
    }
}

impl KVState for InMemoryKVState {
    fn set(&mut self, process: &ProcessId, key: &str, value: String) {
        self.kv
            .entry(process.clone())
            .or_insert_with(HashMap::new)
            .insert(key.into(), value);
    }
    fn get(&self, process: &ProcessId, key: &str) -> Option<String> {
        self.kv.get(process).and_then(|map| map.get(key).cloned())
    }
}
