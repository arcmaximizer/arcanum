use crate::types::ProcessId;
use std::collections::HashMap;
use tokio::sync::mpsc;

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
}

pub async fn run_state(mut rx: mpsc::UnboundedReceiver<StateMsg>, mut state: InMemoryKVState) {
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
