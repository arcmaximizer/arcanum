use crate::types::ProcessId;
use std::collections::HashMap;

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
