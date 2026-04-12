use crate::log::ProcessId;
use std::collections::HashMap;

trait KVState {
    fn set(&mut self, process: &ProcessId, key: &str, value: String);
    fn get(&self, process: &ProcessId, key: &str) -> Option<String>;
}

struct InMemoryKVState {
    kv: HashMap<ProcessId, HashMap<String, String>>,
}

impl KVState for InMemoryKVState {
    fn set(&mut self, process: &ProcessId, key: &str, value: String) {
        self.kv
            .entry(process.clone())
            .or_insert_with(HashMap::new)
            .insert(key.into(), value);
    }
    fn get(&self, process: &ProcessId, key: &str) -> Option<String> {
        self.kv.get(process)?.get(key.into()).cloned()
    }
}
