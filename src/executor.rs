use crate::log::{Event, EventId, ProcessId, EventStatus};
use deno_core::{op2, OpState, FsModuleLoader};
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;
use std::collections::HashMap;

pub async fn run_js(file_path: &str) -> anyhow::Result<()> {
    let main_module = deno_core::resolve_path(file_path, &std::env::current_dir()?)?;
    let mut js_runtime = deno_core::JsRuntime::new(deno_core::RuntimeOptions {
        module_loader: Some(Rc::new(FsModuleLoader)),
        ..Default::default()
    });

    let mod_id = js_runtime.load_main_es_module(&main_module).await?;
    let result = js_runtime.mod_evaluate(mod_id);
    js_runtime.run_event_loop(Default::default()).await?;
    result.await?;
    Ok(())
}

pub struct RunningChunk {
    pub event_id: EventId,
    pub chunk_seq: u64,
}

#[derive(Clone, Default)]
pub struct InMemoryKVState {
    data: HashMap<(String, String), String>,
}

impl InMemoryKVState {
    pub fn get(&self, process: &str, key: &str) -> Option<String> {
        self.data.get(&(process.to_string(), key.to_string())).cloned()
    }
    
    pub fn set(&mut self, process: &str, key: String, value: String) {
        self.data.insert((process.to_string(), key), value);
    }
}

#[op2]
#[string]
pub fn op_kv_get(
    state: &mut OpState,
    #[string] process: String,
    #[string] key: String,
) -> Option<String> {
    let kv = state.try_borrow::<Arc<TokioMutex<InMemoryKVState>>>()?;
    kv.try_lock().ok()?.get(&process, &key)
}

#[op2(fast)]
pub fn op_kv_set(
    state: &mut OpState,
    #[string] process: String,
    #[string] key: String,
    #[string] value: String,
) {
    if let Some(kv) = state.try_borrow::<Arc<TokioMutex<InMemoryKVState>>>() {
        if let Ok(mut kv) = kv.try_lock() {
            kv.set(&process, key, value);
        }
    }
}

#[op2(fast)]
pub fn op_lock(
    _state: &mut OpState,
    #[string] _process: String,
) {
}

#[op2(fast)]
pub fn op_unlock(
    _state: &mut OpState,
    #[string] _process: String,
) {
}

#[op2]
#[string]
pub fn op_send(
    state: &mut OpState,
    #[string] target: String,
    #[string] message: String,
) -> String {
    if let Some(effects) = state.try_borrow::<Arc<TokioMutex<Vec<Event>>>>() {
        if let Ok(mut effects) = effects.try_lock() {
            let target_parts: Vec<&str> = target.split('/').collect();
            if target_parts.len() == 2 {
                effects.push(Event {
                    id: EventId {
                        proc: ProcessId {
                            app: target_parts[0].to_string(),
                            proc: target_parts[1].to_string(),
                        },
                        seq: 0,
                    },
                    cause: None,
                    args: Some(format!(r#"["{}"]"#, message)),
                    status: EventStatus::Pending,
                    metadata: None,
                });
            }
        }
    }
    format!("sent to {}", target)
}

#[op2(fast)]
pub fn op_record_chunk(
    _state: &mut OpState,
    #[string] _status: String,
) {}

#[op2(fast)]
pub fn op_start_chunk(_state: &mut OpState) {}

#[op2(fast)]
pub fn op_add_input(
    _state: &mut OpState,
    #[string] _itype: String,
    #[string] _value: String,
) {}