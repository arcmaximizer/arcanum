use crate::log::{Event, EventId, ProcessId, EventStatus};
use deno_core::{op2, OpState, FsModuleLoader, ModuleSpecifier};
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;
use std::collections::HashMap;

pub struct LoadedHandler {
    pub module_id: deno_core::ModuleId,
}

#[derive(Clone)]
pub struct JsRuntimeState {
    pub kv: Arc<TokioMutex<InMemoryKVState>>,
    pub effects: Arc<TokioMutex<Vec<Event>>>,
}

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

pub fn create_runtime(
    kv: Arc<TokioMutex<InMemoryKVState>>,
    effects: Arc<TokioMutex<Vec<Event>>>,
) -> deno_core::JsRuntime {
    let js_runtime = deno_core::JsRuntime::new(deno_core::RuntimeOptions {
        extensions: vec![crate::ops::executor_ops::init()],
        module_loader: Some(Rc::new(FsModuleLoader)),
        ..Default::default()
    });
    
    let op_state = js_runtime.op_state();
    op_state.borrow_mut().put(Arc::clone(&kv));
    op_state.borrow_mut().put(Arc::clone(&effects));
    
    js_runtime
}

pub async fn load_handler(
    js_runtime: &mut deno_core::JsRuntime,
    code: &str,
) -> Result<LoadedHandler, anyhow::Error> {
    let temp_dir = std::env::temp_dir();
    let temp_file = temp_dir.join(format!("handler_{}.js", uuid::Uuid::new_v4()));
    
    std::fs::write(&temp_file, code)?;
    
    let module_specifier = ModuleSpecifier::from_file_path(&temp_file).unwrap();
    let mod_id = js_runtime.load_main_es_module(&module_specifier).await?;
    let result = js_runtime.mod_evaluate(mod_id);
    js_runtime.run_event_loop(Default::default()).await?;
    result.await?;
    
    std::fs::remove_file(temp_file).ok();
    
    Ok(LoadedHandler { module_id: mod_id })
}

pub async fn call_handler(
    js_runtime: &mut deno_core::JsRuntime,
    msg: serde_json::Value,
) -> Result<serde_json::Value, anyhow::Error> {
    let code = format!(
        "(async () => await handler({}))()",
        serde_json::to_string(&msg)?
    );
    
    let result = js_runtime.execute_script("call_handler.js", code)?;
    
    let result_str = format!("{:?}", result);
    Ok(serde_json::Value::String(result_str))
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