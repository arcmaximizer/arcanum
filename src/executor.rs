use crate::{
    scheduler::{self, SchedulerMsg},
    store, types,
};
use mlua::{Lua, Value};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

const WRAPPER_CODE: &str = r#"
local _yield = coroutine.yield

local function syscall(syscall_type, ...)
    return _yield({type = syscall_type, args = {...}})
end

local http = {}
function http.get(url)
    return syscall("http_get", url)
end

local kv = {}
function kv.get(key)
    return syscall("kv_get", key)
end
function kv.set(key, value)
    return syscall("kv_set", key, value)
end

rawset(_G, "http", http)
rawset(_G, "kv", kv)
rawset(_G, "coroutine", nil)
rawset(_G, "syscall", nil)

return function(main_fn)
    return main_fn
end
"#;

pub async fn run_executor(
    process: types::ProcessId,
    mut work_rx: mpsc::Receiver<scheduler::Proposal>,
    scheduler_tx: mpsc::UnboundedSender<scheduler::SchedulerMsg>,
    store_tx: mpsc::UnboundedSender<store::StoreMsg>,
) {
    let vm = Lua::new();

    let (tx, rx) = oneshot::channel();
    store_tx
        .send(store::StoreMsg::GetAssetByName {
            name: process.clone().into(),
            asset: "main.lua".to_string(),
            resp: tx,
        })
        .unwrap();
    let user_code = String::from_utf8(rx.await.unwrap().unwrap().to_vec()).unwrap();

    // Load wrapper first
    let setup: mlua::Function = vm.load(WRAPPER_CODE).eval().unwrap();
    let user_fn: mlua::Function = vm.load(&user_code).eval().unwrap();
    let wrapped: mlua::Function = setup.call(user_fn).unwrap();

    // wrapped now has the syscall API available
    // coroutine is removed from globals

    let mut threads: HashMap<types::EventId, mlua::Thread> = HashMap::new();

    while let Some(proposal) = work_rx.recv().await {
        if let Some(event) = proposal.event {
            let (tx, rx) = oneshot::channel();
            scheduler_tx
                .send(SchedulerMsg::GetChunks {
                    event: event.clone(),
                    resp: tx,
                })
                .unwrap();
            let chunks = rx
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("No chunks for event {}", event));

            if chunks.is_empty() {
                panic!("No chunks for event {}", event);
            }

            // TODO: replay chunks in VM to restore state
            // For now, just run the new proposal

            let thread = threads
                .entry(event.clone())
                .or_insert_with(|| vm.create_thread(wrapped.clone()).unwrap());

            // Resume with proposal inputs
            match thread.resume::<mlua::Value>(mlua::Value::String(
                vm.create_string(&proposal.inputs.join(",")).unwrap(),
            )) {
                Ok(result) => {
                    println!("Lua returned: {:?}", result);
                }
                Err(e) => {
                    println!("Lua error: {}", e);
                }
            }
        }
    }
}
