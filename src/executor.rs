use crate::{
    scheduler::{self, Proposal, Receipt, SchedulerMsg, Syscall},
    store, types,
};
use mlua::Lua;
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

fn extract_str(value: &mlua::Value, field: &str) -> String {
    value
        .as_table()
        .and_then(|t| t.get::<String>(field).ok())
        .unwrap_or_default()
}

fn parse_syscall(
    value: &mlua::Value,
    event: &types::EventId,
    log_seq: u64,
    kv_state: &HashMap<String, String>,
) -> Syscall {
    let sys_type = extract_str(value, "type");
    let args = value
        .as_table()
        .and_then(|t| t.get::<mlua::Table>("args").ok());

    match sys_type.as_str() {
        "kv_get" => {
            let key = args
                .as_ref()
                .and_then(|a| a.get(1).ok())
                .unwrap_or_default();
            let current_value = kv_state.get(&key).cloned().unwrap_or_default();
            Syscall::KVRead { key, current_value }
        }
        "kv_set" => {
            let key = args
                .as_ref()
                .and_then(|a| a.get(1).ok())
                .unwrap_or_default();
            let new_value = args
                .as_ref()
                .and_then(|a| a.get::<String>(2).ok())
                .unwrap_or_default();
            Syscall::KVWrite { key, new_value }
        }
        "http_get" => {
            let url = args
                .as_ref()
                .and_then(|a| a.get(1).ok())
                .unwrap_or_default();
            Syscall::Call {
                proposal: Proposal {
                    process: types::ProcessId {
                        app: "sys".to_string(),
                        proc: "http".to_string(),
                    },
                    event: None,
                    input: url,
                    promise: Some(scheduler::Promise {
                        id: log_seq,
                        target: event.clone(),
                    }),
                },
            }
        }
        "call" => {
            let target = args
                .as_ref()
                .and_then(|a| a.get::<String>(1).ok())
                .unwrap_or_default();
            let input = args
                .as_ref()
                .and_then(|a| a.get::<String>(2).ok())
                .unwrap_or_default();
            let (app, proc) = target
                .strip_prefix("^")
                .and_then(|t| t.split_once('/'))
                .unwrap_or(("", ""));
            Syscall::Call {
                proposal: Proposal {
                    process: types::ProcessId {
                        app: app.to_string(),
                        proc: proc.to_string(),
                    },
                    event: None,
                    input,
                    promise: Some(scheduler::Promise {
                        id: log_seq,
                        target: event.clone(),
                    }),
                },
            }
        }
        "notify" => {
            let target = args
                .as_ref()
                .and_then(|a| a.get::<String>(1).ok())
                .unwrap_or_default();
            let input = args
                .as_ref()
                .and_then(|a| a.get::<String>(2).ok())
                .unwrap_or_default();
            let (app, proc) = target
                .strip_prefix("^")
                .and_then(|t| t.split_once('/'))
                .unwrap_or(("", ""));
            Syscall::Notify {
                proposal: Proposal {
                    process: types::ProcessId {
                        app: app.to_string(),
                        proc: proc.to_string(),
                    },
                    event: None,
                    input,
                    promise: None,
                },
            }
        }
        _ => panic!("Unknown syscall type: {}", sys_type),
    }
}

fn is_non_blocking(syscall: &Syscall) -> bool {
    matches!(syscall, Syscall::Call { .. } | Syscall::Notify { .. })
}

pub async fn run_executor(
    process: types::ProcessId,
    mut work_rx: mpsc::Receiver<Proposal>,
    scheduler_tx: mpsc::UnboundedSender<SchedulerMsg>,
    store_tx: mpsc::UnboundedSender<store::StoreMsg>,
) {
    let lua = Lua::new();

    let (tx, rx) = oneshot::channel();
    store_tx
        .send(store::StoreMsg::GetAssetByName {
            name: process.clone().into(),
            asset: "main.lua".to_string(),
            resp: tx,
        })
        .unwrap();
    let user_code = String::from_utf8(rx.await.unwrap().unwrap().to_vec()).unwrap();

    let setup: mlua::Function = lua.load(WRAPPER_CODE).eval().unwrap();
    let user_fn: mlua::Function = lua.load(&user_code).eval().unwrap();
    let wrapped: mlua::Function = setup.call(user_fn).unwrap();

    let mut threads: HashMap<types::EventId, mlua::Thread> = HashMap::new();
    let mut kv_state: HashMap<String, String> = HashMap::new();
    let mut log_seq: u64 = 0;
    let mut event_seqs: HashMap<types::EventId, u64> = HashMap::new();

    while let Some(proposal) = work_rx.recv().await {
        let event = if let Some(ref e) = proposal.event {
            e.clone()
        } else {
            let (tx, rx) = oneshot::channel();
            scheduler_tx
                .send(SchedulerMsg::GetNextEventId {
                    process: process.clone(),
                    resp: tx,
                })
                .unwrap();
            rx.await.unwrap()
        };

        let thread = threads
            .entry(event.clone())
            .or_insert_with(|| lua.create_thread(wrapped.clone()).unwrap());

        let mut input = mlua::Value::String(lua.create_string(&proposal.input).unwrap());

        loop {
            let in_event_seq = *event_seqs.entry(event.clone()).or_insert(0);

            match thread.resume::<mlua::Value>(input.clone()) {
                Ok(mlua::Value::Table(table)) => {
                    let syscall =
                        parse_syscall(&mlua::Value::Table(table), &event, log_seq, &kv_state);

                    let receipt = Receipt {
                        proposal: proposal.clone(),
                        in_event_seq,
                        in_log_seq: log_seq,
                        syscalls: vec![syscall.clone()],
                        returns: String::default(),
                    };

                    let is_final_syscall = is_non_blocking(&syscall);

                    let (tx, rx) = oneshot::channel();
                    scheduler_tx
                        .send(SchedulerMsg::Satisfy {
                            proposal: proposal.clone(),
                            receipt,
                            is_final: is_final_syscall,
                            resp: tx,
                        })
                        .unwrap();
                    rx.await.unwrap().unwrap();

                    event_seqs.insert(event.clone(), in_event_seq + 1);
                    log_seq += 1;

                    match syscall {
                        Syscall::KVRead { current_value, .. } => {
                            input = mlua::Value::String(lua.create_string(&current_value).unwrap());
                        }
                        Syscall::KVWrite { key, new_value } => {
                            kv_state.insert(key, new_value);
                            input = mlua::Value::Nil;
                        }
                        Syscall::Call { .. } | Syscall::Notify { .. } => {
                            break;
                        }
                    }
                }
                Ok(return_value) => {
                    let returns = extract_return(&return_value, &lua);
                    let in_event_seq = *event_seqs.entry(event.clone()).or_insert(0);

                    let receipt = Receipt {
                        proposal: proposal.clone(),
                        in_event_seq,
                        in_log_seq: log_seq,
                        syscalls: Vec::new(),
                        returns,
                    };

                    let (tx, rx) = oneshot::channel();
                    scheduler_tx
                        .send(SchedulerMsg::Satisfy {
                            proposal: proposal.clone(),
                            receipt,
                            is_final: true,
                            resp: tx,
                        })
                        .unwrap();
                    rx.await.unwrap().unwrap();

                    log_seq += 1;
                    break;
                }
                Err(e) => {
                    eprintln!("Lua error: {}", e);
                    break;
                }
            }
        }
    }
}

fn extract_return(value: &mlua::Value, lua: &Lua) -> String {
    match value {
        mlua::Value::Nil => String::new(),
        mlua::Value::String(s) => s.to_string_lossy(),
        other => lua
            .coerce_string(other.clone())
            .ok()
            .flatten()
            .map(|s| s.to_string_lossy())
            .unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::{NextAction, Promise};
    use crate::types::{EventId, ProcessId};
    use anyhow::Result;
    use bytes::Bytes;
    use tokio::sync::{mpsc, oneshot};

    struct ExecutorHarness {
        scheduler_rx: mpsc::UnboundedReceiver<SchedulerMsg>,
        store_rx: mpsc::UnboundedReceiver<store::StoreMsg>,
        work_tx: mpsc::Sender<Proposal>,
        process: ProcessId,
    }

    impl ExecutorHarness {
        async fn new() -> Self {
            let (work_tx, work_rx) = mpsc::channel::<Proposal>(32);
            let (scheduler_tx, scheduler_rx) = mpsc::unbounded_channel::<SchedulerMsg>();
            let (store_tx, store_rx) = mpsc::unbounded_channel::<store::StoreMsg>();

            let process = ProcessId {
                app: "test".to_string(),
                proc: "p".to_string(),
            };

            tokio::spawn(run_executor(
                process.clone(),
                work_rx,
                scheduler_tx,
                store_tx,
            ));

            Self {
                scheduler_rx,
                store_rx,
                work_tx,
                process,
            }
        }

        async fn feed_code(&mut self, lua_code: &str) {
            let code = lua_code.to_owned();
            match self.store_rx.recv().await.unwrap() {
                store::StoreMsg::GetAssetByName {
                    name: _,
                    asset,
                    resp,
                } => {
                    assert_eq!(asset, "main.lua");
                    resp.send(Some(Bytes::from(code))).unwrap();
                }
                other => panic!("unexpected store message: {:?}", other),
            }
        }

        async fn send_proposal(&mut self, input: &str) -> Proposal {
            let proposal = Proposal {
                process: self.process.clone(),
                event: None,
                input: input.to_string(),
                promise: None,
            };
            self.work_tx.send(proposal.clone()).await.unwrap();
            proposal
        }

        async fn expect_get_next_event_id(&mut self) -> EventId {
            match self.scheduler_rx.recv().await.unwrap() {
                SchedulerMsg::GetNextEventId { process, resp } => {
                    assert_eq!(process, self.process);
                    let event = EventId {
                        app: "test".to_string(),
                        proc: "p".to_string(),
                        seq: 0,
                    };
                    resp.send(event.clone()).unwrap();
                    event
                }
                other => panic!("expected GetNextEventId, got {:?}", other),
            }
        }

        async fn expect_satisfy(
            &mut self,
            is_final: bool,
        ) -> (Receipt, oneshot::Sender<Result<NextAction>>) {
            match self.scheduler_rx.recv().await.unwrap() {
                SchedulerMsg::Satisfy {
                    proposal: _,
                    receipt,
                    is_final: got_final,
                    resp,
                } => {
                    assert_eq!(got_final, is_final, "is_final mismatch");
                    (receipt, resp)
                }
                other => panic!("expected Satisfy, got {:?}", other),
            }
        }

        fn respond_satisfy(resp: oneshot::Sender<Result<NextAction>>, event: EventId) {
            resp.send(Ok(NextAction {
                event,
                proposal: None,
            }))
            .unwrap();
        }
    }

    // --- Simple return, no syscalls ---

    #[tokio::test]
    async fn test_simple_return() {
        let mut h = ExecutorHarness::new().await;
        h.feed_code("return function() return 'hello' end").await;

        let _p = h.send_proposal("world").await;
        let event = h.expect_get_next_event_id().await;

        let (receipt, resp) = h.expect_satisfy(true).await;
        assert_eq!(receipt.syscalls, vec![]);
        assert_eq!(receipt.returns, "hello");
        assert_eq!(receipt.in_event_seq, 0);
        assert_eq!(receipt.in_log_seq, 0);
        ExecutorHarness::respond_satisfy(resp, event);
    }

    #[tokio::test]
    async fn test_return_nil() {
        let mut h = ExecutorHarness::new().await;
        h.feed_code("return function() return nil end").await;

        h.send_proposal("world").await;
        let event = h.expect_get_next_event_id().await;

        let (receipt, resp) = h.expect_satisfy(true).await;
        assert_eq!(receipt.returns, "");
        ExecutorHarness::respond_satisfy(resp, event);
    }

    #[tokio::test]
    async fn test_return_number() {
        let mut h = ExecutorHarness::new().await;
        h.feed_code("return function() return 42 end").await;

        h.send_proposal("world").await;
        let event = h.expect_get_next_event_id().await;

        let (receipt, resp) = h.expect_satisfy(true).await;
        assert_eq!(receipt.returns, "42");
        ExecutorHarness::respond_satisfy(resp, event);
    }

    // --- KV syscalls ---

    #[tokio::test]
    async fn test_kv_get_then_return() {
        let mut h = ExecutorHarness::new().await;
        h.feed_code("return function() local v = kv.get('foo'); return v end")
            .await;

        h.send_proposal("world").await;
        let event = h.expect_get_next_event_id().await;

        // First: KVRead receipt (intermediate)
        let (rec1, resp1) = h.expect_satisfy(false).await;
        assert_eq!(rec1.in_event_seq, 0);
        assert_eq!(rec1.in_log_seq, 0);
        assert_eq!(rec1.returns, "");
        assert_eq!(
            rec1.syscalls,
            vec![Syscall::KVRead {
                key: "foo".to_string(),
                current_value: String::new(),
            }]
        );
        ExecutorHarness::respond_satisfy(resp1, event.clone());

        // Second: final receipt with empty value
        let (rec2, resp2) = h.expect_satisfy(true).await;
        assert_eq!(rec2.in_event_seq, 1);
        assert_eq!(rec2.in_log_seq, 1);
        assert_eq!(rec2.returns, "");
        assert_eq!(rec2.syscalls, vec![]);
        ExecutorHarness::respond_satisfy(resp2, event);
    }

    #[tokio::test]
    async fn test_kv_set_then_get_then_return() {
        let mut h = ExecutorHarness::new().await;
        h.feed_code("return function() kv.set('x', '42'); local v = kv.get('x'); return v end")
            .await;

        h.send_proposal("world").await;
        let event = h.expect_get_next_event_id().await;

        // KVWrite (intermediate)
        let (rec1, resp1) = h.expect_satisfy(false).await;
        assert_eq!(rec1.in_event_seq, 0);
        assert_eq!(
            rec1.syscalls,
            vec![Syscall::KVWrite {
                key: "x".to_string(),
                new_value: "42".to_string(),
            }]
        );
        ExecutorHarness::respond_satisfy(resp1, event.clone());

        // KVRead with value "42" (intermediate)
        let (rec2, resp2) = h.expect_satisfy(false).await;
        assert_eq!(rec2.in_event_seq, 1);
        assert_eq!(rec2.in_log_seq, 1);
        assert_eq!(
            rec2.syscalls,
            vec![Syscall::KVRead {
                key: "x".to_string(),
                current_value: "42".to_string(),
            }]
        );
        ExecutorHarness::respond_satisfy(resp2, event.clone());

        // Final with "42"
        let (rec3, resp3) = h.expect_satisfy(true).await;
        assert_eq!(rec3.in_event_seq, 2);
        assert_eq!(rec3.in_log_seq, 2);
        assert_eq!(rec3.returns, "42");
        assert_eq!(rec3.syscalls, vec![]);
        ExecutorHarness::respond_satisfy(resp3, event);
    }

    // --- Non-blocking syscall ---

    #[tokio::test]
    async fn test_http_get_produces_call() {
        let mut h = ExecutorHarness::new().await;
        h.feed_code("return function() http.get('https://example.com') end")
            .await;

        h.send_proposal("world").await;
        let event = h.expect_get_next_event_id().await;

        let (receipt, resp) = h.expect_satisfy(true).await;
        assert_eq!(receipt.in_event_seq, 0);
        assert_eq!(receipt.in_log_seq, 0);
        assert_eq!(receipt.returns, "");

        match &receipt.syscalls[0] {
            Syscall::Call { proposal } => {
                assert_eq!(proposal.process.app, "sys");
                assert_eq!(proposal.process.proc, "http");
                assert_eq!(proposal.input, "https://example.com");
                assert_eq!(
                    proposal.promise,
                    Some(Promise {
                        id: 0,
                        target: event.clone(),
                    })
                );
            }
            other => panic!("expected Call, got {:?}", other),
        }
        ExecutorHarness::respond_satisfy(resp, event);
    }

    #[tokio::test]
    async fn test_kv_then_http_get() {
        let mut h = ExecutorHarness::new().await;
        h.feed_code("return function() kv.set('x', '1'); http.get('https://x.com') end")
            .await;

        h.send_proposal("world").await;
        let event = h.expect_get_next_event_id().await;

        // KVWrite (intermediate)
        let (rec1, resp1) = h.expect_satisfy(false).await;
        assert_eq!(
            rec1.syscalls,
            vec![Syscall::KVWrite {
                key: "x".to_string(),
                new_value: "1".to_string(),
            }]
        );
        ExecutorHarness::respond_satisfy(resp1, event.clone());

        // Call (final)
        let (rec2, resp2) = h.expect_satisfy(true).await;
        assert!(
            matches!(&rec2.syscalls[0], Syscall::Call { proposal } if proposal.process.app == "sys")
        );
        ExecutorHarness::respond_satisfy(resp2, event);
    }

    // --- Error handling ---

    #[tokio::test]
    async fn test_lua_error_does_not_produce_receipt() {
        let mut h = ExecutorHarness::new().await;
        h.feed_code("return function() error('boom') end").await;

        h.send_proposal("world").await;
        let _ = h.expect_get_next_event_id().await;

        // Executor should hit Err and break without sending Satisfy.
        // Drop work_tx to allow executor to exit.
        drop(h.work_tx);

        // Verify no Satisfy is sent
        while let Some(msg) = h.scheduler_rx.recv().await {
            if matches!(msg, SchedulerMsg::Satisfy { .. }) {
                panic!("unexpected Satisfy after Lua error");
            }
            // Handle any other messages
            if let SchedulerMsg::GetNextEventId { resp, .. } = msg {
                resp.send(EventId {
                    app: "test".to_string(),
                    proc: "p".to_string(),
                    seq: 0,
                })
                .unwrap();
            }
        }
    }
}
