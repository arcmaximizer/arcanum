use crate::{
    scheduler::{self, Proposal, Receipt, SchedulerHandle, Syscall},
    state::StateHandle,
    types,
};
use mlua::Lua;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing;

fn mlua_to_json(value: &mlua::Value) -> JsonValue {
    match value {
        mlua::Value::Nil => JsonValue::Null,
        mlua::Value::Boolean(b) => JsonValue::Bool(*b),
        mlua::Value::Integer(i) => {
            JsonValue::Number(serde_json::Number::from(*i))
        }
        mlua::Value::Number(n) => {
            let num = serde_json::Number::from_f64(*n).unwrap_or(serde_json::Number::from(0));
            JsonValue::Number(num)
        }
        mlua::Value::String(s) => {
            JsonValue::String(s.to_string_lossy())
        }
        mlua::Value::Table(t) => {
            let mut is_array = true;
            let mut array = Vec::new();
            let mut map = serde_json::Map::new();

            for pair in t.pairs::<mlua::Value, mlua::Value>() {
                if let Ok((k, v)) = pair {
                    match &k {
                        mlua::Value::Integer(idx)
                            if *idx == array.len() as i64 + 1 =>
                        {
                            array.push(mlua_to_json(&v));
                        }
                        mlua::Value::String(s) => {
                            is_array = false;
                            map.insert(s.to_string_lossy(), mlua_to_json(&v));
                        }
                        _ => {
                            is_array = false;
                            map.insert(format!("{:?}", k), mlua_to_json(&v));
                        }
                    }
                }
            }

            if is_array && !array.is_empty() {
                JsonValue::Array(array)
            } else if !map.is_empty() {
                JsonValue::Object(map)
            } else if array.is_empty() {
                JsonValue::Object(serde_json::Map::new())
            } else {
                JsonValue::Array(array)
            }
        }
        _ => JsonValue::Null,
    }
}

fn json_to_mlua(lua: &Lua, value: &JsonValue) -> mlua::Result<mlua::Value> {
    match value {
        JsonValue::Null => Ok(mlua::Value::Nil),
        JsonValue::Bool(b) => Ok(mlua::Value::Boolean(*b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(mlua::Value::Integer(i))
            } else {
                Ok(mlua::Value::Number(n.as_f64().unwrap_or(0.0)))
            }
        }
        JsonValue::String(s) => Ok(mlua::Value::String(lua.create_string(s)?)),
        JsonValue::Array(arr) => {
            let table = lua.create_table()?;
            for (i, v) in arr.iter().enumerate() {
                table.set(i + 1, json_to_mlua(lua, v)?)?;
            }
            Ok(mlua::Value::Table(table))
        }
        JsonValue::Object(map) => {
            let table = lua.create_table()?;
            for (k, v) in map {
                let key: mlua::Value = mlua::Value::String(lua.create_string(k)?);
                table.set(key, json_to_mlua(lua, v)?)?;
            }
            Ok(mlua::Value::Table(table))
        }
    }
}

fn mlua_value_to_bytes(value: &mlua::Value) -> Vec<u8> {
    let json = mlua_to_json(value);
    rmp_serde::to_vec(&json).unwrap_or_default()
}

fn bytes_to_mlua_value(lua: &Lua, bytes: &[u8]) -> mlua::Value {
    if bytes.is_empty() {
        return mlua::Value::Nil;
    }
    let json: JsonValue = rmp_serde::from_slice(bytes).unwrap_or(JsonValue::Null);
    json_to_mlua(lua, &json).unwrap_or(mlua::Value::Nil)
}

const WRAPPER_CODE: &str = include_str!("wrapper.lua");

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
    lua: &Lua,
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
            let url: String = args
                .as_ref()
                .and_then(|a| a.get(1).ok())
                .unwrap_or_default();
            Syscall::Call {
                proposal: Proposal {
                    process: types::ProcessId {
                        namespace: "sys".to_string(),
                        app: "http".to_string(),
                        proc: "runtime".to_string(),
                    },
                    event: None,
                    input: mlua_value_to_bytes(&mlua::Value::String(
                        lua.create_string(&url).unwrap(),
                    )),
                    promise: Some(scheduler::Promise {
                        id: log_seq,
                        target: event.clone(),
                    }),
                },
            }
        }
        "call" => {
            let target: String = args
                .as_ref()
                .and_then(|a| a.get(1).ok())
                .unwrap_or_default();
            let process =
                types::ProcessId::try_from(target.as_str()).unwrap_or_else(|_| types::ProcessId {
                    namespace: String::new(),
                    app: String::new(),
                    proc: String::new(),
                });
            let input = args
                .as_ref()
                .and_then(|a| a.get::<mlua::Value>(2).ok())
                .map(|v| mlua_value_to_bytes(&v))
                .unwrap_or_default();
            Syscall::Call {
                proposal: Proposal {
                    process,
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
            let target: String = args
                .as_ref()
                .and_then(|a| a.get(1).ok())
                .unwrap_or_default();
            let process =
                types::ProcessId::try_from(target.as_str()).unwrap_or_else(|_| types::ProcessId {
                    namespace: String::new(),
                    app: String::new(),
                    proc: String::new(),
                });
            let input = args
                .as_ref()
                .and_then(|a| a.get::<mlua::Value>(2).ok())
                .map(|v| mlua_value_to_bytes(&v))
                .unwrap_or_default();
            Syscall::Notify {
                proposal: Proposal {
                    process,
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
    matches!(syscall, Syscall::Call { .. })
}

pub struct ExecutorHandle {
    sender: mpsc::UnboundedSender<Proposal>,
    process: types::ProcessId,
}

impl ExecutorHandle {
    pub fn new(
        process: types::ProcessId,
        scheduler: SchedulerHandle,
        state: StateHandle,
        user_code: String,
    ) -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        tokio::spawn(run_executor(
            process.clone(),
            receiver,
            scheduler,
            state,
            user_code,
        ));
        Self { sender, process }
    }

    pub fn sender(&self) -> mpsc::UnboundedSender<Proposal> {
        self.sender.clone()
    }

    pub fn process(&self) -> &types::ProcessId {
        &self.process
    }
}

pub async fn run_executor(
    process: types::ProcessId,
    mut work_rx: mpsc::UnboundedReceiver<Proposal>,
    scheduler: SchedulerHandle,
    state: StateHandle,
    user_code: String,
) {
    let lua = Lua::new();

    let setup: mlua::Function = lua.load(WRAPPER_CODE).eval().unwrap();
    let user_fn: mlua::Function = lua.load(&user_code).eval().unwrap();
    let wrapped: mlua::Function = setup.call(user_fn).unwrap();

    let mut threads: HashMap<types::EventId, mlua::Thread> = HashMap::new();

    let mut event_seqs: HashMap<types::EventId, u64> = HashMap::new();

    let mut kv_state: HashMap<String, String> = HashMap::new();

    while let Some(proposal) = work_rx.recv().await {
        if let Some(ref promise) = proposal.promise {
            tracing::debug!(
                "Received proposal: process={} {} input={:?}",
                proposal.process,
                promise,
                proposal.input,
            );
        } else {
            tracing::debug!(
                "Received proposal: process={} input={:?}",
                proposal.process,
                proposal.input,
            );
        }
        let event = if let Some(ref e) = proposal.event {
            e.clone()
        } else {
            tracing::debug!("No event ID provided, requesting from scheduler");
            let e = scheduler.get_next_event_id(process.clone()).await;
            tracing::debug!("event={} Got event ID: seq={}", e, e.seq);
            e
        };

        let thread = threads.entry(event.clone()).or_insert_with(|| {
            tracing::debug!("event={} Creating new Lua thread", event);
            lua.create_thread(wrapped.clone()).unwrap()
        });

        let mut input = bytes_to_mlua_value(&lua, &proposal.input);

        loop {
            let in_event_seq = *event_seqs.entry(event.clone()).or_insert(0);
            tracing::debug!("Loop start: event={} in_event_seq={}", event, in_event_seq);

            let log_seq = scheduler.get_log_seq(process.clone()).await;
            tracing::debug!("event={} Got log_seq={}", event, log_seq);

            tracing::debug!("event={} Resuming Lua thread with input={:?}", event, input);
            match thread.resume::<mlua::Value>(input.clone()) {
                Ok(mlua::Value::Table(table)) => {
                    tracing::debug!("event={} Got syscall from Lua", event);
                    let syscall = parse_syscall(
                        &mlua::Value::Table(table),
                        &event,
                        log_seq,
                        &kv_state,
                        &lua,
                    );
                    tracing::debug!("event={} Parsed syscall: {:?}", event, syscall);

                    let receipt = Receipt {
                        proposal: proposal.clone(),
                        in_event_seq,
                        in_log_seq: log_seq,
                        syscalls: vec![syscall.clone()],
                        returns: Vec::new(),
                    };

                    let is_final_syscall = is_non_blocking(&syscall);
                    tracing::debug!(
                        "event={} Sending satisfy: is_final={}",
                        event,
                        is_final_syscall
                    );

                    let next_action = scheduler
                        .satisfy(proposal.clone(), receipt, is_final_syscall)
                        .await
                        .unwrap();
                    tracing::debug!(
                        "Got next action: event={} proposal={:?}",
                        next_action.event,
                        next_action.proposal
                    );

                    event_seqs.insert(event.clone(), in_event_seq + 1);

                    match syscall {
                        Syscall::KVRead { key, .. } => {
                            tracing::debug!("event={} KVRead: key={}", event, key);
                            let value = kv_state.get(&key).cloned().unwrap_or_default();
                            tracing::debug!("event={} KVRead: value={}", event, value);
                            input = mlua::Value::String(lua.create_string(&value).unwrap());
                        }
                        Syscall::KVWrite { key, new_value } => {
                            tracing::debug!(
                                "event={} KVWrite: key={} value={}",
                                event,
                                key,
                                new_value
                            );
                            state
                                .set(process.clone(), key.clone(), new_value.clone())
                                .await;
                            kv_state.insert(key, new_value);
                            tracing::debug!("event={} KVWrite: state updated", event);
                            input = mlua::Value::Nil;
                        }
                        Syscall::Notify { proposal, .. } => {
                            tracing::debug!(
                                "event={} Notify: target={} input={:?}",
                                event,
                                proposal.process,
                                proposal.input
                            );
                            input = mlua::Value::Nil;
                        }
                        Syscall::Call { proposal, .. } => {
                            if let Some(ref promise) = proposal.promise {
                                tracing::debug!(
                                    "event={} Call: target={} {} input={:?}",
                                    event,
                                    proposal.process,
                                    promise,
                                    proposal.input
                                );
                            } else {
                                tracing::debug!(
                                    "event={} Call: target={} input={:?}",
                                    event,
                                    proposal.process,
                                    proposal.input
                                );
                            }
                            scheduler.add_proposal(proposal.clone()).await;
                            break;
                        }
                    }
                }
                Ok(return_value) => {
                    tracing::debug!("event={} Lua returned: {:?}", event, return_value);
                    let returns = mlua_value_to_bytes(&return_value);
                    tracing::debug!("event={} Serialized return bytes: {:?}", event, returns);
                    let in_event_seq = *event_seqs.entry(event.clone()).or_insert(0);

                    let receipt = Receipt {
                        proposal: proposal.clone(),
                        in_event_seq,
                        in_log_seq: log_seq,
                        syscalls: Vec::new(),
                        returns,
                    };

                    scheduler
                        .satisfy(proposal.clone(), receipt, true)
                        .await
                        .unwrap();
                    tracing::debug!("event={} Sent final satisfy, awaiting response", event);
                    tracing::debug!("event={} Final satisfy complete, breaking loop", event);

                    threads.remove(&event);
                    break;
                }
                Err(e) => {
                    tracing::error!("event={} Lua error: {}", event, e);
                    tracing::debug!("event={} Lua error occurred, breaking loop", event);
                    threads.remove(&event);
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::{NextAction, Promise, SchedulerMsg};
    use crate::state::StateMsg;
    use crate::types::{EventId, ProcessId};
    use anyhow::Result;
    use tokio::sync::{mpsc, oneshot};

    fn mp(s: &str) -> Vec<u8> {
        rmp_serde::to_vec(&serde_json::Value::String(s.into())).unwrap()
    }

    fn mp_null() -> Vec<u8> {
        rmp_serde::to_vec(&serde_json::Value::Null).unwrap()
    }

    struct ExecutorHarness {
        scheduler_rx: mpsc::UnboundedReceiver<SchedulerMsg>,
        state_rx: mpsc::UnboundedReceiver<StateMsg>,
        work_tx: mpsc::UnboundedSender<Proposal>,
        process: ProcessId,
        log_seq: u64,
    }

    impl ExecutorHarness {
        async fn new(lua_code: String) -> Self {
            let (work_tx, work_rx) = mpsc::unbounded_channel::<Proposal>();
            let (scheduler_tx, scheduler_rx) = mpsc::unbounded_channel::<SchedulerMsg>();
            let (state_tx, state_rx) = mpsc::unbounded_channel::<StateMsg>();

            let process = ProcessId {
                namespace: "test".to_string(),
                app: "p".to_string(),
                proc: "p".to_string(),
            };

            let scheduler = SchedulerHandle::from_sender(scheduler_tx);
            let state = StateHandle::from_sender(state_tx);
            tokio::spawn(run_executor(
                process.clone(),
                work_rx,
                scheduler,
                state,
                lua_code,
            ));

            Self {
                scheduler_rx,
                state_rx,
                work_tx,
                process,
                log_seq: 0,
            }
        }

        async fn send_proposal(&mut self, input: &str) -> Proposal {
            let bytes = mp(input);
            let proposal = Proposal {
                process: self.process.clone(),
                event: None,
                input: bytes,
                promise: None,
            };
            self.work_tx.send(proposal.clone()).unwrap();
            proposal
        }

        async fn expect_get_next_event_id(&mut self) -> EventId {
            match self.scheduler_rx.recv().await.unwrap() {
                SchedulerMsg::GetNextEventId { process, resp } => {
                    assert_eq!(process, self.process);
                    let event = EventId {
                        namespace: "test".to_string(),
                        app: "p".to_string(),
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

        fn respond_satisfy(&mut self, resp: oneshot::Sender<Result<NextAction>>, event: EventId) {
            resp.send(Ok(NextAction {
                event,
                proposal: None,
            }))
            .unwrap();
            self.log_seq += 1;
        }

        async fn expect_get_log_seq(&mut self) -> u64 {
            match self.scheduler_rx.recv().await.unwrap() {
                SchedulerMsg::GetLogSeq { process, resp } => {
                    assert_eq!(process, self.process);
                    resp.send(0).unwrap();
                    0
                }
                other => panic!("expected GetLogSeq, got {:?}", other),
            }
        }

        async fn expect_state_get(&mut self) -> (String, oneshot::Sender<Option<String>>) {
            match self.state_rx.recv().await.unwrap() {
                StateMsg::Get {
                    process: _,
                    key,
                    resp,
                } => (key, resp),
                other => panic!("expected StateMsg::Get, got {:?}", other),
            }
        }

        async fn expect_state_set(&mut self) -> (String, String, oneshot::Sender<()>) {
            match self.state_rx.recv().await.unwrap() {
                StateMsg::Set {
                    process: _,
                    key,
                    value,
                    resp,
                } => (key, value, resp),
                other => panic!("expected StateMsg::Set, got {:?}", other),
            }
        }

        async fn drain_to_get_log_seq(&mut self) {
            let mut got_log_seq = false;
            loop {
                tokio::select! {
                                    msg = self.scheduler_rx.recv() => {
                                        match msg {
                                            Some(SchedulerMsg::GetLogSeq { process, resp }) => {
                                                assert_eq!(process, self.process);
                                                let seq = self.log_seq;
                                                resp.send(seq).unwrap();
                                                got_log_seq = true;
                                            }
                                            Some(SchedulerMsg::GetNextEventId { process, resp }) => {
                                                assert_eq!(process, self.process);
                                                resp.send(EventId {
                namespace: "test".to_string(),
                                                app: "p".to_string(),
                                                proc: "p".to_string(),
                                                seq: 0,
                                            }).unwrap();
                                            }
                                            Some(SchedulerMsg::Satisfy { resp, .. }) => {
                                                resp.send(Ok(NextAction {
                                                    event: EventId { namespace: "test".to_string(), app: "p".to_string(), proc: "p".to_string(), seq: 0 },
                                                    proposal: None,
                                                })).unwrap();
                                            }
                                            _ => {}
                                        }
                                    }
                                    msg = self.state_rx.recv() => {
                                        match msg {
                                            Some(StateMsg::Set { resp, .. }) => {
                                                resp.send(()).unwrap();
                                            }
                                            Some(StateMsg::Get { resp, .. }) => {
                                                resp.send(None).unwrap();
                                            }
                                            None => break,
                                        }
                                    }
                                }
                if got_log_seq {
                    break;
                }
            }
        }
    }

    // --- Simple return, no syscalls ---

    #[tokio::test]
    async fn test_simple_return() {
        let mut h = ExecutorHarness::new("return function() return 'hello' end".to_string()).await;

        let _p = h.send_proposal("world").await;
        let event = h.expect_get_next_event_id().await;
        h.drain_to_get_log_seq().await;

        let (receipt, resp) = h.expect_satisfy(true).await;
        assert_eq!(receipt.syscalls, vec![]);
        assert_eq!(receipt.returns, mp("hello"));
        assert_eq!(receipt.in_event_seq, 0);
        assert_eq!(receipt.in_log_seq, 0);
        h.respond_satisfy(resp, event);
    }

    #[tokio::test]
    async fn test_return_nil() {
        let mut h = ExecutorHarness::new("return function() return nil end".to_string()).await;

        h.send_proposal("world").await;
        let event = h.expect_get_next_event_id().await;
        h.drain_to_get_log_seq().await;

        let (receipt, resp) = h.expect_satisfy(true).await;
        assert_eq!(receipt.returns, mp_null());
        h.respond_satisfy(resp, event);
    }

    #[tokio::test]
    async fn test_return_number() {
        let mut h = ExecutorHarness::new("return function() return 42 end".to_string()).await;

        h.send_proposal("world").await;
        let event = h.expect_get_next_event_id().await;
        h.drain_to_get_log_seq().await;

        let (receipt, resp) = h.expect_satisfy(true).await;
        assert_eq!(receipt.returns, rmp_serde::to_vec(&42).unwrap());
        h.respond_satisfy(resp, event);
    }

    // --- KV syscalls ---

    #[tokio::test]
    async fn test_kv_get_then_return() {
        let mut h = ExecutorHarness::new(
            "return function() local v = kv.get('foo'); return v end".to_string(),
        )
        .await;

        h.send_proposal("world").await;
        let event = h.expect_get_next_event_id().await;
        h.drain_to_get_log_seq().await;

        // First: KVRead receipt (intermediate)
        let (rec1, resp1) = h.expect_satisfy(false).await;
        assert_eq!(rec1.in_event_seq, 0);
        assert_eq!(rec1.in_log_seq, 0);
        assert_eq!(rec1.returns, Vec::<u8>::new());
        assert_eq!(
            rec1.syscalls,
            vec![Syscall::KVRead {
                key: "foo".to_string(),
                current_value: String::new(),
            }]
        );
        h.respond_satisfy(resp1, event.clone());

        h.drain_to_get_log_seq().await;

        // Second: final receipt with empty string value (KV state returned "")
        let (rec2, resp2) = h.expect_satisfy(true).await;
        assert_eq!(rec2.in_event_seq, 1);
        assert_eq!(rec2.in_log_seq, 1);
        assert_eq!(rec2.returns, mp(""));
        assert_eq!(rec2.syscalls, vec![]);
        h.respond_satisfy(resp2, event);
    }

    #[tokio::test]
    async fn test_kv_set_then_get_then_return() {
        let mut h = ExecutorHarness::new(
            "return function() kv.set('x', '42'); local v = kv.get('x'); return v end".to_string(),
        )
        .await;

        h.send_proposal("world").await;
        let event = h.expect_get_next_event_id().await;
        h.drain_to_get_log_seq().await;

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
        h.respond_satisfy(resp1, event.clone());

        h.drain_to_get_log_seq().await;

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
        h.respond_satisfy(resp2, event.clone());

        h.drain_to_get_log_seq().await;

        // Final with "42"
        let (rec3, resp3) = h.expect_satisfy(true).await;
        assert_eq!(rec3.in_event_seq, 2);
        assert_eq!(rec3.in_log_seq, 2);
        assert_eq!(rec3.returns, mp("42"));
        assert_eq!(rec3.syscalls, vec![]);
        h.respond_satisfy(resp3, event);
    }

    // --- Non-blocking syscall ---

    #[tokio::test]
    async fn test_http_get_produces_call() {
        let mut h = ExecutorHarness::new(
            "return function() http.get('https://example.com') end".to_string(),
        )
        .await;

        h.send_proposal("world").await;
        let event = h.expect_get_next_event_id().await;
        h.drain_to_get_log_seq().await;

        let (receipt, resp) = h.expect_satisfy(true).await;
        assert_eq!(receipt.in_event_seq, 0);
        assert_eq!(receipt.in_log_seq, 0);
        assert_eq!(receipt.returns, Vec::<u8>::new());

        match &receipt.syscalls[0] {
            Syscall::Call { proposal } => {
                assert_eq!(proposal.process.namespace, "sys");
                assert_eq!(proposal.process.app, "http");
                assert_eq!(proposal.process.proc, "runtime");
                assert_eq!(proposal.input, mp("https://example.com"));
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
        h.respond_satisfy(resp, event);
    }

    #[tokio::test]
    async fn test_kv_then_http_get() {
        let mut h = ExecutorHarness::new(
            "return function() kv.set('x', '1'); http.get('https://x.com') end".to_string(),
        )
        .await;

        h.send_proposal("world").await;
        let event = h.expect_get_next_event_id().await;
        h.drain_to_get_log_seq().await;

        // KVWrite (intermediate)
        let (rec1, resp1) = h.expect_satisfy(false).await;
        assert_eq!(
            rec1.syscalls,
            vec![Syscall::KVWrite {
                key: "x".to_string(),
                new_value: "1".to_string(),
            }]
        );
        h.respond_satisfy(resp1, event.clone());

        h.drain_to_get_log_seq().await;

        // Call (final)
        let (rec2, resp2) = h.expect_satisfy(true).await;
        assert!(
            matches!(&rec2.syscalls[0], Syscall::Call { proposal } if proposal.process.namespace == "sys")
        );
        h.respond_satisfy(resp2, event);
    }

    // --- Error handling ---

    #[tokio::test]
    async fn test_lua_error_does_not_produce_receipt() {
        let mut h = ExecutorHarness::new("return function() error('boom') end".to_string()).await;

        h.send_proposal("world").await;
        let _ = h.expect_get_next_event_id().await;
        h.drain_to_get_log_seq().await;

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
                    namespace: "test".to_string(),
                    app: "p".to_string(),
                    proc: "p".to_string(),
                    seq: 0,
                })
                .unwrap();
            }
        }
    }
}
