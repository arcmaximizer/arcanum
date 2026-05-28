use std::time::{Duration, Instant};

use arcanum::executor::ExecutorHandle;
use arcanum::scheduler::{
    InMemoryScheduler, Proposal, Receipt, RuntimeStatus, SchedulerHandle, Syscall,
};
use arcanum::state::{InMemoryKVState, StateHandle};
use arcanum::types::{EventId, ProcessId};
use tokio::sync::mpsc;

fn mp(s: &str) -> Vec<u8> {
    rmp_serde::to_vec(&serde_json::Value::String(s.into())).unwrap()
}

fn mp_null() -> Vec<u8> {
    rmp_serde::to_vec(&serde_json::Value::Null).unwrap()
}

fn msgpack_value(value: &serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec(value).unwrap()
}

fn mp_data(s: &str) -> Vec<u8> {
    msgpack_value(&serde_json::json!({"data": s}))
}

async fn wait_for_chunks(
    scheduler: &SchedulerHandle,
    event: EventId,
    min_count: usize,
    timeout: Duration,
) -> Vec<Receipt> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Some(chunks) = scheduler.get_chunks(event.clone()).await {
            if chunks.len() >= min_count {
                return chunks;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let actual = scheduler.get_chunks(event.clone()).await;
    panic!(
        "timed out waiting for {} chunks, got {:?}",
        min_count,
        actual.map(|c| c.len())
    );
}

async fn wait_for_schedule(
    scheduler: &SchedulerHandle,
    process: &ProcessId,
    timeout: Duration,
) -> Option<Proposal> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        let next = scheduler.get_next(process.clone()).await;
        if next.is_some() {
            return next;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    scheduler.get_next(process.clone()).await
}

async fn wait_for_empty_schedule(
    scheduler: &SchedulerHandle,
    process: &ProcessId,
    timeout: Duration,
) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if scheduler.get_next(process.clone()).await.is_none() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for empty schedule for {}", process);
}

// --- Test 1: Basic return ---

#[tokio::test]
async fn test_basic_return() {
    let scheduler = SchedulerHandle::new(Box::new(InMemoryScheduler::new()));
    let state = StateHandle::new(InMemoryKVState::new());

    let process = ProcessId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "echo".into(),
    };
    let event = EventId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "echo".into(),
        seq: 0,
    };

    let executor = ExecutorHandle::new(
        process.clone(),
        scheduler.clone(),
        state.clone(),
        r#"return function() return 'hello' end"#.to_string(),
    );
    scheduler.register_executor(process.clone(), executor.sender());

    scheduler
        .add_proposal(Proposal {
            process: process.clone(),
            event: None,
            input: mp("start"),
            promise: None,
        })
        .await;

    let chunks = wait_for_chunks(&scheduler, event, 1, Duration::from_secs(2)).await;
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].status, RuntimeStatus::End);
    assert_eq!(chunks[0].returns, mp_data("hello"));
    assert_eq!(chunks[0].syscalls, vec![]);

    // Proposal should be popped from schedule
    assert!(scheduler.get_next(process.clone()).await.is_none());
}

// --- Test 2: KV get ---

#[tokio::test]
async fn test_kv_get() {
    let scheduler = SchedulerHandle::new(Box::new(InMemoryScheduler::new()));
    let state = StateHandle::new(InMemoryKVState::new());

    let process = ProcessId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "kvtest".into(),
    };
    let event = EventId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "kvtest".into(),
        seq: 0,
    };

    let executor = ExecutorHandle::new(
        process.clone(),
        scheduler.clone(),
        state.clone(),
        r#"return function()
            local v = kv.get('mykey')
            return v
        end"#
            .to_string(),
    );
    scheduler.register_executor(process.clone(), executor.sender());

    scheduler
        .add_proposal(Proposal {
            process: process.clone(),
            event: None,
            input: mp("start"),
            promise: None,
        })
        .await;

    let chunks = wait_for_chunks(&scheduler, event, 2, Duration::from_secs(2)).await;
    assert_eq!(chunks.len(), 2);

    // First: KVRead syscall (intermediate)
    assert_eq!(chunks[0].status, RuntimeStatus::Normal);
    assert_eq!(chunks[0].returns, Vec::<u8>::new());
    assert_eq!(
        chunks[0].syscalls,
        vec![Syscall::KVRead {
            key: "mykey".to_string(),
            current_value: String::new(),
        }]
    );

    // Second: return "" (empty kv_state cache)
    assert_eq!(chunks[1].status, RuntimeStatus::End);
    assert_eq!(chunks[1].returns, mp_data(""));
    assert_eq!(chunks[1].syscalls, vec![]);
}

// --- Test 3: Lua error ---

#[tokio::test]
async fn test_lua_error_satisfies() {
    let scheduler = SchedulerHandle::new(Box::new(InMemoryScheduler::new()));
    let state = StateHandle::new(InMemoryKVState::new());

    let process = ProcessId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "errtest".into(),
    };
    let event = EventId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "errtest".into(),
        seq: 0,
    };

    let executor = ExecutorHandle::new(
        process.clone(),
        scheduler.clone(),
        state.clone(),
        r#"return function() error('boom') end"#.to_string(),
    );
    scheduler.register_executor(process.clone(), executor.sender());

    scheduler
        .add_proposal(Proposal {
            process: process.clone(),
            event: None,
            input: mp("start"),
            promise: None,
        })
        .await;

    // Error produces a satisfy with RuntimeStatus::Error and proposal is popped
    let chunks = wait_for_chunks(&scheduler, event, 1, Duration::from_secs(2)).await;
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].status, RuntimeStatus::Error);
    assert!(String::from_utf8_lossy(&chunks[0].returns).contains("boom"));
    assert_eq!(chunks[0].syscalls, vec![]);

    // Proposal should be popped from schedule
    assert!(scheduler.get_next(process.clone()).await.is_none());
}

// --- Test 4: Notify routes to target ---

#[tokio::test]
async fn test_notify_routes_to_target() {
    let scheduler = SchedulerHandle::new(Box::new(InMemoryScheduler::new()));
    let state = StateHandle::new(InMemoryKVState::new());

    let proc_a = ProcessId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "a".into(),
    };
    let proc_b = ProcessId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "b".into(),
    };

    let executor_a = ExecutorHandle::new(
        proc_a.clone(),
        scheduler.clone(),
        state.clone(),
        r#"return function()
            notify("^test/test/b", "hello from a")
            return "done"
        end"#
            .to_string(),
    );
    scheduler.register_executor(proc_a.clone(), executor_a.sender());

    // Do NOT register B — the notification should still land in B's schedule

    scheduler
        .add_proposal(Proposal {
            process: proc_a.clone(),
            event: None,
            input: mp("start"),
            promise: None,
        })
        .await;

    // Wait for B to get the notification proposal
    let b_proposal = wait_for_schedule(&scheduler, &proc_b, Duration::from_secs(2))
        .await
        .expect("B should have received a notification proposal");

    assert_eq!(b_proposal.process, proc_b);
    assert_eq!(b_proposal.input, mp("hello from a"));
    assert!(b_proposal.promise.is_none());
}

// --- Test 5: Call with promise resolution ---

#[tokio::test]
async fn test_call_with_promise_resolution() {
    let scheduler = SchedulerHandle::new(Box::new(InMemoryScheduler::new()));
    let state = StateHandle::new(InMemoryKVState::new());

    let proc_a = ProcessId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "a".into(),
    };
    let proc_b = ProcessId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "b".into(),
    };
    let event_a = EventId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "a".into(),
        seq: 0,
    };
    let event_b = EventId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "b".into(),
        seq: 0,
    };

    let executor_a = ExecutorHandle::new(
        proc_a.clone(),
        scheduler.clone(),
        state.clone(),
        r#"return function(v)
            return call("^test/test/b", "ping")
        end"#
            .to_string(),
    );
    let executor_b = ExecutorHandle::new(
        proc_b.clone(),
        scheduler.clone(),
        state.clone(),
        r#"return function(v)
            return "pong"
        end"#
            .to_string(),
    );

    scheduler.register_executor(proc_a.clone(), executor_a.sender());
    scheduler.register_executor(proc_b.clone(), executor_b.sender());

    scheduler
        .add_proposal(Proposal {
            process: proc_a.clone(),
            event: None,
            input: mp("start"),
            promise: None,
        })
        .await;

    // A should end up with 2 chunks: [Call syscall, End("pong")]
    let chunks_a = wait_for_chunks(&scheduler, event_a, 2, Duration::from_secs(2)).await;
    assert_eq!(chunks_a.len(), 2);

    // First chunk: Call syscall
    assert_eq!(chunks_a[0].status, RuntimeStatus::Normal);
    assert_eq!(chunks_a[0].returns, Vec::<u8>::new());
    assert_eq!(chunks_a[0].syscalls.len(), 1);
    assert!(matches!(
        &chunks_a[0].syscalls[0],
        Syscall::Call { proposal } if proposal.process == proc_b
    ));

    // Second chunk: End with B's return value (wrapped in { data = ... })
    assert_eq!(chunks_a[1].status, RuntimeStatus::End);
    assert_eq!(chunks_a[1].returns, mp_data("pong"));
    assert_eq!(chunks_a[1].syscalls, vec![]);

    // B should have 1 chunk: End("pong")
    let chunks_b = wait_for_chunks(&scheduler, event_b, 1, Duration::from_secs(2)).await;
    assert_eq!(chunks_b.len(), 1);
    assert_eq!(chunks_b[0].status, RuntimeStatus::End);
    assert_eq!(chunks_b[0].returns, mp_data("pong"));
    assert_eq!(chunks_b[0].syscalls, vec![]);
}

// --- Test 6: Runtime satisfy ---

#[tokio::test]
async fn test_runtime_satisfy_resolves_promise() {
    let scheduler = SchedulerHandle::new(Box::new(InMemoryScheduler::new()));
    let state = StateHandle::new(InMemoryKVState::new());

    let caller_proc = ProcessId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "caller".into(),
    };
    let runtime_proc = ProcessId {
        namespace: "sys".into(),
        app: "http".into(),
        proc: "runtime".into(),
    };
    let caller_event = EventId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "caller".into(),
        seq: 0,
    };

    let executor_caller = ExecutorHandle::new(
        caller_proc.clone(),
        scheduler.clone(),
        state.clone(),
        r#"return function(v)
            return call("^sys/http/runtime", "https://example.com")
        end"#
            .to_string(),
    );
    scheduler.register_executor(caller_proc.clone(), executor_caller.sender());

    // Create a runtime channel (simulates an HTTP-like process)
    let (runtime_tx, mut runtime_rx) = mpsc::unbounded_channel();
    scheduler.register_runtime(runtime_proc.clone(), runtime_tx);

    // Send proposal to caller
    scheduler
        .add_proposal(Proposal {
            process: caller_proc.clone(),
            event: None,
            input: mp("start"),
            promise: None,
        })
        .await;

    // Wait for the runtime to receive the call
    let runtime_call = tokio::time::timeout(Duration::from_secs(2), runtime_rx.recv())
        .await
        .expect("timeout")
        .expect("runtime channel closed");
    assert_eq!(runtime_call.proposal.input, mp("https://example.com"));

    // Runtime responds via runtime_satisfy — must wrap in { data = ... }
    let response = format!("fetched: https://example.com");
    let wrapped_response = msgpack_value(&serde_json::json!({"data": response}));
    scheduler
        .runtime_satisfy(runtime_call.proposal, wrapped_response.clone())
        .await
        .unwrap();

    // Caller's promise should resolve: second chunk with the response
    let chunks = wait_for_chunks(&scheduler, caller_event, 2, Duration::from_secs(2)).await;
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].status, RuntimeStatus::Normal);
    assert_eq!(chunks[0].returns, Vec::<u8>::new());
    assert!(matches!(
        &chunks[0].syscalls[0],
        Syscall::Call { proposal } if proposal.process == runtime_proc
    ));
    assert_eq!(chunks[1].status, RuntimeStatus::End);
    assert_eq!(chunks[1].returns, wrapped_response);
    assert_eq!(chunks[1].syscalls, vec![]);
}
