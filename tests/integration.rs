use std::time::{Duration, Instant};

use arcanum::executor::ExecutorHandle;
use arcanum::manager::ManagerHandle;
use arcanum::proc::http::HttpHandle;
use arcanum::scheduler::{
    InMemoryScheduler, Proposal, Receipt, RuntimeStatus, SchedulerHandle, Syscall, run_scheduler,
};
use arcanum::state::{InMemoryKVState, StateHandle};
use arcanum::store::{InMemoryPackageStore, StoreHandle};
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
    let (sched_tx, sched_rx) = mpsc::unbounded_channel();
    let scheduler = SchedulerHandle::from_sender(sched_tx);
    let state = StateHandle::new(InMemoryKVState::new());
    let store = StoreHandle::new(Box::new(InMemoryPackageStore::new()));
    let manager = ManagerHandle::new(store, scheduler.clone(), state.clone());
    tokio::spawn(run_scheduler(
        sched_rx,
        Box::new(InMemoryScheduler::new()),
        manager.clone(),
    ));

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
    manager.register_executor(process.clone(), executor.sender());

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
    let (sched_tx, sched_rx) = mpsc::unbounded_channel();
    let scheduler = SchedulerHandle::from_sender(sched_tx);
    let state = StateHandle::new(InMemoryKVState::new());
    let store = StoreHandle::new(Box::new(InMemoryPackageStore::new()));
    let manager = ManagerHandle::new(store, scheduler.clone(), state.clone());
    tokio::spawn(run_scheduler(
        sched_rx,
        Box::new(InMemoryScheduler::new()),
        manager.clone(),
    ));

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
    manager.register_executor(process.clone(), executor.sender());

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
    let (sched_tx, sched_rx) = mpsc::unbounded_channel();
    let scheduler = SchedulerHandle::from_sender(sched_tx);
    let state = StateHandle::new(InMemoryKVState::new());
    let store = StoreHandle::new(Box::new(InMemoryPackageStore::new()));
    let manager = ManagerHandle::new(store, scheduler.clone(), state.clone());
    tokio::spawn(run_scheduler(
        sched_rx,
        Box::new(InMemoryScheduler::new()),
        manager.clone(),
    ));

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
    manager.register_executor(process.clone(), executor.sender());

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
    let (sched_tx, sched_rx) = mpsc::unbounded_channel();
    let scheduler = SchedulerHandle::from_sender(sched_tx);
    let state = StateHandle::new(InMemoryKVState::new());
    let store = StoreHandle::new(Box::new(InMemoryPackageStore::new()));
    let manager = ManagerHandle::new(store, scheduler.clone(), state.clone());
    tokio::spawn(run_scheduler(
        sched_rx,
        Box::new(InMemoryScheduler::new()),
        manager.clone(),
    ));

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
    manager.register_executor(proc_a.clone(), executor_a.sender());

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
    let (sched_tx, sched_rx) = mpsc::unbounded_channel();
    let scheduler = SchedulerHandle::from_sender(sched_tx);
    let state = StateHandle::new(InMemoryKVState::new());
    let store = StoreHandle::new(Box::new(InMemoryPackageStore::new()));
    let manager = ManagerHandle::new(store, scheduler.clone(), state.clone());
    tokio::spawn(run_scheduler(
        sched_rx,
        Box::new(InMemoryScheduler::new()),
        manager.clone(),
    ));

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

    manager.register_executor(proc_a.clone(), executor_a.sender());
    manager.register_executor(proc_b.clone(), executor_b.sender());

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

// --- Test 7: Concurrent proposals to same process ---

#[tokio::test]
async fn test_concurrent_proposals_ordered() {
    let (sched_tx, sched_rx) = mpsc::unbounded_channel();
    let scheduler = SchedulerHandle::from_sender(sched_tx);
    let state = StateHandle::new(InMemoryKVState::new());
    let store = StoreHandle::new(Box::new(InMemoryPackageStore::new()));
    let manager = ManagerHandle::new(store, scheduler.clone(), state.clone());
    tokio::spawn(run_scheduler(
        sched_rx,
        Box::new(InMemoryScheduler::new()),
        manager.clone(),
    ));

    let process = ProcessId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "counter".into(),
    };
    let event1 = EventId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "counter".into(),
        seq: 0,
    };
    let event2 = EventId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "counter".into(),
        seq: 1,
    };

    let executor = ExecutorHandle::new(
        process.clone(),
        scheduler.clone(),
        state.clone(),
        r#"return function(v)
            local prev = kv.get('counter')
            kv.set('counter', v)
            return prev
        end"#
            .to_string(),
    );
    manager.register_executor(process.clone(), executor.sender());

    // Set initial state
    state
        .set(process.clone(), "counter".into(), "0".into())
        .await;

    // Send two proposals back-to-back
    scheduler
        .add_proposal(Proposal {
            process: process.clone(),
            event: None,
            input: mp("first"),
            promise: None,
        })
        .await;
    scheduler
        .add_proposal(Proposal {
            process: process.clone(),
            event: None,
            input: mp("second"),
            promise: None,
        })
        .await;

    // Both events should get 3 chunks: KVRead, KVWrite, End
    let chunks1 = wait_for_chunks(&scheduler, event1, 3, Duration::from_secs(2)).await;
    let chunks2 = wait_for_chunks(&scheduler, event2, 3, Duration::from_secs(2)).await;

    assert_eq!(chunks1.len(), 3);
    assert_eq!(chunks2.len(), 3);

    // --- Event 1 (seq=0): reads initial "0", sets to "first", returns "0" ---

    // Chunk 0: KVRead sees initial state
    assert_eq!(chunks1[0].status, RuntimeStatus::Normal);
    assert_eq!(
        chunks1[0].syscalls,
        vec![Syscall::KVRead {
            key: "counter".into(),
            current_value: "0".into(),
        }]
    );
    assert_eq!(chunks1[0].returns, Vec::<u8>::new());
    assert_eq!(chunks1[0].in_event_seq, 0);

    // Chunk 1: KVWrite sets to "first"
    assert_eq!(chunks1[1].status, RuntimeStatus::Normal);
    assert_eq!(
        chunks1[1].syscalls,
        vec![Syscall::KVWrite {
            key: "counter".into(),
            new_value: "first".into(),
        }]
    );
    assert_eq!(chunks1[1].returns, Vec::<u8>::new());
    assert_eq!(chunks1[1].in_event_seq, 1);

    // Chunk 2: End — returns prev ("0")
    assert_eq!(chunks1[2].status, RuntimeStatus::End);
    assert_eq!(chunks1[2].returns, mp_data("0"));
    assert_eq!(chunks1[2].syscalls, vec![]);
    assert_eq!(chunks1[2].in_event_seq, 2);

    // --- Event 2 (seq=1): reads "first" (set by event 1), sets to "second", returns "first" ---

    // Chunk 0: KVRead sees state after event 1
    assert_eq!(chunks2[0].status, RuntimeStatus::Normal);
    assert_eq!(
        chunks2[0].syscalls,
        vec![Syscall::KVRead {
            key: "counter".into(),
            current_value: "first".into(),
        }]
    );
    assert_eq!(chunks2[0].returns, Vec::<u8>::new());
    assert_eq!(chunks2[0].in_event_seq, 0);

    // Chunk 1: KVWrite sets to "second"
    assert_eq!(chunks2[1].status, RuntimeStatus::Normal);
    assert_eq!(
        chunks2[1].syscalls,
        vec![Syscall::KVWrite {
            key: "counter".into(),
            new_value: "second".into(),
        }]
    );
    assert_eq!(chunks2[1].returns, Vec::<u8>::new());
    assert_eq!(chunks2[1].in_event_seq, 1);

    // Chunk 2: End — returns prev ("first")
    assert_eq!(chunks2[2].status, RuntimeStatus::End);
    assert_eq!(chunks2[2].returns, mp_data("first"));
    assert_eq!(chunks2[2].syscalls, vec![]);
    assert_eq!(chunks2[2].in_event_seq, 2);

    // Both proposals should be popped from schedule
    assert!(scheduler.get_next(process.clone()).await.is_none());
}

// --- Test 6: Runtime satisfy ---

#[tokio::test]
async fn test_runtime_satisfy_resolves_promise() {
    let (sched_tx, sched_rx) = mpsc::unbounded_channel();
    let scheduler = SchedulerHandle::from_sender(sched_tx);
    let state = StateHandle::new(InMemoryKVState::new());
    let store = StoreHandle::new(Box::new(InMemoryPackageStore::new()));
    let manager = ManagerHandle::new(store, scheduler.clone(), state.clone());
    tokio::spawn(run_scheduler(
        sched_rx,
        Box::new(InMemoryScheduler::new()),
        manager.clone(),
    ));

    let caller_proc = ProcessId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "caller".into(),
    };
    let runtime_proc = ProcessId {
        namespace: "sys".into(),
        app: "http".into(),
        proc: "entrypoint".into(),
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
            return call("^sys/http", "https://example.com")
        end"#
            .to_string(),
    );
    manager.register_executor(caller_proc.clone(), executor_caller.sender());

    // Create a runtime channel (simulates an HTTP-like process)
    let (runtime_tx, mut runtime_rx) = mpsc::unbounded_channel();
    manager.register_runtime(runtime_proc.clone(), runtime_tx);

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

// --- Test 8: HTTP client GET ---

#[tokio::test]
async fn test_http_client_get() {
    use axum::{Json, Router, routing::get};

    let app = Router::new().route(
        "/json",
        get(|| async { Json(serde_json::json!({"ok": true, "data": "hello"})) }),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let (sched_tx, sched_rx) = mpsc::unbounded_channel();
    let scheduler = SchedulerHandle::from_sender(sched_tx);
    let state = StateHandle::new(InMemoryKVState::new());
    let store = StoreHandle::new(Box::new(InMemoryPackageStore::new()));
    let manager = ManagerHandle::new(store, scheduler.clone(), state.clone());
    tokio::spawn(run_scheduler(
        sched_rx,
        Box::new(InMemoryScheduler::new()),
        manager.clone(),
    ));

    let http = HttpHandle::new(scheduler.clone());
    let http_process = ProcessId {
        namespace: "sys".into(),
        app: "http".into(),
        proc: "entrypoint".into(),
    };
    manager.register_runtime(http_process.clone(), http.sender());

    let caller_proc = ProcessId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "caller".into(),
    };
    let caller_event = EventId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "caller".into(),
        seq: 0,
    };

    let executor = ExecutorHandle::new(
        caller_proc.clone(),
        scheduler.clone(),
        state.clone(),
        r#"return function(v)
            return call("^sys/http", v)
        end"#
            .to_string(),
    );
    manager.register_executor(caller_proc.clone(), executor.sender());

    let input = msgpack_value(&serde_json::json!({
        "method": "GET",
        "url": format!("http://127.0.0.1:{}/json", addr.port()),
    }));
    scheduler
        .add_proposal(Proposal {
            process: caller_proc.clone(),
            event: None,
            input,
            promise: None,
        })
        .await;

    let chunks = wait_for_chunks(&scheduler, caller_event, 2, Duration::from_secs(5)).await;
    assert_eq!(chunks.len(), 2);

    assert_eq!(chunks[0].status, RuntimeStatus::Normal);
    assert_eq!(chunks[0].in_event_seq, 0);
    assert!(matches!(
        &chunks[0].syscalls[0],
        Syscall::Call { proposal } if proposal.process == http_process
    ));

    assert_eq!(chunks[1].status, RuntimeStatus::End);
    assert_eq!(chunks[1].in_event_seq, 1);
    assert_eq!(chunks[1].syscalls, vec![]);

    let response: serde_json::Value = rmp_serde::from_slice(&chunks[1].returns).unwrap();
    assert_eq!(response["data"]["ok"], true);
    assert_eq!(response["data"]["status"], 200);
    let body: serde_json::Value =
        serde_json::from_str(response["data"]["body"].as_str().unwrap()).unwrap();
    assert_eq!(body["ok"], true);
    assert_eq!(body["data"], "hello");
}

// --- Test 9: HTTP client POST with body ---

#[tokio::test]
async fn test_http_client_post() {
    use axum::{Json, Router, routing::post};

    let app = Router::new().route(
        "/echo",
        post(|body: Json<serde_json::Value>| async { body }),
    );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let (sched_tx, sched_rx) = mpsc::unbounded_channel();
    let scheduler = SchedulerHandle::from_sender(sched_tx);
    let state = StateHandle::new(InMemoryKVState::new());
    let store = StoreHandle::new(Box::new(InMemoryPackageStore::new()));
    let manager = ManagerHandle::new(store, scheduler.clone(), state.clone());
    tokio::spawn(run_scheduler(
        sched_rx,
        Box::new(InMemoryScheduler::new()),
        manager.clone(),
    ));

    let http = HttpHandle::new(scheduler.clone());
    let http_process = ProcessId {
        namespace: "sys".into(),
        app: "http".into(),
        proc: "entrypoint".into(),
    };
    manager.register_runtime(http_process.clone(), http.sender());

    let caller_proc = ProcessId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "caller".into(),
    };
    let caller_event = EventId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "caller".into(),
        seq: 0,
    };

    let executor = ExecutorHandle::new(
        caller_proc.clone(),
        scheduler.clone(),
        state.clone(),
        r#"return function(v)
            return call("^sys/http", v)
        end"#
            .to_string(),
    );
    manager.register_executor(caller_proc.clone(), executor.sender());

    let input = msgpack_value(&serde_json::json!({
        "method": "POST",
        "url": format!("http://127.0.0.1:{}/echo", addr.port()),
        "headers": {"content-type": "application/json"},
        "body": "{\"hello\":\"world\"}",
    }));
    scheduler
        .add_proposal(Proposal {
            process: caller_proc.clone(),
            event: None,
            input,
            promise: None,
        })
        .await;

    let chunks = wait_for_chunks(&scheduler, caller_event, 2, Duration::from_secs(5)).await;
    assert_eq!(chunks.len(), 2);

    let response: serde_json::Value = rmp_serde::from_slice(&chunks[1].returns).unwrap();
    assert_eq!(response["data"]["ok"], true);
    assert_eq!(response["data"]["status"], 200);
    let body: serde_json::Value =
        serde_json::from_str(response["data"]["body"].as_str().unwrap()).unwrap();
    assert_eq!(body["hello"], "world");
}

// --- Test 10: HTTP server routes POST to process ---

#[tokio::test]
async fn test_http_server_routes_to_executor() {
    use arcanum::proc::http_server::HttpServerHandle;

    let (sched_tx, sched_rx) = mpsc::unbounded_channel();
    let scheduler = SchedulerHandle::from_sender(sched_tx);
    let state = StateHandle::new(InMemoryKVState::new());
    let store = StoreHandle::new(Box::new(InMemoryPackageStore::new()));
    let manager = ManagerHandle::new(store, scheduler.clone(), state.clone());
    tokio::spawn(run_scheduler(
        sched_rx,
        Box::new(InMemoryScheduler::new()),
        manager.clone(),
    ));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let _server = HttpServerHandle::from_listener(scheduler.clone(), listener);

    // Register a simple echo executor
    let process = ProcessId {
        namespace: "test".into(),
        app: "echo".into(),
        proc: "entrypoint".into(),
    };

    let executor = ExecutorHandle::new(
        process.clone(),
        scheduler.clone(),
        state.clone(),
        r#"return function(v)
            return "echo: " .. v
        end"#
            .to_string(),
    );
    manager.register_executor(process.clone(), executor.sender());

    // Send HTTP request to the server
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{}/test/echo/entrypoint", port))
        .body("hello")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body_text = resp.text().await.unwrap();
    let body: serde_json::Value = serde_json::from_str(&body_text).unwrap();
    assert_eq!(body["data"], "echo: hello");
}

// --- Test 11: HTTP server error for unknown process ---

#[tokio::test]
async fn test_http_server_unknown_process() {
    use arcanum::proc::http_server::HttpServerHandle;

    let (sched_tx, sched_rx) = mpsc::unbounded_channel();
    let scheduler = SchedulerHandle::from_sender(sched_tx);
    let state = StateHandle::new(InMemoryKVState::new());
    let store = StoreHandle::new(Box::new(InMemoryPackageStore::new()));
    let manager = ManagerHandle::new(store, scheduler.clone(), state.clone());
    tokio::spawn(run_scheduler(
        sched_rx,
        Box::new(InMemoryScheduler::new()),
        manager.clone(),
    ));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let _server = HttpServerHandle::from_listener(scheduler.clone(), listener);

    // No executor registered for the target process

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{}/test/nonexistent/foo", port))
        .body("hello")
        .send()
        .await
        .unwrap();

    // The HTTP server times out since no executor consumes the proposal
    assert_eq!(resp.status(), 504);
}
