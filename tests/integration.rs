use std::io::Write;
use std::time::{Duration, Instant};

use arcanum::manager::ManagerHandle;
use arcanum::proc::http::HttpHandle;
use arcanum::scheduler::{
    InMemoryScheduler, Proposal, Receipt, RuntimeStatus, SchedulerHandle, Syscall, run_scheduler,
};
use arcanum::store::{InMemoryPackageStore, StoreHandle};
use arcanum::types::{EventId, ProcessId};
use tempfile::TempDir;
use tokio::sync::mpsc;

struct TestEnv {
    scheduler: SchedulerHandle,
    store: StoreHandle,
    manager: ManagerHandle,
    _tmpdir: TempDir,
}

fn setup() -> TestEnv {
    let tmpdir = TempDir::new().unwrap();
    let (sched_tx, sched_rx) = mpsc::unbounded_channel();
    let scheduler = SchedulerHandle::from_sender(sched_tx);
    let store = StoreHandle::new(Box::new(InMemoryPackageStore::new()));
    let manager = ManagerHandle::new(
        store.clone(),
        scheduler.clone(),
        tmpdir.path().join("state"),
    );
    tokio::spawn(run_scheduler(
        sched_rx,
        Box::new(InMemoryScheduler::new()),
        manager.clone(),
    ));
    TestEnv {
        scheduler,
        store,
        manager,
        _tmpdir: tmpdir,
    }
}

fn mp(s: &str) -> Vec<u8> {
    rmp_serde::to_vec(&serde_json::Value::String(s.into())).unwrap()
}

fn msgpack_value(value: &serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec(value).unwrap()
}

fn mp_data(s: &str) -> Vec<u8> {
    msgpack_value(&serde_json::json!({"data": s}))
}

fn make_tar_gz(code: &str) -> Vec<u8> {
    let mut tar_bytes = Vec::new();
    {
        let mut ar = tar::Builder::new(&mut tar_bytes);
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_mode(0o644);
        header.set_size(code.len() as u64);
        header.set_path("main.lua").unwrap();
        header.set_cksum();
        ar.append(&header, code.as_bytes()).unwrap();
        ar.finish().unwrap();
    }
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&tar_bytes).unwrap();
    encoder.finish().unwrap()
}

async fn add_package(store: &StoreHandle, namespace: &str, app: &str, code: &str) {
    let tarball = make_tar_gz(code);
    let key = store
        .add_package(tarball.into())
        .await
        .expect("failed to add package");
    let name = format!("^{}/{}", namespace, app);
    store.set_name(name, key);
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
    let env = setup();

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

    add_package(
        &env.store,
        "test",
        "test",
        r#"return { echo = function(ctx, msg) return 'hello' end }"#,
    )
    .await;

    env.scheduler
        .add_proposal(Proposal {
            process: process.clone(),
            event: None,
            input: mp("start"),
            promise: None,
        })
        .await;

    let chunks = wait_for_chunks(&env.scheduler, event, 1, Duration::from_secs(2)).await;
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].status, RuntimeStatus::End);
    assert_eq!(chunks[0].returns, mp_data("hello"));
    assert_eq!(chunks[0].syscalls, vec![]);

    assert!(env.scheduler.get_next(process.clone()).await.is_none());
}

// --- Test 2: KV + SQL combined ---

#[tokio::test]
async fn test_kv_and_sql() {
    let env = setup();

    let process = ProcessId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "kvsqltest".into(),
    };
    let event = EventId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "kvsqltest".into(),
        seq: 0,
    };

    add_package(
        &env.store,
        "test",
        "test",
        r#"return {
            kvsqltest = function(ctx, msg)
                kv.set('mykey', 'myvalue')
                local v = kv.get('mykey')
                sql.exec("CREATE TABLE IF NOT EXISTS t (msg TEXT)")
                sql.exec("INSERT INTO t VALUES (?)", "hello")
                local r = sql.query("SELECT msg FROM t WHERE msg = ?", "hello")
                return {kv = v, sql = r}
            end,
        }"#,
    )
    .await;

    env.scheduler
        .add_proposal(Proposal {
            process: process.clone(),
            event: None,
            input: mp("start"),
            promise: None,
        })
        .await;

    let chunks = wait_for_chunks(&env.scheduler, event, 6, Duration::from_secs(2)).await;
    assert_eq!(chunks.len(), 6);

    // Chunk 0: KVWrite syscall
    assert_eq!(chunks[0].status, RuntimeStatus::Normal);
    assert_eq!(chunks[0].in_event_seq, 0);
    assert_eq!(chunks[0].returns, Vec::<u8>::new());
    assert_eq!(
        chunks[0].syscalls,
        vec![Syscall::KVWrite {
            key: "mykey".to_string(),
            new_value: "myvalue".to_string(),
        }]
    );

    // Chunk 1: KVRead syscall
    assert_eq!(chunks[1].status, RuntimeStatus::Normal);
    assert_eq!(chunks[1].in_event_seq, 1);
    assert_eq!(chunks[1].returns, Vec::<u8>::new());
    assert_eq!(
        chunks[1].syscalls,
        vec![Syscall::KVRead {
            key: "mykey".to_string(),
            current_value: "myvalue".to_string(),
        }]
    );

    // Chunk 2: SqlExec syscall (CREATE TABLE) — no params
    assert_eq!(chunks[2].status, RuntimeStatus::Normal);
    assert_eq!(chunks[2].in_event_seq, 2);
    assert_eq!(chunks[2].returns, Vec::<u8>::new());
    assert_eq!(
        chunks[2].syscalls,
        vec![Syscall::SqlExec {
            sql: "CREATE TABLE IF NOT EXISTS t (msg TEXT)".to_string(),
            params: vec![],
        }]
    );

    // Chunk 3: SqlExec syscall (INSERT) — parameterized
    let hello_params = msgpack_value(&serde_json::json!(["hello"]));
    assert_eq!(chunks[3].status, RuntimeStatus::Normal);
    assert_eq!(chunks[3].in_event_seq, 3);
    assert_eq!(chunks[3].returns, Vec::<u8>::new());
    assert_eq!(
        chunks[3].syscalls,
        vec![Syscall::SqlExec {
            sql: "INSERT INTO t VALUES (?)".to_string(),
            params: hello_params.clone(),
        }]
    );

    // Chunk 4: SqlQuery syscall — parameterized
    assert_eq!(chunks[4].status, RuntimeStatus::Normal);
    assert_eq!(chunks[4].in_event_seq, 4);
    assert_eq!(chunks[4].returns, Vec::<u8>::new());
    assert_eq!(
        chunks[4].syscalls,
        vec![Syscall::SqlQuery {
            sql: "SELECT msg FROM t WHERE msg = ?".to_string(),
            params: hello_params,
        }]
    );

    // Chunk 5: End — final Lua return
    assert_eq!(chunks[5].status, RuntimeStatus::End);
    assert_eq!(chunks[5].in_event_seq, 5);
    assert_eq!(chunks[5].syscalls, vec![]);
    let expected = msgpack_value(&serde_json::json!({
        "data": {
            "kv": "myvalue",
            "sql": {
                "rows": [{"msg": "hello"}],
                "columns": ["msg"]
            }
        }
    }));
    assert_eq!(chunks[5].returns, expected);
}

// --- Test 3: Lua error ---

#[tokio::test]
async fn test_lua_error_satisfies() {
    let env = setup();

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

    add_package(
        &env.store,
        "test",
        "test",
        r#"return { errtest = function(ctx, msg) error('boom') end }"#,
    )
    .await;

    env.scheduler
        .add_proposal(Proposal {
            process: process.clone(),
            event: None,
            input: mp("start"),
            promise: None,
        })
        .await;

    // Error produces a satisfy with RuntimeStatus::Error and proposal is popped
    let chunks = wait_for_chunks(&env.scheduler, event, 1, Duration::from_secs(2)).await;
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].status, RuntimeStatus::Error);
    assert!(String::from_utf8_lossy(&chunks[0].returns).contains("boom"));
    assert_eq!(chunks[0].syscalls, vec![]);

    // Proposal should be popped from schedule
    assert!(env.scheduler.get_next(process.clone()).await.is_none());
}

// --- Test 4: Notify routes to target ---

#[tokio::test]
async fn test_notify_routes_to_target() {
    let env = setup();

    let proc_a = ProcessId {
        namespace: "test".into(),
        app: "a".into(),
        proc: "entrypoint".into(),
    };
    let proc_b = ProcessId {
        namespace: "test".into(),
        app: "b".into(),
        proc: "entrypoint".into(),
    };

    add_package(
        &env.store,
        "test",
        "a",
        r#"return {
            entrypoint = function(ctx, msg)
                notify("^test/b/entrypoint", "hello from a")
                return "done"
            end,
        }"#,
    )
    .await;

    // Do NOT register B — the notification should still land in B's schedule

    env.scheduler
        .add_proposal(Proposal {
            process: proc_a.clone(),
            event: None,
            input: mp("start"),
            promise: None,
        })
        .await;

    // Wait for B to get the notification proposal
    let b_proposal = wait_for_schedule(&env.scheduler, &proc_b, Duration::from_secs(2))
        .await
        .expect("B should have received a notification proposal");

    assert_eq!(b_proposal.process, proc_b);
    assert_eq!(b_proposal.input, mp("hello from a"));
    assert!(b_proposal.promise.is_none());
}

// --- Test 5: Call with promise resolution ---

#[tokio::test]
async fn test_call_with_promise_resolution() {
    let env = setup();

    let proc_a = ProcessId {
        namespace: "test".into(),
        app: "a".into(),
        proc: "entrypoint".into(),
    };
    let proc_b = ProcessId {
        namespace: "test".into(),
        app: "b".into(),
        proc: "entrypoint".into(),
    };
    let event_a = EventId {
        namespace: "test".into(),
        app: "a".into(),
        proc: "entrypoint".into(),
        seq: 0,
    };
    let event_b = EventId {
        namespace: "test".into(),
        app: "b".into(),
        proc: "entrypoint".into(),
        seq: 0,
    };

    add_package(
        &env.store,
        "test",
        "a",
        r#"return {
            entrypoint = function(ctx, msg)
                return call("^test/b/entrypoint", "ping")
            end,
        }"#,
    )
    .await;
    add_package(
        &env.store,
        "test",
        "b",
        r#"return {
            entrypoint = function(ctx, msg)
                return "pong"
            end,
        }"#,
    )
    .await;

    env.scheduler
        .add_proposal(Proposal {
            process: proc_a.clone(),
            event: None,
            input: mp("start"),
            promise: None,
        })
        .await;

    // A should end up with 2 chunks: [Call syscall, End("pong")]
    let chunks_a = wait_for_chunks(&env.scheduler, event_a, 2, Duration::from_secs(2)).await;
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
    let chunks_b = wait_for_chunks(&env.scheduler, event_b, 1, Duration::from_secs(2)).await;
    assert_eq!(chunks_b.len(), 1);
    assert_eq!(chunks_b[0].status, RuntimeStatus::End);
    assert_eq!(chunks_b[0].returns, mp_data("pong"));
    assert_eq!(chunks_b[0].syscalls, vec![]);
}

// --- Test 7: Concurrent proposals to same process ---

#[tokio::test]
async fn test_concurrent_proposals_ordered() {
    let env = setup();

    let process = ProcessId {
        namespace: "test".into(),
        app: "counter".into(),
        proc: "entrypoint".into(),
    };
    let event1 = EventId {
        namespace: "test".into(),
        app: "counter".into(),
        proc: "entrypoint".into(),
        seq: 0,
    };
    let event2 = EventId {
        namespace: "test".into(),
        app: "counter".into(),
        proc: "entrypoint".into(),
        seq: 1,
    };

    add_package(
        &env.store,
        "test",
        "counter",
        r#"return {
            entrypoint = function(ctx, msg)
                local prev = kv.get('counter')
                kv.set('counter', msg)
                return prev
            end,
        }"#,
    )
    .await;

    // Set initial state via per-process state actor
    env.manager
        .get_state_handle(process.clone())
        .await
        .set(process.clone(), "counter".into(), "0".into())
        .await;

    // Send two proposals back-to-back
    env.scheduler
        .add_proposal(Proposal {
            process: process.clone(),
            event: None,
            input: mp("first"),
            promise: None,
        })
        .await;
    env.scheduler
        .add_proposal(Proposal {
            process: process.clone(),
            event: None,
            input: mp("second"),
            promise: None,
        })
        .await;

    // Both events should get 3 chunks: KVRead, KVWrite, End
    let chunks1 = wait_for_chunks(&env.scheduler, event1, 3, Duration::from_secs(2)).await;
    let chunks2 = wait_for_chunks(&env.scheduler, event2, 3, Duration::from_secs(2)).await;

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
    assert!(env.scheduler.get_next(process.clone()).await.is_none());
}

// --- Test 6: Stateless satisfy ---

#[tokio::test]
async fn test_stateless_satisfy_resolves_promise() {
    let env = setup();

    let caller_proc = ProcessId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "caller".into(),
    };
    let stateless_proc = ProcessId {
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

    add_package(
        &env.store,
        "test",
        "test",
        r#"return {
            caller = function(ctx, msg)
                return call("^sys/http", msg)
            end,
        }"#,
    )
    .await;

    // Create a stateless channel (simulates an HTTP-like process)
    let (stateless_tx, mut stateless_rx) = mpsc::unbounded_channel();
    env.manager
        .register_stateless(stateless_proc.clone(), stateless_tx);

    // Send proposal to caller
    env.scheduler
        .add_proposal(Proposal {
            process: caller_proc.clone(),
            event: None,
            input: mp("start"),
            promise: None,
        })
        .await;

    // Wait for the stateless process to receive the call
    let stateless_call = tokio::time::timeout(Duration::from_secs(2), stateless_rx.recv())
        .await
        .expect("timeout")
        .expect("stateless channel closed");
    assert_eq!(stateless_call.proposal.input, mp("start"));

    // Stateless process responds via stateless_satisfy — must wrap in { data = ... }
    let response = format!("fetched: https://example.com");
    let wrapped_response = msgpack_value(&serde_json::json!({"data": response}));
    env.scheduler
        .stateless_satisfy(stateless_call.proposal, wrapped_response.clone())
        .await
        .unwrap();

    // Caller's promise should resolve: second chunk with the response
    let chunks = wait_for_chunks(&env.scheduler, caller_event, 2, Duration::from_secs(2)).await;
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].status, RuntimeStatus::Normal);
    assert_eq!(chunks[0].returns, Vec::<u8>::new());
    assert!(matches!(
        &chunks[0].syscalls[0],
        Syscall::Call { proposal } if proposal.process == stateless_proc
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

    let env = setup();

    let http = HttpHandle::new(env.scheduler.clone());
    let http_process = ProcessId {
        namespace: "sys".into(),
        app: "http".into(),
        proc: "entrypoint".into(),
    };
    env.manager
        .register_stateless(http_process.clone(), http.sender());

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

    add_package(
        &env.store,
        "test",
        "test",
        r#"return {
            caller = function(ctx, msg)
                return call("^sys/http", msg)
            end,
        }"#,
    )
    .await;

    let input = msgpack_value(&serde_json::json!({
        "method": "GET",
        "url": format!("http://127.0.0.1:{}/json", addr.port()),
    }));
    env.scheduler
        .add_proposal(Proposal {
            process: caller_proc.clone(),
            event: None,
            input,
            promise: None,
        })
        .await;

    let chunks = wait_for_chunks(&env.scheduler, caller_event, 2, Duration::from_secs(5)).await;
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

    let env = setup();

    let http = HttpHandle::new(env.scheduler.clone());
    let http_process = ProcessId {
        namespace: "sys".into(),
        app: "http".into(),
        proc: "entrypoint".into(),
    };
    env.manager
        .register_stateless(http_process.clone(), http.sender());

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

    add_package(
        &env.store,
        "test",
        "test",
        r#"return {
            caller = function(ctx, msg)
                return call("^sys/http", msg)
            end,
        }"#,
    )
    .await;

    let input = msgpack_value(&serde_json::json!({
        "method": "POST",
        "url": format!("http://127.0.0.1:{}/echo", addr.port()),
        "headers": {"content-type": "application/json"},
        "body": "{\"hello\":\"world\"}",
    }));
    env.scheduler
        .add_proposal(Proposal {
            process: caller_proc.clone(),
            event: None,
            input,
            promise: None,
        })
        .await;

    let chunks = wait_for_chunks(&env.scheduler, caller_event, 2, Duration::from_secs(5)).await;
    assert_eq!(chunks.len(), 2);

    let response: serde_json::Value = rmp_serde::from_slice(&chunks[1].returns).unwrap();
    assert_eq!(response["data"]["ok"], true);
    assert_eq!(response["data"]["status"], 200);
    let body: serde_json::Value =
        serde_json::from_str(response["data"]["body"].as_str().unwrap()).unwrap();
    assert_eq!(body["hello"], "world");
}

// --- Test 10: HTTP server routes by Host header to executor ---

#[tokio::test]
async fn test_http_server_routes_by_host_header() {
    use arcanum::proc::http_server::HttpServerHandle;

    let env = setup();
    let server = HttpServerHandle::new(env.scheduler.clone(), 0).await;
    let port = server.port;
    let http_server_proc = ProcessId {
        namespace: "sys".into(),
        app: "http-server".into(),
        proc: "entrypoint".into(),
    };
    env.manager
        .register_stateless(http_server_proc.clone(), server.sender());

    add_package(
        &env.store,
        "test",
        "echo",
        r#"return {
            entrypoint = function(ctx, msg)
                return "echo: " .. msg
            end,
        }"#,
    )
    .await;

    // Register host route via the http-server stateless handler
    let http_server_proc = ProcessId {
        namespace: "sys".into(),
        app: "http-server".into(),
        proc: "entrypoint".into(),
    };

    env.scheduler
        .add_proposal(Proposal {
            process: http_server_proc.clone(),
            event: None,
            input: msgpack_value(&serde_json::json!({
                "action": "add",
                "app": "test/echo",
                "host": "example.com",
            })),
            promise: None,
        })
        .await;

    wait_for_empty_schedule(&env.scheduler, &http_server_proc, Duration::from_secs(2)).await;

    // Send HTTP request with Host header
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{}/any/path", port))
        .header("Host", "example.com")
        .body("hello")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(body["data"], "echo: hello");
}

// --- Test 11: HTTP server returns 404 for unregistered host ---

#[tokio::test]
async fn test_http_server_unknown_host() {
    use arcanum::proc::http_server::HttpServerHandle;

    let env = setup();
    let server = HttpServerHandle::new(env.scheduler.clone(), 0).await;
    let port = server.port;
    let http_server_proc = ProcessId {
        namespace: "sys".into(),
        app: "http-server".into(),
        proc: "entrypoint".into(),
    };
    env.manager
        .register_stateless(http_server_proc, server.sender());

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{}/any/path", port))
        .header("Host", "unknown.example.com")
        .body("hello")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 404);
}
