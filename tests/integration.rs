use std::io::Write;
use std::time::{Duration, Instant};

use arcanum::in_memory::InMemoryPackageStore;
use arcanum::manager::ManagerHandle;
use arcanum::mgmt::MgmtHandle;
use arcanum::proc::http::HttpHandle;
use arcanum::scheduler::{
    InMemoryScheduler, PersistentScheduler, Proposal, Receipt, RuntimeStatus, SchedulerHandle,
    Syscall, run_scheduler,
};
use arcanum::store::{FileSystemPackageStore, PackageStore, StoreHandle};
use arcanum::types::{AppId, EventId, HandlerId, ProcessId};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
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

fn make_tar_gz_with_files(files: &[(&str, &[u8])]) -> Vec<u8> {
    let mut tar_bytes = Vec::new();
    {
        let mut ar = tar::Builder::new(&mut tar_bytes);
        for (path, content) in files {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Regular);
            header.set_mode(0o644);
            header.set_size(content.len() as u64);
            header.set_path(path).unwrap();
            header.set_cksum();
            ar.append(&header, *content).unwrap();
        }
        ar.finish().unwrap();
    }
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&tar_bytes).unwrap();
    encoder.finish().unwrap()
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

    env.manager
        .register_process(
            process.clone(),
            HandlerId {
                namespace: "test".into(),
                app: "test".into(),
                handler: "echo".into(),
            },
        )
        .await
        .unwrap();

    env.scheduler
        .add_proposal(Proposal {
            process: process.clone(),
            event: None,
            input: mp("start"),
            promise: None,
            from: ProcessId {
                namespace: String::new(),
                app: String::new(),
                proc: String::new(),
            },
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

    env.manager
        .register_process(
            process.clone(),
            HandlerId {
                namespace: "test".into(),
                app: "test".into(),
                handler: "kvsqltest".into(),
            },
        )
        .await
        .unwrap();

    env.scheduler
        .add_proposal(Proposal {
            process: process.clone(),
            event: None,
            input: mp("start"),
            promise: None,
            from: ProcessId {
                namespace: String::new(),
                app: String::new(),
                proc: String::new(),
            },
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

// --- Test 2b: Unknown syscall produces error instead of panic ---

#[tokio::test]
async fn test_unknown_syscall_is_error() {
    let env = setup();

    let process = ProcessId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "badcall".into(),
    };
    let event = EventId {
        namespace: "test".into(),
        app: "test".into(),
        proc: "badcall".into(),
        seq: 0,
    };

    add_package(
        &env.store,
        "test",
        "test",
        r#"return { badcall = function(ctx, msg) raw_syscall("bogus") end }"#,
    )
    .await;

    env.manager
        .register_process(
            process.clone(),
            HandlerId {
                namespace: "test".into(),
                app: "test".into(),
                handler: "badcall".into(),
            },
        )
        .await
        .unwrap();

    env.scheduler
        .add_proposal(Proposal {
            process: process.clone(),
            event: None,
            input: mp("start"),
            promise: None,
            from: ProcessId {
                namespace: String::new(),
                app: String::new(),
                proc: String::new(),
            },
        })
        .await;

    // Should get an error receipt, not a panic
    let chunks = wait_for_chunks(&env.scheduler, event, 1, Duration::from_secs(2)).await;
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].status, RuntimeStatus::Error);
    assert!(String::from_utf8_lossy(&chunks[0].returns).contains("Unknown syscall type"));
    assert_eq!(chunks[0].syscalls, vec![]);

    assert!(env.scheduler.get_next(process.clone()).await.is_none());
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

    env.manager
        .register_process(
            process.clone(),
            HandlerId {
                namespace: "test".into(),
                app: "test".into(),
                handler: "errtest".into(),
            },
        )
        .await
        .unwrap();

    env.scheduler
        .add_proposal(Proposal {
            process: process.clone(),
            event: None,
            input: mp("start"),
            promise: None,
            from: ProcessId {
                namespace: String::new(),
                app: String::new(),
                proc: String::new(),
            },
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
            from: ProcessId {
                namespace: String::new(),
                app: String::new(),
                proc: String::new(),
            },
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
            from: ProcessId {
                namespace: String::new(),
                app: String::new(),
                proc: String::new(),
            },
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
    let event_init = EventId {
        namespace: "test".into(),
        app: "counter".into(),
        proc: "entrypoint".into(),
        seq: 1,
    };
    let event2 = EventId {
        namespace: "test".into(),
        app: "counter".into(),
        proc: "entrypoint".into(),
        seq: 2,
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
            from: ProcessId {
                namespace: String::new(),
                app: String::new(),
                proc: String::new(),
            },
        })
        .await;
    env.scheduler
        .add_proposal(Proposal {
            process: process.clone(),
            event: None,
            input: mp("second"),
            promise: None,
            from: ProcessId {
                namespace: String::new(),
                app: String::new(),
                proc: String::new(),
            },
        })
        .await;

    // Each event gets 3 chunks: KVRead, KVWrite, End
    let chunks1 = wait_for_chunks(&env.scheduler, event1, 3, Duration::from_secs(2)).await;
    let chunks_init = wait_for_chunks(&env.scheduler, event_init, 3, Duration::from_secs(2)).await;
    let chunks2 = wait_for_chunks(&env.scheduler, event2, 3, Duration::from_secs(2)).await;

    assert_eq!(chunks1.len(), 3);
    assert_eq!(chunks_init.len(), 3);
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

    // --- Init notification (seq=1): reads "first", sets to nil (""), returns "first" ---

    assert_eq!(chunks_init[0].status, RuntimeStatus::Normal);
    assert_eq!(
        chunks_init[0].syscalls,
        vec![Syscall::KVRead {
            key: "counter".into(),
            current_value: "first".into(),
        }]
    );
    assert_eq!(chunks_init[0].returns, Vec::<u8>::new());
    assert_eq!(chunks_init[0].in_event_seq, 0);

    assert_eq!(chunks_init[1].status, RuntimeStatus::Normal);
    assert_eq!(
        chunks_init[1].syscalls,
        vec![Syscall::KVWrite {
            key: "counter".into(),
            new_value: "".into(),
        }]
    );
    assert_eq!(chunks_init[1].returns, Vec::<u8>::new());
    assert_eq!(chunks_init[1].in_event_seq, 1);

    assert_eq!(chunks_init[2].status, RuntimeStatus::End);
    assert_eq!(chunks_init[2].returns, mp_data("first"));
    assert_eq!(chunks_init[2].syscalls, vec![]);
    assert_eq!(chunks_init[2].in_event_seq, 2);

    // --- Event 2 (seq=2): reads "" (set by init), sets to "second", returns "" ---

    assert_eq!(chunks2[0].status, RuntimeStatus::Normal);
    assert_eq!(
        chunks2[0].syscalls,
        vec![Syscall::KVRead {
            key: "counter".into(),
            current_value: "".into(),
        }]
    );
    assert_eq!(chunks2[0].returns, Vec::<u8>::new());
    assert_eq!(chunks2[0].in_event_seq, 0);

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

    assert_eq!(chunks2[2].status, RuntimeStatus::End);
    assert_eq!(chunks2[2].returns, mp_data(""));
    assert_eq!(chunks2[2].syscalls, vec![]);
    assert_eq!(chunks2[2].in_event_seq, 2);

    // All proposals should be popped from schedule
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
                if msg == nil then return nil end
                return call("^sys/http", msg)
            end,
        }"#,
    )
    .await;

    env.manager
        .register_process(
            caller_proc.clone(),
            HandlerId {
                namespace: "test".into(),
                app: "test".into(),
                handler: "caller".into(),
            },
        )
        .await
        .unwrap();

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
            from: ProcessId {
                namespace: String::new(),
                app: String::new(),
                proc: String::new(),
            },
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
                if msg == nil then return nil end
                return call("^sys/http", msg)
            end,
        }"#,
    )
    .await;

    env.manager
        .register_process(
            caller_proc.clone(),
            HandlerId {
                namespace: "test".into(),
                app: "test".into(),
                handler: "caller".into(),
            },
        )
        .await
        .unwrap();

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
            from: ProcessId {
                namespace: String::new(),
                app: String::new(),
                proc: String::new(),
            },
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
                if msg == nil then return nil end
                return call("^sys/http", msg)
            end,
        }"#,
    )
    .await;

    env.manager
        .register_process(
            caller_proc.clone(),
            HandlerId {
                namespace: "test".into(),
                app: "test".into(),
                handler: "caller".into(),
            },
        )
        .await
        .unwrap();

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
            from: ProcessId {
                namespace: String::new(),
                app: String::new(),
                proc: String::new(),
            },
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
    assert_eq!(body["hello"], "world");
}

// --- Test 10: HTTP server routes by Host header to executor ---

#[tokio::test]
async fn test_http_server_routes_by_host_header() {
    use arcanum::proc::http_server::HttpServerHandle;

    let env = setup();
    let http_server_proc = ProcessId {
        namespace: "sys".into(),
        app: "http-server".into(),
        proc: "entrypoint".into(),
    };
    let server = HttpServerHandle::new(env.scheduler.clone(), 0, http_server_proc.clone()).await;
    let port = server.port;
    env.manager
        .register_stateless(http_server_proc.clone(), server.sender());

    add_package(
        &env.store,
        "test",
        "echo",
        r#"return {
            entrypoint = function(ctx, msg)
                return "echo: " .. msg.body
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
            from: ProcessId {
                namespace: String::new(),
                app: String::new(),
                proc: String::new(),
            },
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

// --- Test 11a: HTTP server full response (status, headers, body) ---

#[tokio::test]
async fn test_http_server_full_response() {
    use arcanum::proc::http_server::HttpServerHandle;

    let env = setup();
    let http_server_proc = ProcessId {
        namespace: "sys".into(),
        app: "http-server".into(),
        proc: "entrypoint".into(),
    };
    let server = HttpServerHandle::new(env.scheduler.clone(), 0, http_server_proc.clone()).await;
    let port = server.port;
    env.manager
        .register_stateless(http_server_proc.clone(), server.sender());

    add_package(
        &env.store,
        "test",
        "fullresp",
        r#"return {
            entrypoint = function(ctx, msg)
                return {
                    status = 201,
                    headers = {["X-Custom"] = "hello"},
                    body = {created = true, id = msg.body},
                }
            end,
        }"#,
    )
    .await;

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
                "app": "test/fullresp",
                "host": "full.example.com",
            })),
            promise: None,
            from: ProcessId {
                namespace: String::new(),
                app: String::new(),
                proc: String::new(),
            },
        })
        .await;

    wait_for_empty_schedule(&env.scheduler, &http_server_proc, Duration::from_secs(2)).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{}/create-thing", port))
        .header("Host", "full.example.com")
        .body("thing-1")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 201);
    assert_eq!(
        resp.headers().get("x-custom").unwrap().to_str().unwrap(),
        "hello"
    );
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(body["data"]["created"], true);
    assert_eq!(body["data"]["id"], "thing-1");
}

// --- Test 11b: HTTP server full response with string body ---

#[tokio::test]
async fn test_http_server_full_response_string_body() {
    use arcanum::proc::http_server::HttpServerHandle;

    let env = setup();
    let http_server_proc = ProcessId {
        namespace: "sys".into(),
        app: "http-server".into(),
        proc: "entrypoint".into(),
    };
    let server = HttpServerHandle::new(env.scheduler.clone(), 0, http_server_proc.clone()).await;
    let port = server.port;
    env.manager
        .register_stateless(http_server_proc.clone(), server.sender());

    add_package(
        &env.store,
        "test",
        "htmlresp",
        r#"return {
            entrypoint = function(ctx, msg)
                return {
                    status = 200,
                    headers = {["Content-Type"] = "text/html"},
                    body = "<h1>Hello</h1>",
                }
            end,
        }"#,
    )
    .await;

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
                "app": "test/htmlresp",
                "host": "html.example.com",
            })),
            promise: None,
            from: ProcessId {
                namespace: String::new(),
                app: String::new(),
                proc: String::new(),
            },
        })
        .await;

    wait_for_empty_schedule(&env.scheduler, &http_server_proc, Duration::from_secs(2)).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://127.0.0.1:{}/", port))
        .header("Host", "html.example.com")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "text/html"
    );
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(body["data"], "<h1>Hello</h1>");
}

// --- Test 11: HTTP server returns 404 for unregistered host ---

#[tokio::test]
async fn test_http_server_unknown_host() {
    use arcanum::proc::http_server::HttpServerHandle;

    let env = setup();
    let http_server_proc = ProcessId {
        namespace: "sys".into(),
        app: "http-server".into(),
        proc: "entrypoint".into(),
    };
    let server = HttpServerHandle::new(env.scheduler.clone(), 0, http_server_proc.clone()).await;
    let port = server.port;
    env.manager
        .register_stateless(http_server_proc.clone(), server.sender());

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{}/some-path", port))
        .header("host", "example.com")
        .body("test")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

// --- Test 12: Spawn sends blank init notification ---

#[tokio::test]
async fn test_spawn_sends_init_notification() {
    let env = setup();

    let process = ProcessId {
        namespace: "test".into(),
        app: "init-test".into(),
        proc: "entrypoint".into(),
    };
    let event = EventId {
        namespace: "test".into(),
        app: "init-test".into(),
        proc: "entrypoint".into(),
        seq: 0,
    };

    add_package(
        &env.store,
        "test",
        "init-test",
        r#"return { entrypoint = function(ctx, msg) return msg end }"#,
    )
    .await;

    // Spawn the actor — this triggers the blank init notification via the scheduler
    env.manager.spawn_actor(process.clone());

    // The executor receives and processes the blank init (nil), returns nil
    let chunks = wait_for_chunks(&env.scheduler, event, 1, Duration::from_secs(2)).await;
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].status, RuntimeStatus::End);
    assert_eq!(
        chunks[0].returns,
        msgpack_value(&serde_json::json!({"data": null}))
    );
    assert_eq!(chunks[0].syscalls, vec![]);
}

// --- Test 13: Auto-spawn on first proposal also sends init notification ---

#[tokio::test]
async fn test_auto_spawn_sends_init_notification() {
    let env = setup();

    let process = ProcessId {
        namespace: "test".into(),
        app: "auto-init".into(),
        proc: "entrypoint".into(),
    };
    // Original proposal arrives first (seq 0), init notification arrives second (seq 1)
    let event_first = EventId {
        namespace: "test".into(),
        app: "auto-init".into(),
        proc: "entrypoint".into(),
        seq: 0,
    };
    let event_init = EventId {
        namespace: "test".into(),
        app: "auto-init".into(),
        proc: "entrypoint".into(),
        seq: 1,
    };

    add_package(
        &env.store,
        "test",
        "auto-init",
        r#"return { entrypoint = function(ctx, msg) return msg end }"#,
    )
    .await;

    // Send a proposal — auto-spawns the actor, which also enqueues an init notification
    env.scheduler
        .add_proposal(Proposal {
            process: process.clone(),
            event: None,
            input: mp("hello"),
            promise: None,
            from: ProcessId {
                namespace: String::new(),
                app: String::new(),
                proc: String::new(),
            },
        })
        .await;

    // The original proposal (seq 0) is forwarded first since spawn_actor is synchronous
    // within the RouteProposal handler
    let first_chunks =
        wait_for_chunks(&env.scheduler, event_first, 1, Duration::from_secs(2)).await;
    assert_eq!(first_chunks.len(), 1);
    assert_eq!(first_chunks[0].status, RuntimeStatus::End);
    assert_eq!(first_chunks[0].returns, mp_data("hello"));
    assert_eq!(first_chunks[0].syscalls, vec![]);

    // The init notification (seq 1) arrives after, via a second RouteProposal
    let init_chunks = wait_for_chunks(&env.scheduler, event_init, 1, Duration::from_secs(2)).await;
    assert_eq!(init_chunks.len(), 1);
    assert_eq!(init_chunks[0].status, RuntimeStatus::End);
    assert_eq!(
        init_chunks[0].returns,
        msgpack_value(&serde_json::json!({"data": null}))
    );
}

// --- Blackbox test: full flow through filesystem, SQLite, and HTTP ---

#[tokio::test]
async fn test_blackbox_full_flow() {
    use arcanum::proc::http_server::HttpServerHandle;

    let tmpdir = TempDir::new().unwrap();
    let store_dir = tmpdir.path().join("store");
    std::fs::create_dir_all(&store_dir).unwrap();

    // Create a tar.gz package with arcanum.toml + main.lua on disk
    let code = br#"return { entrypoint = function(ctx, msg) return ctx.from end }"#;
    let toml = br#"name = "^test/echo""#;
    let tarball = make_tar_gz_with_files(&[("main.lua", &code[..]), ("arcanum.toml", &toml[..])]);
    std::fs::write(store_dir.join("pkg.tar.gz"), &tarball).unwrap();

    // Open filesystem store (reads tar.gz, extracts arcanum.toml, registers name)
    let store = StoreHandle::new(Box::new(FileSystemPackageStore::open(&store_dir).unwrap()));

    // Name was auto-registered from arcanum.toml inside the tar.gz
    let names = store.list_names().await;
    assert!(
        names.contains(&"^test/echo".to_string()),
        "name must be auto-registered from arcanum.toml inside tar.gz"
    );

    // Set up persistent SQLite-backed scheduler + manager
    let (sched_tx, sched_rx) = mpsc::unbounded_channel();
    let scheduler = SchedulerHandle::from_sender(sched_tx);
    let state_dir = tmpdir.path().join("state");
    let manager = ManagerHandle::new(store.clone(), scheduler.clone(), state_dir);

    let scheduler_db = tmpdir.path().join("scheduler.db");
    let persistent_scheduler = PersistentScheduler::open(&scheduler_db).unwrap();
    tokio::spawn(run_scheduler(
        sched_rx,
        Box::new(persistent_scheduler),
        manager.clone(),
    ));

    // Auto-spawn entrypoints like main.rs does
    for name in store.list_names().await {
        if let Ok(app_id) = AppId::try_from(name.as_str()) {
            let process = app_id.with_process("entrypoint".to_string());
            manager.spawn_actor(process);
        }
    }

    // Start HTTP server (the only external interface)
    let http_server_proc = ProcessId {
        namespace: "sys".into(),
        app: "http-server".into(),
        proc: "entrypoint".into(),
    };
    let server = HttpServerHandle::new(scheduler.clone(), 0, http_server_proc.clone()).await;
    let port = server.port;
    manager.register_stateless(http_server_proc.clone(), server.sender());

    // Register a route: map Host header "example.com" to ^test/echo
    scheduler
        .add_proposal(Proposal {
            process: http_server_proc.clone(),
            event: None,
            input: msgpack_value(&serde_json::json!({
                "action": "add",
                "app": "test/echo",
                "host": "example.com",
            })),
            promise: None,
            from: ProcessId {
                namespace: String::new(),
                app: String::new(),
                proc: String::new(),
            },
        })
        .await;

    // Allow the route registration and init notification to settle
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Make an HTTP request through the server
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{}/any-path", port))
        .header("Host", "example.com")
        .body("hello")
        .send()
        .await
        .unwrap();

    // The handler returns ctx.from — for an HTTP-triggered event this is
    // the HTTP server's own process ID
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(body["data"], "^sys/http-server/entrypoint");
}

// --- Management server helpers ---

async fn send_mgmt_frame(stream: &mut TcpStream, value: &serde_json::Value) {
    let bytes = rmp_serde::to_vec(value).unwrap();
    let len = bytes.len() as u32;
    stream.write_all(&len.to_be_bytes()).await.unwrap();
    stream.write_all(&bytes).await.unwrap();
}

async fn read_mgmt_frame(stream: &mut TcpStream) -> serde_json::Value {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.unwrap();
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await.unwrap();
    rmp_serde::from_slice(&buf).unwrap()
}

// --- Test 14: Management server call and notify ---

#[tokio::test]
async fn test_mgmt_call_and_notify() {
    let env = setup();

    add_package(
        &env.store,
        "test",
        "mgmt-echo",
        r#"return {
            entrypoint = function(ctx, msg)
                return "hello " .. tostring(msg) .. " from " .. ctx.from
            end,
            error_handler = function(ctx, msg)
                error("boom!")
            end,
        }"#,
    )
    .await;

    let error_process = ProcessId {
        namespace: "test".into(),
        app: "mgmt-echo".into(),
        proc: "error_handler".into(),
    };
    env.manager
        .register_process(
            error_process.clone(),
            HandlerId {
                namespace: "test".into(),
                app: "mgmt-echo".into(),
                handler: "error_handler".into(),
            },
        )
        .await
        .unwrap();

    let mgmt = MgmtHandle::new(env.scheduler.clone(), 0).await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", mgmt.port))
        .await
        .unwrap();

    // Test 1: call to entrypoint (auto-spawned)
    send_mgmt_frame(
        &mut stream,
        &serde_json::json!({
            "type": "call",
            "target": "^test/mgmt-echo",
            "data": "world",
        }),
    )
    .await;

    let resp = read_mgmt_frame(&mut stream).await;
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["data"], "hello world from ^sys/mgmt/entrypoint");

    // Test 2: call with explicit /entrypoint suffix
    send_mgmt_frame(
        &mut stream,
        &serde_json::json!({
            "type": "call",
            "target": "^test/mgmt-echo/entrypoint",
            "data": "explicit",
        }),
    )
    .await;

    let resp = read_mgmt_frame(&mut stream).await;
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["data"], "hello explicit from ^sys/mgmt/entrypoint");

    // Test 3: call to a registered non-entrypoint process
    send_mgmt_frame(
        &mut stream,
        &serde_json::json!({
            "type": "call",
            "target": "^test/mgmt-echo/error_handler",
            "data": "ignored",
        }),
    )
    .await;

    let resp = read_mgmt_frame(&mut stream).await;
    assert_eq!(resp["ok"], false);
    assert!(resp["error"].as_str().unwrap().contains("boom!"));

    // Test 4: notify — should get ack
    send_mgmt_frame(
        &mut stream,
        &serde_json::json!({
            "type": "notify",
            "target": "^test/mgmt-echo",
            "data": "ping",
        }),
    )
    .await;

    let resp = read_mgmt_frame(&mut stream).await;
    assert_eq!(resp["ok"], true);
    assert!(resp.get("data").is_none());

    // Test 5: call with no data field
    send_mgmt_frame(
        &mut stream,
        &serde_json::json!({
            "type": "call",
            "target": "^test/mgmt-echo",
        }),
    )
    .await;

    let resp = read_mgmt_frame(&mut stream).await;
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["data"], "hello nil from ^sys/mgmt/entrypoint");

    // Test 6: call with JSON object data
    send_mgmt_frame(
        &mut stream,
        &serde_json::json!({
            "type": "call",
            "target": "^test/mgmt-echo",
            "data": {"key": "value", "n": 42},
        }),
    )
    .await;

    let resp = read_mgmt_frame(&mut stream).await;
    assert_eq!(resp["ok"], true);
    assert!(resp["data"].as_str().unwrap().starts_with("hello "));

    // Test 7: invalid target (empty namespace)
    send_mgmt_frame(
        &mut stream,
        &serde_json::json!({
            "type": "call",
            "target": "/a/b",
            "data": null,
        }),
    )
    .await;

    let resp = read_mgmt_frame(&mut stream).await;
    assert_eq!(resp["ok"], false);
    assert!(resp["error"].as_str().unwrap().contains("invalid target"));

    // Test 8: unknown message type
    send_mgmt_frame(
        &mut stream,
        &serde_json::json!({
            "type": "bogus",
            "target": "^test/mgmt-echo",
        }),
    )
    .await;

    let resp = read_mgmt_frame(&mut stream).await;
    assert_eq!(resp["ok"], false);
    assert!(
        resp["error"]
            .as_str()
            .unwrap()
            .contains("unknown message type")
    );
}

// --- Test 15: Management server call timeout ---

#[tokio::test]
async fn test_mgmt_call_timeout() {
    let env = setup();

    add_package(
        &env.store,
        "test",
        "timeout-app",
        r#"return {
            entrypoint = function(ctx, msg)
                -- This call will never resolve (no such process) so the caller suspends
                return call("^test/timeout-app/noone", "hello")
            end,
        }"#,
    )
    .await;

    let mgmt = MgmtHandle::new(env.scheduler.clone(), 0).await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", mgmt.port))
        .await
        .unwrap();

    // Call with a 100ms timeout — the inner call() suspends forever, so the
    // management server should hit its timeout before the process finishes.
    send_mgmt_frame(
        &mut stream,
        &serde_json::json!({
            "type": "call",
            "target": "^test/timeout-app",
            "data": null,
            "timeoutMs": 100,
        }),
    )
    .await;

    let resp = read_mgmt_frame(&mut stream).await;
    assert_eq!(resp["ok"], false);
    assert_eq!(resp["error"], "timeout");
}

// --- Test 16: Management server notifies deliver ---

#[tokio::test]
async fn test_mgmt_notify_delivers_to_target() {
    let env = setup();

    let src_process = ProcessId {
        namespace: "test".into(),
        app: "notify-test".into(),
        proc: "src".into(),
    };
    let dest_process = ProcessId {
        namespace: "test".into(),
        app: "notify-test".into(),
        proc: "dest".into(),
    };

    add_package(
        &env.store,
        "test",
        "notify-test",
        r#"return {
            src = function(ctx, msg)
                notify("^test/notify-test/dest", "fire-and-forget")
                return "sent"
            end,
            dest = function(ctx, msg)
                kv.set("received", msg)
                return "ok"
            end,
        }"#,
    )
    .await;

    env.manager
        .register_process(
            src_process.clone(),
            HandlerId {
                namespace: "test".into(),
                app: "notify-test".into(),
                handler: "src".into(),
            },
        )
        .await
        .unwrap();
    env.manager
        .register_process(
            dest_process.clone(),
            HandlerId {
                namespace: "test".into(),
                app: "notify-test".into(),
                handler: "dest".into(),
            },
        )
        .await
        .unwrap();

    let mgmt = MgmtHandle::new(env.scheduler.clone(), 0).await;
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", mgmt.port))
        .await
        .unwrap();

    // Trigger src via mgmt — it will notify dest internally
    send_mgmt_frame(
        &mut stream,
        &serde_json::json!({
            "type": "call",
            "target": "^test/notify-test/src",
            "data": null,
        }),
    )
    .await;

    let resp = read_mgmt_frame(&mut stream).await;
    assert_eq!(resp["ok"], true);
    assert_eq!(resp["data"], "sent");

    // Verify dest received the message
    let state = env.manager.get_state_handle(dest_process.clone()).await;
    let received = state
        .get(dest_process, "received".to_string())
        .await
        .unwrap_or_default();
    assert_eq!(received, "fire-and-forget");
}

// --- Test 17: Management server multiple concurrent connections ---

#[tokio::test]
async fn test_mgmt_multiple_connections() {
    let env = setup();

    add_package(
        &env.store,
        "test",
        "multi",
        r#"return {
            entrypoint = function(ctx, msg)
                return msg
            end,
        }"#,
    )
    .await;

    let mgmt = MgmtHandle::new(env.scheduler.clone(), 0).await;

    let mut stream1 = TcpStream::connect(format!("127.0.0.1:{}", mgmt.port))
        .await
        .unwrap();
    let mut stream2 = TcpStream::connect(format!("127.0.0.1:{}", mgmt.port))
        .await
        .unwrap();

    send_mgmt_frame(
        &mut stream1,
        &serde_json::json!({
            "type": "call",
            "target": "^test/multi",
            "data": "conn1",
        }),
    )
    .await;
    send_mgmt_frame(
        &mut stream2,
        &serde_json::json!({
            "type": "call",
            "target": "^test/multi",
            "data": "conn2",
        }),
    )
    .await;

    let resp1 = read_mgmt_frame(&mut stream1).await;
    let resp2 = read_mgmt_frame(&mut stream2).await;

    assert_eq!(resp1["ok"], true);
    assert_eq!(resp2["ok"], true);
    let data1 = resp1["data"].as_str().unwrap();
    let data2 = resp2["data"].as_str().unwrap();
    assert!(data1.contains("conn1"));
    assert!(data2.contains("conn2"));
}

// --- Test 18: Rescan picks up new package version ---

#[test]
fn test_rescan_updates_package() {
    let tmpdir = TempDir::new().unwrap();
    let store_dir = tmpdir.path().join("store");
    std::fs::create_dir_all(&store_dir).unwrap();

    // V1: version 1.0.0
    let v1_code = "return { entrypoint = function(ctx, msg) return 'v1' end }";
    let v1_toml = "name = \"test/reload\"\nversion = \"1.0.0\"";
    let v1_tarball = make_tar_gz_with_files(&[
        ("main.lua", v1_code.as_bytes()),
        ("arcanum.toml", v1_toml.as_bytes()),
    ]);
    std::fs::write(store_dir.join("pkg-v1.tar.gz"), &v1_tarball).unwrap();

    // Open store — it reads v1
    let store = FileSystemPackageStore::open(&store_dir).unwrap();
    assert!(store.list_names().contains(&"^test/reload".into()));
    let v1_key = store.resolve_name("^test/reload").unwrap();
    let v1_asset = store.get_asset(&v1_key, "main.lua").unwrap();
    assert!(String::from_utf8_lossy(&v1_asset).contains("'v1'"));

    // V2: version 2.0.0 — write to filesystem, don't re-open store
    let v2_code = "return { entrypoint = function(ctx, msg) return 'v2' end }";
    let v2_toml = "name = \"test/reload\"\nversion = \"2.0.0\"";
    let v2_tarball = make_tar_gz_with_files(&[
        ("main.lua", v2_code.as_bytes()),
        ("arcanum.toml", v2_toml.as_bytes()),
    ]);
    std::fs::write(store_dir.join("pkg-v2.tar.gz"), &v2_tarball).unwrap();

    // Rescan should detect the new version
    let mut store = store;
    let updated = store.rescan();
    assert!(
        updated.contains(&"^test/reload".into()),
        "rescan should detect update"
    );

    // Verify name now resolves to the new key
    let v2_key = store.resolve_name("^test/reload").unwrap();
    assert_ne!(v1_key, v2_key, "key should change for new version");

    let v2_asset = store.get_asset(&v2_key, "main.lua").unwrap();
    assert!(
        String::from_utf8_lossy(&v2_asset).contains("'v2'"),
        "asset should contain v2 code"
    );

    // Old key still resolves for old package
    let old_asset = store.get_asset(&v1_key, "main.lua").unwrap();
    assert!(String::from_utf8_lossy(&old_asset).contains("'v1'"));
}

// --- Test 19: Rescan ignores same or lower version ---

#[test]
fn test_rescan_ignores_lower_version() {
    let tmpdir = TempDir::new().unwrap();
    let store_dir = tmpdir.path().join("store");
    std::fs::create_dir_all(&store_dir).unwrap();

    let v1_code = "return { entrypoint = function() return 'v1' end }";
    let v1_toml = "name = \"test/stable\"\nversion = \"5.0.0\"";
    let v1_tarball = make_tar_gz_with_files(&[
        ("main.lua", v1_code.as_bytes()),
        ("arcanum.toml", v1_toml.as_bytes()),
    ]);
    std::fs::write(store_dir.join("v1.tar.gz"), &v1_tarball).unwrap();

    let mut store = FileSystemPackageStore::open(&store_dir).unwrap();
    let v1_key = store.resolve_name("^test/stable").unwrap();

    // V2: version 4.0.0 (lower) — should be ignored
    let v2_code = "return { entrypoint = function() return 'v2' end }";
    let v2_toml = "name = \"test/stable\"\nversion = \"4.0.0\"";
    let v2_tarball = make_tar_gz_with_files(&[
        ("main.lua", v2_code.as_bytes()),
        ("arcanum.toml", v2_toml.as_bytes()),
    ]);
    std::fs::write(store_dir.join("v2.tar.gz"), &v2_tarball).unwrap();

    let updated = store.rescan();
    assert!(!updated.contains(&"^test/stable".into()));

    let still_key = store.resolve_name("^test/stable").unwrap();
    assert_eq!(still_key, v1_key, "key should not change for lower version");
}

// --- Test 20: Respawn swaps executor to new code ---

#[tokio::test]
async fn test_respawn_app_swaps_executor() {
    let env = setup();

    let process = ProcessId {
        namespace: "test".into(),
        app: "swap-app".into(),
        proc: "entrypoint".into(),
    };

    // Add v1 package
    let v1_code = "return { entrypoint = function(ctx, msg) return 'v1:' .. tostring(msg) end }";
    add_package(&env.store, "test", "swap-app", v1_code).await;

    // Spawn and verify v1
    env.manager.spawn_actor(process.clone());
    // "hello" proposal arrives before the init notification because spawn_actor
    // sends the init message asynchronously through the scheduler
    env.scheduler
        .add_proposal(Proposal {
            process: process.clone(),
            event: None,
            input: mp("hello"),
            promise: None,
            from: ProcessId {
                namespace: String::new(),
                app: String::new(),
                proc: String::new(),
            },
        })
        .await;

    let event0 = EventId {
        namespace: "test".into(),
        app: "swap-app".into(),
        proc: "entrypoint".into(),
        seq: 0,
    };

    let chunks = wait_for_chunks(&env.scheduler, event0, 1, Duration::from_secs(2)).await;
    assert_eq!(chunks[0].returns, mp_data("v1:hello"));

    // "Update" the package: add new code, point name to new key
    let v2_code = "return { entrypoint = function(ctx, msg) return 'v2:' .. tostring(msg) end }";
    let tarball = make_tar_gz(v2_code);
    let v2_key = env.store.add_package(tarball.into()).await.unwrap();
    env.store.set_name("^test/swap-app".into(), v2_key);

    // Respawn the app — cancels in-flight, swaps executor
    let app_id = AppId::try_from("^test/swap-app").unwrap();
    env.manager.respawn_app(app_id);

    // Wait for respawn to complete (asynchronous, fire-and-forget)
    tokio::time::sleep(Duration::from_millis(500)).await;

    // New proposal goes to new executor with v2 code
    env.scheduler
        .add_proposal(Proposal {
            process: process.clone(),
            event: None,
            input: mp("world"),
            promise: None,
            from: ProcessId {
                namespace: String::new(),
                app: String::new(),
                proc: String::new(),
            },
        })
        .await;

    // After respawn: seq 2 = init notification, seq 3 = "world"
    let event_world = EventId {
        namespace: "test".into(),
        app: "swap-app".into(),
        proc: "entrypoint".into(),
        seq: 3,
    };
    let chunks2 = wait_for_chunks(&env.scheduler, event_world, 1, Duration::from_secs(2)).await;
    assert_eq!(chunks2[0].returns, mp_data("v2:world"));
}
