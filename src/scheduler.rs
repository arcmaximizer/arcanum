use crate::types::{EventId, ProcessId};
use anyhow::{Result, anyhow, bail};
use std::collections::{HashMap, HashSet, VecDeque};
use tokio::sync::mpsc;
use tracing;

#[derive(Debug, Clone)]
pub struct RuntimeCall {
    pub proposal: Proposal,
}

#[derive(Debug)]
pub enum SchedulerMsg {
    AddProposal {
        proposal: Proposal,
        resp: tokio::sync::oneshot::Sender<u64>,
    },
    RegisterExecutor {
        process: ProcessId,
        tx: mpsc::UnboundedSender<Proposal>,
    },
    UnregisterExecutor {
        process: ProcessId,
    },
    RegisterRuntime {
        process: ProcessId,
        tx: mpsc::UnboundedSender<RuntimeCall>,
    },
    UnregisterRuntime {
        process: ProcessId,
    },
    GetNext {
        process: ProcessId,
        resp: tokio::sync::oneshot::Sender<Option<Proposal>>,
    },
    GetChunks {
        event: EventId,
        resp: tokio::sync::oneshot::Sender<Option<Vec<Receipt>>>,
    },
    GetNextEventId {
        process: ProcessId,
        resp: tokio::sync::oneshot::Sender<EventId>,
    },
    GetLogSeq {
        process: ProcessId,
        resp: tokio::sync::oneshot::Sender<u64>,
    },
    Satisfy {
        proposal: Proposal,
        receipt: Receipt,
        is_final: bool,
        resp: tokio::sync::oneshot::Sender<Result<NextAction>>,
    },
    RuntimeSatisfy {
        proposal: Proposal,
        returns: String,
        resp: tokio::sync::oneshot::Sender<Result<NextAction>>,
    },
}

pub async fn run_scheduler(
    mut rx: mpsc::UnboundedReceiver<SchedulerMsg>,
    mut scheduler: Box<dyn Scheduler + Send>,
) {
    let mut executor_senders: HashMap<ProcessId, mpsc::UnboundedSender<Proposal>> = HashMap::new();
    let mut runtime_senders: HashMap<ProcessId, mpsc::UnboundedSender<RuntimeCall>> =
        HashMap::new();

    fn route_proposal(
        proposal: &Proposal,
        executors: &HashMap<ProcessId, mpsc::UnboundedSender<Proposal>>,
        runtimes: &HashMap<ProcessId, mpsc::UnboundedSender<RuntimeCall>>,
    ) {
        if let Some(rx) = runtimes.get(&proposal.process) {
            let _ = rx.send(RuntimeCall {
                proposal: proposal.clone(),
            });
        } else if let Some(tx) = executors.get(&proposal.process) {
            let _ = tx.send(proposal.clone());
        }
    }

    while let Some(msg) = rx.recv().await {
        match msg {
            SchedulerMsg::AddProposal { proposal, resp } => {
                let id = scheduler.add_proposal(proposal.clone());
                let _ = resp.send(id);

                route_proposal(&proposal, &executor_senders, &runtime_senders);
            }
            SchedulerMsg::RegisterExecutor { process, tx } => {
                executor_senders.insert(process, tx);
            }
            SchedulerMsg::UnregisterExecutor { process } => {
                executor_senders.remove(&process);
            }
            SchedulerMsg::RegisterRuntime { process, tx } => {
                scheduler.register_runtime(process.clone());
                runtime_senders.insert(process, tx);
            }
            SchedulerMsg::UnregisterRuntime { process } => {
                scheduler.unregister_runtime(&process);
                runtime_senders.remove(&process);
            }
            SchedulerMsg::GetNext { process, resp } => {
                let proposal = scheduler.get_next_proposal(&process).cloned();
                let _ = resp.send(proposal);
            }
            SchedulerMsg::GetChunks { event, resp } => {
                let chunks = scheduler.get_chunks_from_event(&event).cloned();
                let _ = resp.send(chunks);
            }
            SchedulerMsg::GetNextEventId { process, resp } => {
                let event_id = scheduler.get_next_event_id(&process);
                let _ = resp.send(event_id);
            }
            SchedulerMsg::GetLogSeq { process, resp } => {
                let log_seq = scheduler.get_log_seq(&process);
                let _ = resp.send(log_seq);
            }
            SchedulerMsg::Satisfy {
                proposal,
                receipt,
                is_final,
                resp,
            } => {
                let result = scheduler.satisfy_proposal(&proposal, receipt, is_final);
                if let Ok((_action, new_proposals)) = &result {
                    if !new_proposals.is_empty() {
                        tracing::debug!(
                            "Adding {} new proposals from satisfy:",
                            new_proposals.len()
                        );
                        for p in new_proposals {
                            tracing::debug!(
                                "  -> process={} input={}",
                                p.process,
                                p.input
                            );
                        }
                    }
                    for p in new_proposals {
                        route_proposal(p, &executor_senders, &runtime_senders);
                    }
                }
                let _ = resp.send(result.map(|(action, _)| action));
            }
            SchedulerMsg::RuntimeSatisfy {
                proposal,
                returns,
                resp,
            } => {
                let result = scheduler.runtime_satisfy(&proposal, returns);
                if let Ok((_action, new_proposals)) = &result {
                    if !new_proposals.is_empty() {
                        tracing::debug!(
                            "Adding {} new proposals from runtime_satisfy:",
                            new_proposals.len()
                        );
                        for p in new_proposals {
                            tracing::debug!(
                                "  -> process={} input={}",
                                p.process,
                                p.input
                            );
                        }
                    }
                    for p in new_proposals {
                        route_proposal(p, &executor_senders, &runtime_senders);
                    }
                }
                let _ = resp.send(result.map(|(action, _)| action));
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Receipt {
    pub proposal: Proposal,
    /// Sequence index inside a given event's history
    /// e.g. ^arc/my-app/my-process/e48/c1
    pub in_event_seq: u64,
    /// Sequence index inside the chunk history
    /// e.g. ^arc/my-app/my-process/c48
    pub in_log_seq: u64,
    pub syscalls: Vec<Syscall>,
    pub returns: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Syscall {
    KVRead { key: String, current_value: String },
    KVWrite { key: String, new_value: String },
    Call { proposal: Proposal },
    Notify { proposal: Proposal },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Proposal {
    pub process: ProcessId,
    pub event: Option<EventId>,
    pub input: String,
    pub promise: Option<Promise>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Promise {
    pub id: u64,
    pub target: EventId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NextAction {
    pub event: EventId,
    pub proposal: Option<Proposal>,
}

pub trait Scheduler {
    fn add_proposal(&mut self, proposal: Proposal) -> u64;
    fn get_next_proposal(&mut self, process: &ProcessId) -> Option<&Proposal>;
    fn get_next_event_id(&mut self, process: &ProcessId) -> EventId;
    fn get_log_seq(&self, process: &ProcessId) -> u64;
    fn satisfy_proposal(
        &mut self,
        proposal: &Proposal,
        receipt: Receipt,
        is_final: bool,
    ) -> Result<(NextAction, Vec<Proposal>)>;
    fn get_chunks_from_event(&self, event: &EventId) -> Option<&Vec<Receipt>>;
    fn get_chunk_from_event(&self, event: &EventId, chunk_seq: u64) -> Option<&Receipt>;
    fn register_runtime(&mut self, process: ProcessId);
    fn unregister_runtime(&mut self, process: &ProcessId);
    fn is_runtime(&self, process: &ProcessId) -> bool;
    fn runtime_satisfy(
        &mut self,
        proposal: &Proposal,
        returns: String,
    ) -> Result<(NextAction, Vec<Proposal>)>;
}

pub struct InMemoryScheduler {
    pub event_chunks: HashMap<EventId, Vec<Receipt>>,
    pub process_chunks: HashMap<ProcessId, Vec<Receipt>>,
    pub schedule: HashMap<ProcessId, VecDeque<Proposal>>,
    pub event_counter: HashMap<ProcessId, u64>,
    pub proposal_counter: u64,
    pub runtime_processes: HashSet<ProcessId>,
}

impl InMemoryScheduler {
    pub fn new() -> Self {
        Self {
            event_chunks: HashMap::new(),
            process_chunks: HashMap::new(),
            schedule: HashMap::new(),
            event_counter: HashMap::new(),
            proposal_counter: 0,
            runtime_processes: HashSet::new(),
        }
    }
}

impl Scheduler for InMemoryScheduler {
    fn add_proposal(&mut self, proposal: Proposal) -> u64 {
        let schedule = self
            .schedule
            .entry(proposal.process.clone())
            .or_insert(VecDeque::new());

        schedule.push_back(proposal);
        (schedule.len() - 1) as u64
    }
    fn get_next_proposal(&mut self, process: &ProcessId) -> Option<&Proposal> {
        self.schedule.get(process)?.get(0)
    }
    fn get_next_event_id(&mut self, process: &ProcessId) -> EventId {
        let seq = *self.event_counter.entry(process.clone()).or_insert(0);
        EventId {
            namespace: process.namespace.clone(),
            app: process.app.clone(),
            proc: process.proc.clone(),
            seq,
        }
    }
    fn get_log_seq(&self, process: &ProcessId) -> u64 {
        self.process_chunks
            .get(process)
            .map(|c| c.len() as u64)
            .unwrap_or(0)
    }
    fn satisfy_proposal(
        &mut self,
        proposal: &Proposal,
        receipt: Receipt,
        is_final: bool,
    ) -> Result<(NextAction, Vec<Proposal>)> {
        // This should always validate data to ensure that any state transitions always follow some
        // invariants. Keep effects at the end in case of reverts and use transactions if possible.

        // Extract values needed for validation before taking any mutable borrows
        let process = &proposal.process;
        let in_log_seq = receipt.in_log_seq;
        let in_event_seq = receipt.in_event_seq;
        let event_id = *self.event_counter.entry(process.clone()).or_insert(0);
        if proposal.event.is_none() {
            self.event_counter.insert(process.clone(), event_id + 1);
        }
        let event = proposal.event.clone().unwrap_or(EventId {
            namespace: process.namespace.clone(),
            app: process.app.clone(),
            proc: process.proc.clone(),
            seq: event_id,
        });

        let schedule = self
            .schedule
            .entry(process.clone())
            .or_insert(VecDeque::new());
        let event_chunks = self.event_chunks.entry(event.clone()).or_insert(vec![]);
        let process_chunks = self.process_chunks.entry(process.clone()).or_insert(vec![]);

        // Proposal checks
        let first_proposal = schedule
            .get(0)
            .ok_or(anyhow!("No proposals exist in schedule"))?
            .clone();

        if *proposal != first_proposal {
            bail!("Proposal does not match first scheduled proposal")
        }

        if in_log_seq != process_chunks.len() as u64 {
            bail!("Chunk is misaligned with chunk history")
        }

        // Event checks
        if in_event_seq != event_chunks.len() as u64 {
            bail!("Chunk is misaligned with per-event log")
        }

        if in_event_seq != 0 {
            let prev_receipt = event_chunks.get((in_event_seq - 1) as usize).unwrap();

            if prev_receipt.proposal.process != receipt.proposal.process {
                bail!("Chunk process is mismatched with previous chunk")
            }
        }

        // Parse the receipt's syscalls
        let mut calls = Vec::new();
        let mut notif_proposals = Vec::new();

        for syscall in receipt.syscalls.iter() {
            match syscall {
                Syscall::Call { proposal } => {
                    calls.push(proposal.clone());
                }
                Syscall::Notify { proposal } => notif_proposals.push(proposal.clone()),
                _ => {}
            }
        }

        if calls.len() > 1 {
            bail!("Only one Call syscall is allowed");
        }

        // Get root chunk for promise resolution
        let root_chunk = if in_event_seq == 0 {
            receipt.clone()
        } else {
            event_chunks.first().unwrap().clone()
        };
        let promise = root_chunk.proposal.promise.clone();
        let root_returns = root_chunk.returns.clone();
        let promise_target = promise.as_ref().map(|p| p.target.clone());

        if is_final {
            schedule.pop_front();
        }

        event_chunks.push(receipt.clone());

        self.process_chunks
            .entry(process.clone())
            .or_insert(vec![])
            .push(receipt);

        let source_chunk_data = if let Some(ref target) = promise_target {
            self.event_chunks
                .get(target)
                .and_then(|chunks| chunks.first())
                .map(|chunk| (target.clone(), chunk.proposal.process.clone()))
        } else {
            None
        };

        let mut new_proposals = Vec::new();

        for nt in notif_proposals {
            self.schedule
                .entry(nt.process.clone())
                .or_insert(VecDeque::new())
                .push_back(nt.clone());
            new_proposals.push(nt);
        }

        // Satisfy any existing promises
        if is_final {
            if let Some((source_event, source_process)) = source_chunk_data {
                let promise_proposal = Proposal {
                    event: Some(source_event),
                    process: source_process,
                    input: root_returns,
                    promise: None,
                };
                self.add_proposal(promise_proposal.clone());
                new_proposals.push(promise_proposal);
            }
        }

        let action = NextAction {
            event,
            proposal: calls.last().cloned(),
        };

        Ok((action, new_proposals))
    }
    fn get_chunks_from_event(&self, event: &EventId) -> Option<&Vec<Receipt>> {
        self.event_chunks.get(event)
    }
    fn get_chunk_from_event(&self, event: &EventId, chunk_seq: u64) -> Option<&Receipt> {
        self.event_chunks.get(event)?.get(chunk_seq as usize)
    }

    fn register_runtime(&mut self, process: ProcessId) {
        self.runtime_processes.insert(process);
    }

    fn unregister_runtime(&mut self, process: &ProcessId) {
        self.runtime_processes.remove(process);
    }

    fn is_runtime(&self, process: &ProcessId) -> bool {
        self.runtime_processes.contains(process)
    }

    fn runtime_satisfy(
        &mut self,
        proposal: &Proposal,
        returns: String,
    ) -> Result<(NextAction, Vec<Proposal>)> {
        let process = &proposal.process;

        let schedule = self.schedule.get_mut(process).ok_or(anyhow!(
            "No schedule exists for process {}",
            process
        ))?;

        let first_proposal = schedule
            .get(0)
            .ok_or(anyhow!("No proposals exist in schedule"))?
            .clone();

        if *proposal != first_proposal {
            bail!("Proposal does not match first scheduled proposal")
        }

        schedule.pop_front();

        let promise = proposal.promise.clone();
        let promise_target = promise.as_ref().map(|p| p.target.clone());

        let source_chunk_data = if let Some(ref target) = promise_target {
            self.event_chunks
                .get(target)
                .and_then(|chunks| chunks.first())
                .map(|chunk| (target.clone(), chunk.proposal.process.clone()))
        } else {
            None
        };

        let mut new_proposals = Vec::new();

        if let Some((source_event, source_process)) = source_chunk_data {
            let promise_proposal = Proposal {
                event: Some(source_event),
                process: source_process,
                input: returns,
                promise: None,
            };
            self.add_proposal(promise_proposal.clone());
            new_proposals.push(promise_proposal);
        }

        let action = NextAction {
            event: EventId {
                namespace: process.namespace.clone(),
                app: process.app.clone(),
                proc: process.proc.clone(),
                seq: 0,
            },
            proposal: None,
        };

        Ok((action, new_proposals))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn proc(id: &str) -> ProcessId {
        ProcessId {
            namespace: "test".to_string(),
            app: id.to_string(),
            proc: id.to_string(),
        }
    }

    fn event(p: &ProcessId, seq: u64) -> EventId {
        EventId {
            namespace: p.namespace.clone(),
            app: p.app.clone(),
            proc: p.proc.clone(),
            seq,
        }
    }

    fn prop(input: &str) -> Proposal {
        Proposal {
            process: proc("worker"),
            event: None,
            input: input.to_string(),
            promise: None,
        }
    }

    fn receipt(
        proposal: &Proposal,
        in_event_seq: u64,
        in_log_seq: u64,
        syscalls: Vec<Syscall>,
        returns: &str,
    ) -> Receipt {
        Receipt {
            proposal: proposal.clone(),
            in_event_seq,
            in_log_seq,
            syscalls,
            returns: returns.to_string(),
        }
    }

    fn kv_read(key: &str, value: &str) -> Syscall {
        Syscall::KVRead {
            key: key.to_string(),
            current_value: value.to_string(),
        }
    }

    fn kv_write(key: &str, value: &str) -> Syscall {
        Syscall::KVWrite {
            key: key.to_string(),
            new_value: value.to_string(),
        }
    }

    fn call(target: &ProcessId, input: &str, log_seq: u64, event: &EventId) -> Syscall {
        Syscall::Call {
            proposal: Proposal {
                process: target.clone(),
                event: None,
                input: input.to_string(),
                promise: Some(Promise {
                    id: log_seq,
                    target: event.clone(),
                }),
            },
        }
    }

    fn notify(target: &ProcessId, input: &str) -> Syscall {
        Syscall::Notify {
            proposal: Proposal {
                process: target.clone(),
                event: None,
                input: input.to_string(),
                promise: None,
            },
        }
    }

    // --- add_proposal ---

    #[test]
    fn test_add_proposal() {
        let mut s = InMemoryScheduler::new();
        let p = prop("hello");
        let idx = s.add_proposal(p.clone());
        assert_eq!(idx, 0);
        assert_eq!(s.get_next_proposal(&proc("worker")), Some(&p));
    }

    #[test]
    fn test_add_proposal_sequential_indices() {
        let mut s = InMemoryScheduler::new();
        assert_eq!(s.add_proposal(prop("a")), 0);
        assert_eq!(s.add_proposal(prop("b")), 1);
        assert_eq!(s.add_proposal(prop("c")), 2);
    }

    #[test]
    fn test_add_proposal_multiple_processes() {
        let mut s = InMemoryScheduler::new();
        let mut p_a = prop("a");
        p_a.process = proc("a");
        let mut p_b = prop("b");
        p_b.process = proc("b");

        s.add_proposal(p_a.clone());
        s.add_proposal(p_b.clone());

        assert_eq!(s.get_next_proposal(&proc("a")), Some(&p_a));
        assert_eq!(s.get_next_proposal(&proc("b")), Some(&p_b));
    }

    // --- get_next_proposal ---

    #[test]
    fn test_get_next_proposal_empty() {
        let mut s = InMemoryScheduler::new();
        assert_eq!(s.get_next_proposal(&proc("nope")), None);
    }

    #[test]
    fn test_get_next_proposal_after_pop() {
        let mut s = InMemoryScheduler::new();
        let p = prop("first");
        s.add_proposal(p.clone());
        s.add_proposal(prop("second"));

        let rec = receipt(&p, 0, 0, vec![], "ok");
        s.satisfy_proposal(&p, rec, true).unwrap();

        assert_eq!(
            s.get_next_proposal(&proc("worker")).unwrap().input,
            "second"
        );
    }

    // --- get_next_event_id ---

    #[test]
    fn test_get_next_event_id_defaults_to_zero() {
        let mut s = InMemoryScheduler::new();
        let e = s.get_next_event_id(&proc("worker"));
        assert_eq!(e.seq, 0);
        assert_eq!(e.namespace, "test");
        assert_eq!(e.app, "worker");
        assert_eq!(e.proc, "worker");
    }

    #[test]
    fn test_get_next_event_id_is_idempotent() {
        let mut s = InMemoryScheduler::new();
        let e1 = s.get_next_event_id(&proc("worker"));
        let e2 = s.get_next_event_id(&proc("worker"));
        assert_eq!(e1, e2);
        assert_eq!(e1.seq, 0);
    }

    #[test]
    fn test_get_next_event_id_separate_processes() {
        let mut s = InMemoryScheduler::new();
        // Manually seed a higher counter for "worker"
        s.event_counter.insert(proc("worker"), 5);
        let e = s.get_next_event_id(&proc("worker"));
        assert_eq!(e.seq, 5);
        // "other" should still default to 0
        let e2 = s.get_next_event_id(&proc("other"));
        assert_eq!(e2.seq, 0);
    }

    // --- satisfy_proposal: basic ---

    #[test]
    fn test_satisfy_proposal_basic() {
        let mut s = InMemoryScheduler::new();
        let p = prop("hello");
        s.add_proposal(p.clone());

        let rec = receipt(&p, 0, 0, vec![], "world");
        let (action, _) = s.satisfy_proposal(&p, rec, true).unwrap();

        assert_eq!(action.event, event(&proc("worker"), 0));
        assert_eq!(action.proposal, None);
        assert!(s.schedule.get(&proc("worker")).unwrap().is_empty());
        assert_eq!(s.process_chunks.get(&proc("worker")).unwrap().len(), 1);
        assert_eq!(
            s.event_chunks
                .get(&event(&proc("worker"), 0))
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn test_satisfy_proposal_wrong_process_seq() {
        let mut s = InMemoryScheduler::new();
        let p = prop("hello");
        s.add_proposal(p.clone());

        let rec = receipt(&p, 0, 1, vec![], "ok");
        let err = s.satisfy_proposal(&p, rec, true).unwrap_err();
        assert!(err.to_string().contains("misaligned with chunk history"));
    }

    #[test]
    fn test_satisfy_proposal_wrong_event_seq() {
        let mut s = InMemoryScheduler::new();
        let p = prop("hello");
        s.add_proposal(p.clone());

        let rec = receipt(&p, 1, 0, vec![], "ok");
        let err = s.satisfy_proposal(&p, rec, true).unwrap_err();
        assert!(err.to_string().contains("misaligned with per-event log"));
    }

    #[test]
    fn test_satisfy_proposal_not_first_in_schedule() {
        let mut s = InMemoryScheduler::new();
        s.add_proposal(prop("first"));
        let p2 = prop("second");
        s.add_proposal(p2.clone());

        let rec = receipt(&p2, 0, 0, vec![], "ok");
        let err = s.satisfy_proposal(&p2, rec, true).unwrap_err();
        assert!(
            err.to_string()
                .contains("Proposal does not match first scheduled proposal")
        );
    }

    #[test]
    fn test_satisfy_proposal_empty_schedule() {
        let mut s = InMemoryScheduler::new();
        let p = prop("orphan");
        let rec = receipt(&p, 0, 0, vec![], "ok");
        let err = s.satisfy_proposal(&p, rec, true).unwrap_err();
        assert!(err.to_string().contains("No proposals exist in schedule"));
    }

    #[test]
    fn test_satisfy_proposal_mismatched_process() {
        let mut s = InMemoryScheduler::new();
        let mut p = prop("hello");
        p.event = Some(event(&proc("worker"), 0));
        s.add_proposal(p.clone());

        // Add two receipts with the same proposal
        let rec1 = receipt(&p, 0, 0, vec![], "");
        s.satisfy_proposal(&p, rec1, false).unwrap();

        // Try a second receipt for the same event but with a different process in the receipt
        let mut wrong_rec = receipt(&p, 1, 1, vec![], "");
        wrong_rec.proposal.process = proc("other");
        let err = s.satisfy_proposal(&p, wrong_rec, true).unwrap_err();
        assert!(err.to_string().contains("process is mismatched"));
    }

    // --- satisfy_proposal: intermediate (is_final = false) ---

    #[test]
    fn test_intermediate_receipt_does_not_pop() {
        let mut s = InMemoryScheduler::new();
        let p = prop("hello");
        s.add_proposal(p.clone());

        let rec = receipt(&p, 0, 0, vec![kv_read("k", "v")], "");
        s.satisfy_proposal(&p, rec, false).unwrap();

        // Proposal should still be in schedule
        assert_eq!(s.get_next_proposal(&proc("worker")), Some(&p));
        assert_eq!(s.process_chunks.get(&proc("worker")).unwrap().len(), 1);
    }

    #[test]
    fn test_multiple_intermediate_then_final() {
        let mut s = InMemoryScheduler::new();
        let mut p = prop("hello");
        let ev = event(&proc("worker"), 0);
        p.event = Some(ev.clone());
        s.add_proposal(p.clone());

        // Two intermediate KV receipts
        s.satisfy_proposal(&p, receipt(&p, 0, 0, vec![kv_read("a", "")], ""), false)
            .unwrap();
        s.satisfy_proposal(&p, receipt(&p, 1, 1, vec![kv_write("a", "42")], ""), false)
            .unwrap();

        // Final receipt
        s.satisfy_proposal(&p, receipt(&p, 2, 2, vec![], "done"), true)
            .unwrap();

        // Schedule should be empty now
        assert!(s.schedule.get(&proc("worker")).unwrap().is_empty());
        assert_eq!(s.process_chunks.get(&proc("worker")).unwrap().len(), 3);

        let ev = event(&proc("worker"), 0);
        let chunks = s.get_chunks_from_event(&ev).unwrap();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[2].returns, "done");
    }

    #[test]
    fn test_call_after_kv_receipts() {
        let mut s = InMemoryScheduler::new();
        let mut p = prop("hello");
        let ev = event(&proc("worker"), 0);
        p.event = Some(ev.clone());
        s.add_proposal(p.clone());

        // KV read receipt (intermediate)
        s.satisfy_proposal(&p, receipt(&p, 0, 0, vec![kv_read("k", "v")], ""), false)
            .unwrap();

        // KV write receipt (intermediate)
        s.satisfy_proposal(&p, receipt(&p, 1, 1, vec![kv_write("k", "42")], ""), false)
            .unwrap();

        // Call receipt (final)
        let target = proc("callee");
        let call_sys = call(&target, "ping", 2, &ev);
        let (action, _) = s
            .satisfy_proposal(&p, receipt(&p, 2, 2, vec![call_sys.clone()], ""), true)
            .unwrap();

        assert_eq!(action.event, ev);
        // The call's proposal should be the NextAction proposal
        if let Syscall::Call { proposal } = &call_sys {
            assert_eq!(action.proposal.as_ref(), Some(proposal));
        } else {
            panic!("expected Call");
        }
    }

    // --- Notify routing ---

    #[test]
    fn test_notify_routes_to_target_process() {
        let mut s = InMemoryScheduler::new();
        let p = prop("hello");
        s.add_proposal(p.clone());

        let target = proc("other");
        let rec = receipt(&p, 0, 0, vec![notify(&target, "fire!")], "");
        s.satisfy_proposal(&p, rec, true).unwrap();

        assert_eq!(s.get_next_proposal(&target).unwrap().input, "fire!");
    }

    // --- Promise resolution ---

    #[test]
    fn test_promise_resolution() {
        let mut s = InMemoryScheduler::new();

        let proc_a = proc("a");
        let proc_b = proc("b");
        let ev_a = event(&proc_a, 0);

        let p_a = Proposal {
            process: proc_a.clone(),
            event: None,
            input: "call_b".to_string(),
            promise: None,
        };
        s.add_proposal(p_a.clone());

        // First receipt for A: contains a Call to B with a promise
        let call_sys = call(&proc_b, "hey", 0, &ev_a);
        let (action, _) = s
            .satisfy_proposal(&p_a, receipt(&p_a, 0, 0, vec![call_sys.clone()], ""), true)
            .unwrap();

        // Add the call proposal to B's schedule (simulating executor routing)
        let p_b = action.proposal.unwrap();
        s.add_proposal(p_b.clone());

        // B processes it, returning "reply"
        let ev_b = event(&proc_b, 0);
        let rec_b = receipt(&p_b, 0, 0, vec![], "reply");
        let (action, _) = s.satisfy_proposal(&p_b, rec_b, true).unwrap();

        assert_eq!(action.event, ev_b);

        // Promise resolution: A should have a new proposal with "reply"
        let a_next = s.get_next_proposal(&proc_a).unwrap();
        assert_eq!(a_next.input, "reply");
        assert_eq!(a_next.event, Some(ev_a.clone()));
        assert!(a_next.promise.is_none());
    }

    #[test]
    fn test_promise_resolution_only_on_final() {
        let mut s = InMemoryScheduler::new();

        let proc_a = proc("a");
        let proc_b = proc("b");
        let ev_a = event(&proc_a, 0);

        let p_a = prop("call_b");
        s.add_proposal(p_a.clone());

        let call_sys = call(&proc_b, "hey", 0, &ev_a);
        let (action, _) = s
            .satisfy_proposal(&p_a, receipt(&p_a, 0, 0, vec![call_sys.clone()], ""), true)
            .unwrap();

        // Add the call proposal to B's schedule
        let p_b = action.proposal.unwrap();
        s.add_proposal(p_b.clone());

        // B's intermediate receipt (is_final=false)
        s.satisfy_proposal(&p_b, receipt(&p_b, 0, 0, vec![], "reply"), false)
            .unwrap();

        // Promise should NOT be resolved yet (is_final=false)
        assert!(s.get_next_proposal(&proc_a).is_none());
    }

    // --- get_chunks_from_event ---

    #[test]
    fn test_get_chunks_from_event_empty() {
        let s = InMemoryScheduler::new();
        assert_eq!(s.get_chunks_from_event(&event(&proc("worker"), 0)), None);
    }

    #[test]
    fn test_get_chunks_from_event_after_satisfy() {
        let mut s = InMemoryScheduler::new();
        let p = prop("hello");
        s.add_proposal(p.clone());

        let rec = receipt(&p, 0, 0, vec![], "ok");
        s.satisfy_proposal(&p, rec, true).unwrap();

        let chunks = s.get_chunks_from_event(&event(&proc("worker"), 0)).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].returns, "ok");
    }

    // --- get_chunk_from_event ---

    #[test]
    fn test_get_chunk_from_event_by_seq() {
        let mut s = InMemoryScheduler::new();
        let mut p = prop("hello");
        let ev = event(&proc("worker"), 0);
        p.event = Some(ev.clone());
        s.add_proposal(p.clone());

        s.satisfy_proposal(&p, receipt(&p, 0, 0, vec![], "first"), false)
            .unwrap();
        s.satisfy_proposal(&p, receipt(&p, 1, 1, vec![], "second"), true)
            .unwrap();

        let ev = event(&proc("worker"), 0);
        assert_eq!(s.get_chunk_from_event(&ev, 0).unwrap().returns, "first");
        assert_eq!(s.get_chunk_from_event(&ev, 1).unwrap().returns, "second");
        assert_eq!(s.get_chunk_from_event(&ev, 2), None);
    }

    // --- Runtime process registration ---

    #[test]
    fn test_register_runtime() {
        let mut s = InMemoryScheduler::new();
        let p = proc("http");
        assert!(!s.is_runtime(&p));
        s.register_runtime(p.clone());
        assert!(s.is_runtime(&p));
    }

    #[test]
    fn test_unregister_runtime() {
        let mut s = InMemoryScheduler::new();
        let p = proc("http");
        s.register_runtime(p.clone());
        assert!(s.is_runtime(&p));
        s.unregister_runtime(&p);
        assert!(!s.is_runtime(&p));
    }

    // --- runtime_satisfy: promise resolution ---

    #[test]
    fn test_runtime_satisfy_resolves_promise() {
        let mut s = InMemoryScheduler::new();

        let caller = proc("caller");
        let http_process = proc("http");
        let ev = event(&caller, 0);

        // Set up the caller's event with a root chunk (simulating a completed Call from the caller)
        let p_caller = Proposal {
            process: caller.clone(),
            event: None,
            input: "start".to_string(),
            promise: None,
        };
        s.add_proposal(p_caller.clone());

        let call_sys = call(&http_process, "https://example.com", 0, &ev);
        let (action, _) = s
            .satisfy_proposal(
                &p_caller,
                receipt(&p_caller, 0, 0, vec![call_sys], ""),
                true,
            )
            .unwrap();

        // The call proposal should be returned as the next action
        let p_http = action.proposal.unwrap();
        assert_eq!(p_http.process, http_process);
        assert_eq!(p_http.input, "https://example.com");
        assert!(p_http.promise.is_some());

        // Add the runtime proposal to the http schedule
        s.add_proposal(p_http.clone());

        // Now satisfy the runtime call — no receipts, just the return value
        let (action, new_proposals) = s
            .runtime_satisfy(&p_http, "response body".to_string())
            .unwrap();

        assert_eq!(action.event.namespace, "test");
        assert_eq!(action.event.app, "http");
        assert_eq!(action.event.proc, "http");
        assert_eq!(action.proposal, None);

        // Promise should be resolved: caller gets a new proposal with the return value
        assert_eq!(new_proposals.len(), 1);
        let resolved = &new_proposals[0];
        assert_eq!(resolved.process, caller);
        assert_eq!(resolved.input, "response body");
        assert_eq!(resolved.event, Some(ev));
        assert!(resolved.promise.is_none());

        // The resolved proposal should be in the caller's schedule
        let caller_next = s.get_next_proposal(&caller).unwrap();
        assert_eq!(caller_next.input, "response body");
    }

    #[test]
    fn test_runtime_satisfy_pops_from_schedule() {
        let mut s = InMemoryScheduler::new();

        let http_process = proc("http");
        let p1 = Proposal {
            process: http_process.clone(),
            event: None,
            input: "first".to_string(),
            promise: None,
        };
        let p2 = Proposal {
            process: http_process.clone(),
            event: None,
            input: "second".to_string(),
            promise: None,
        };

        s.add_proposal(p1.clone());
        s.add_proposal(p2.clone());

        s.runtime_satisfy(&p1, "ok".to_string()).unwrap();

        // p1 should be popped, p2 should be next
        let next = s.get_next_proposal(&http_process).unwrap();
        assert_eq!(next.input, "second");
    }

    // --- runtime_satisfy: error cases ---

    #[test]
    fn test_runtime_satisfy_no_schedule() {
        let mut s = InMemoryScheduler::new();
        let p = Proposal {
            process: proc("http"),
            event: None,
            input: "url".to_string(),
            promise: None,
        };
        let err = s.runtime_satisfy(&p, "ok".to_string()).unwrap_err();
        assert!(err.to_string().contains("No schedule exists"));
    }

    #[test]
    fn test_runtime_satisfy_empty_schedule() {
        let mut s = InMemoryScheduler::new();
        let http_process = proc("http");
        s.add_proposal(Proposal {
            process: http_process.clone(),
            event: None,
            input: "first".to_string(),
            promise: None,
        });
        // Pop it to empty the schedule
        s.schedule.get_mut(&http_process).unwrap().pop_front();

        let p = Proposal {
            process: http_process,
            event: None,
            input: "url".to_string(),
            promise: None,
        };
        let err = s.runtime_satisfy(&p, "ok".to_string()).unwrap_err();
        assert!(err.to_string().contains("No proposals exist in schedule"));
    }

    #[test]
    fn test_runtime_satisfy_not_first() {
        let mut s = InMemoryScheduler::new();

        let http_process = proc("http");
        let p1 = Proposal {
            process: http_process.clone(),
            event: None,
            input: "first".to_string(),
            promise: None,
        };
        let p2 = Proposal {
            process: http_process.clone(),
            event: None,
            input: "second".to_string(),
            promise: None,
        };

        s.add_proposal(p1);
        s.add_proposal(p2.clone());

        // Try to satisfy p2 while p1 is still first in queue
        let err = s.runtime_satisfy(&p2, "ok".to_string()).unwrap_err();
        assert!(
            err.to_string()
                .contains("Proposal does not match first scheduled proposal")
        );
    }

    // --- runtime_satisfy: no receipts stored ---

    #[test]
    fn test_runtime_satisfy_no_receipts_stored() {
        let mut s = InMemoryScheduler::new();

        let caller = proc("caller");
        let http_process = proc("http");
        let ev = event(&caller, 0);

        // Set up caller's event
        let p_caller = Proposal {
            process: caller.clone(),
            event: None,
            input: "start".to_string(),
            promise: None,
        };
        s.add_proposal(p_caller.clone());

        let call_sys = call(&http_process, "https://example.com", 0, &ev);
        let (action, _) = s
            .satisfy_proposal(
                &p_caller,
                receipt(&p_caller, 0, 0, vec![call_sys], ""),
                true,
            )
            .unwrap();

        let p_http = action.proposal.unwrap();
        s.add_proposal(p_http.clone());
        s.runtime_satisfy(&p_http, "response".to_string()).unwrap();

        // The runtime process should have NO chunks stored
        assert!(s.process_chunks.get(&http_process).is_none());
        assert!(s.event_chunks.get(&event(&http_process, 0)).is_none());

        // But the caller's chunks should still exist (they go through normal satisfy)
        assert!(s.process_chunks.get(&caller).is_some());
    }

    // --- runtime_satisfy: no promise (no new proposals) ---

    #[test]
    fn test_runtime_satisfy_no_promise_no_new_proposals() {
        let mut s = InMemoryScheduler::new();

        let http_process = proc("http");
        let p = Proposal {
            process: http_process.clone(),
            event: None,
            input: "url".to_string(),
            promise: None,
        };
        s.add_proposal(p.clone());

        let (_action, new_proposals) = s.runtime_satisfy(&p, "ok".to_string()).unwrap();

        assert!(new_proposals.is_empty());
    }
}
