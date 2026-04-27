use crate::types::{EventId, ProcessId};
use anyhow::{Result, anyhow, bail};
use std::collections::{HashMap, VecDeque};

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
    pub returns: Vec<String>,
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
    pub inputs: Vec<String>,
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
    fn satisfy_proposal(&mut self, proposal: &Proposal, receipt: Receipt) -> Result<NextAction>;
    fn get_chunks_from_event(&self, event: &EventId) -> Option<&Vec<Receipt>>;
    fn get_chunk_from_event(&self, event: &EventId, chunk_seq: u64) -> Option<&Receipt>;
}

pub struct InMemoryScheduler {
    pub event_chunks: HashMap<EventId, Vec<Receipt>>,
    pub process_chunks: HashMap<ProcessId, Vec<Receipt>>,
    pub schedule: HashMap<ProcessId, VecDeque<Proposal>>,
    pub event_counter: HashMap<ProcessId, u64>,
    pub proposal_counter: u64,
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
    fn satisfy_proposal(&mut self, proposal: &Proposal, receipt: Receipt) -> Result<NextAction> {
        // This should always validate data to ensure that any state transitions always follow some
        // invariants. Keep effects at the end in case of reverts and use transactions if possible.
        // Todo: list these invariants someday

        // Extract values needed for validation before taking any mutable borrows
        let process = &proposal.process;
        let in_log_seq = receipt.in_log_seq;
        let in_event_seq = receipt.in_event_seq;
        let event_id = *self.event_counter.entry(process.clone()).or_insert(0);
        let event = proposal.event.clone().unwrap_or(EventId {
            app: process.app.clone(),
            proc: process.proc.clone(),
            seq: event_id,
        });

        let schedule = self
            .schedule
            .entry(process.clone())
            .or_insert(VecDeque::new());
        let event_chunks = self
            .event_chunks
            .entry(event.clone())
            .or_insert(vec![]);
        let process_chunks = self
            .process_chunks
            .entry(process.clone())
            .or_insert(vec![]);

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

            if let Some(last_call) = prev_receipt.syscalls.last() {
                match last_call {
                    Syscall::Call { proposal: _ } => {}
                    _ => {
                        bail!("Event already ended in previous chunk")
                    }
                }
            }

            if prev_receipt.proposal.process != receipt.proposal.process {
                bail!("Chunk process is mismatched with previous chunk")
            }
        }

        // Parse the receipt's syscalls
        let mut calls = Vec::new();
        let mut notif_proposals = Vec::new();
        let mut has_call_at_end = false;

        for (i, syscall) in receipt.syscalls.iter().enumerate() {
            match syscall {
                Syscall::Call { proposal } => {
                    calls.push(proposal.clone());
                    has_call_at_end = i + 1 == receipt.syscalls.len();
                }
                Syscall::Notify { proposal } => notif_proposals.push(proposal.clone()),
                _ => {}
            }
        }

        if calls.len() > 1 {
            bail!("Only one Call syscall is allowed");
        }

        if let Some(_) = calls.last() {
            if !has_call_at_end {
                bail!("Call must be at the end of syscalls list");
            }
        }

        // Get root chunk for promise resolution
        let root_chunk = event_chunks.first().unwrap().clone();
        let promise = root_chunk.proposal.promise.clone();
        let root_returns = root_chunk.returns.clone();

        let source_chunk_data = if let Some(ref p) = promise {
            // Query the chunk data in the promise
            let source_event = p.target;
            self.event_chunks
                .get(&source_event)
                .and_then(|chunks| chunks.first())
                .map(|chunk| (source_event, chunk.proposal.process.clone()))
        } else {
            None
        };

        // Effects
        schedule.pop_front();

        event_chunks.push(receipt.clone());

        self.process_chunks
            .entry(process.clone())
            .or_insert(vec![])
            .push(receipt);

        for nt in notif_proposals {
            self.schedule
                .entry(process.clone())
                .or_insert(VecDeque::new())
                .push_back(nt);
        }

        // Satisfy any existing promises
        if let Some((source_event, source_process)) = source_chunk_data {
            self.add_proposal(Proposal {
                event: Some(source_event),
                process: source_process,
                inputs: root_returns,
                promise: None,
            });
        }

        let action = NextAction {
            event,
            proposal: calls.last().cloned(),
        };

        Ok(action)
    }
    fn get_chunks_from_event(&self, event: &EventId) -> Option<&Vec<Receipt>> {
        self.event_chunks.get(event)
    }
    fn get_chunk_from_event(&self, event: &EventId, chunk_seq: u64) -> Option<&Receipt> {
        self.event_chunks.get(event)?.get(chunk_seq as usize)
    }
}
