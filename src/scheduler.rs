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
    pub promise: Option<Promise>,
}

pub trait Scheduler {
    fn add_proposal(&mut self, proposal: Proposal);
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
    fn add_proposal(&mut self, proposal: Proposal) {
        todo!()
    }
    fn get_next_proposal(&mut self, process: &ProcessId) -> Option<&Proposal> {
        self.schedule.get(process)?.get(0)
    }
    fn satisfy_proposal(&mut self, proposal: &Proposal, receipt: Receipt) -> Result<NextAction> {
        // This should always validate data to ensure that any state transitions always follow some
        // invariants. Keep effects at the end in case of reverts and use transactions if possible.
        // Todo: list these invariants someday

        // Proposal checks

        let mut schedule = self
            .schedule
            .entry(proposal.process)
            .or_insert(VecDeque::new());

        let first_proposal = schedule
            .get(0)
            .ok_or(anyhow!("No proposals exist in schedule"))?;
        if proposal != first_proposal {
            bail!("Proposal does not match first scheduled proposal")
        }

        let mut process_chunks = self
            .process_chunks
            .entry(proposal.process)
            .or_insert(vec![]);

        if receipt.in_log_seq != process_chunks.len() as u64 {
            bail!("Chunk is misaligned with chunk history")
        }

        // Event checks

        let event_id = self.event_counter.entry(proposal.process).or_insert(0);
        let event = if let Some(e) = proposal.event {
            e
        } else {
            EventId {
                app: proposal.process.app,
                proc: proposal.process.proc,
                seq: *event_id,
            }
        };

        let mut event_chunks = self.event_chunks.entry(event).or_insert(vec![]);
        if receipt.in_event_seq != event_chunks.len() as u64 {
            bail!("Chunk is misaligned with per-event log")
        }

        // Parse the receipt's syscalls
        let calls: Vec<Proposal> = receipt
            .syscalls
            .iter()
            .filter_map(|x| match x {
                Syscall::Call { proposal } => Some(*proposal),
                _ => None,
            })
            .collect();

        let notif_proposals: Vec<Proposal> = receipt
            .syscalls
            .iter()
            .filter_map(|x| match x {
                Syscall::Notify { proposal } => Some(*proposal),
                _ => None,
            })
            .collect();

        if calls.len() > 1 {
            bail!("Only one Call syscall is allowed");
        }

        if let Some(call) = calls.last() {
            if !receipt
                .syscalls
                .last()
                .map(|s| matches!(s, Syscall::Call { .. }))
                .unwrap_or(false)
            {
                bail!("Call must be at the end of syscalls list");
            }
        }

        // Effects

        schedule.pop_front();
        event_chunks.push(receipt);
        process_chunks.push(receipt);

        for nt in notif_proposals {
            let proc_schedule = self
                .schedule
                .entry(proposal.process)
                .or_insert(VecDeque::new());
            proc_schedule.push_back(nt);
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
