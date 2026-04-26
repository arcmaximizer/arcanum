#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct EventId {
    pub app: String,
    pub proc: String,
    pub seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct ProcessId {
    pub app: String,
    pub proc: String,
}
