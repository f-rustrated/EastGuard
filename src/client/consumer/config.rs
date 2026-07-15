#[derive(Debug, Clone)]
pub enum DeliverySemantic {
    AtLeastOnce,
    AtMostOnceBestEffort,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitMode {
    Auto,
    Manual,
}

#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// The policy (Earliest/Latest) used to start consumption on newly assigned ranges.
    pub start_policy: StartPolicy,
    pub group_id: Option<String>,
    pub auto_commit_interval_ms: u64,
    pub delivery_semantic: DeliverySemantic,
    pub commit_mode: CommitMode,
}

impl ConsumerConfig {
    pub fn new(start_policy: StartPolicy) -> Self {
        Self {
            start_policy,
            group_id: None,
            auto_commit_interval_ms: 5000, // Default to 1 second for groups
            delivery_semantic: DeliverySemantic::AtLeastOnce,
            commit_mode: CommitMode::Auto,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartPolicy {
    Latest,
    Earliest,
}

impl std::str::FromStr for StartPolicy {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "latest" => Ok(StartPolicy::Latest),
            "earliest" => Ok(StartPolicy::Earliest),
            _ => anyhow::bail!("Start policy must be 'earliest' or 'latest'"),
        }
    }
}
