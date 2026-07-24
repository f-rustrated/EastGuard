use crate::client::ClientError;

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

    pub(crate) fn validate(&self) -> Result<(), ClientError> {
        if self.group_id.as_ref().is_some_and(|group| group.is_empty()) {
            return Err(ClientError::invalid_configuration(
                "consumer.group_id",
                "must not be empty",
            ));
        }
        if self.group_id.is_some()
            && self.commit_mode == CommitMode::Auto
            && self.auto_commit_interval_ms == 0
        {
            return Err(ClientError::invalid_configuration(
                "consumer.auto_commit_interval_ms",
                "must be greater than zero when group auto-commit is enabled",
            ));
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_group_id() {
        let mut config = ConsumerConfig::new(StartPolicy::Earliest);
        config.group_id = Some(String::new());

        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_zero_group_auto_commit_interval() {
        let mut config = ConsumerConfig::new(StartPolicy::Earliest);
        config.group_id = Some("group".to_string());
        config.auto_commit_interval_ms = 0;

        assert!(config.validate().is_err());
    }
}
