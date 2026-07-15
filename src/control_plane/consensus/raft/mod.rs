use crate::control_plane::{NodeId, Replicas};

pub(crate) mod catch_up;
pub(crate) mod command;
pub(crate) mod errors;
pub(crate) mod log;
pub(crate) mod state;
pub(crate) mod storage;

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(crate) fn compute_replacement_replica_set(
    old: &[NodeId],
    dead_nodes: &[NodeId],
    live_nodes: &[NodeId],
) -> Replicas {
    let replication_factor = old.len();
    let mut new_set: Vec<NodeId> = old
        .iter()
        .filter(|n| !dead_nodes.contains(n))
        .cloned()
        .collect();
    for candidate in live_nodes {
        if new_set.len() >= replication_factor {
            break;
        }
        if !new_set.contains(candidate) && !dead_nodes.contains(candidate) {
            new_set.push(candidate.clone());
        }
    }

    Replicas::new(new_set)
}
