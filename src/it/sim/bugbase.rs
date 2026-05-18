use std::path::PathBuf;

use crate::it::sim::scenario::{FaultKind, SimScenario, run_for_scenario};

/// Minimize a failing scenario by stripping events and reducing parameters,
/// returning the smallest `SimScenario` that still fails.
pub(super) fn shrink_scenario(failing: &SimScenario) -> SimScenario {
    let mut best = failing.clone();

    // Strip fault events one at a time.
    let mut fault_changed = true;
    while fault_changed {
        fault_changed = false;
        let mut i = 0;
        while i < best.faults.len() {
            let mut candidate = best.clone();
            candidate.faults.remove(i);
            if run_for_scenario(&candidate).is_err() {
                best = candidate;
                fault_changed = true;
            } else {
                i += 1;
            }
        }
    }

    // Strip command events one at a time.
    let mut cmd_changed = true;
    while cmd_changed {
        cmd_changed = false;
        let mut i = 0;
        while i < best.commands.len() {
            let mut candidate = best.clone();
            candidate.commands.remove(i);
            if run_for_scenario(&candidate).is_err() {
                best = candidate;
                cmd_changed = true;
            } else {
                i += 1;
            }
        }
    }

    // Reduce node count toward the minimum of 3.
    while best.node_count > 3 {
        let mut candidate = best.clone();
        candidate.node_count -= 1;
        let n = candidate.node_count;
        candidate.faults.retain(|f| match &f.kind {
            FaultKind::KillNode(m) => *m <= n,
            FaultKind::PartitionNode(m) | FaultKind::HealNode(m) => *m <= n,
        });
        if run_for_scenario(&candidate).is_err() {
            best = candidate;
        } else {
            break;
        }
    }

    // Reduce simulation_secs via binary search toward the minimum of 30.
    let mut lo = 30u64;
    let mut hi = best.simulation_secs;
    while lo < hi {
        let mid = (lo + hi) / 2;
        let mut candidate = best.clone();
        candidate.simulation_secs = mid;
        candidate.faults.retain(|f| f.at_secs + 20 < mid);
        candidate.commands.retain(|c| c.at_secs + 30 < mid);
        if run_for_scenario(&candidate).is_err() {
            best = candidate;
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }

    best
}

/// Serialize `scenario` to `bugbase/<crc32>.json`, creating the directory if needed.
/// Returns the path of the written file.
pub(super) fn record_failure(scenario: &SimScenario) -> std::io::Result<PathBuf> {
    let json = serde_json::to_string_pretty(scenario).expect("scenario serialization failed");
    let hash = crc32fast::hash(json.as_bytes());
    let dir = std::path::Path::new("bugbase");
    std::fs::create_dir_all(dir)?;
    let path = dir.join(format!("{hash:08x}.json"));
    std::fs::write(&path, &json)?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::it::sim::scenario::run_for_scenario;

    /// Re-runs every scenario saved under `bugbase/` to verify bugs are fixed.
    /// Files remain until manually deleted after the fix is confirmed.
    #[test]
    #[serial_test::serial]
    fn bugbase_regression() {
        let dir = std::path::Path::new("bugbase");
        if !dir.exists() {
            return;
        }
        for entry in std::fs::read_dir(dir).expect("read bugbase dir failed") {
            let path = entry.expect("dir entry failed").path();
            if path.extension().is_some_and(|e| e == "json") {
                let json = std::fs::read_to_string(&path).expect("read json failed");
                let scenario: SimScenario =
                    serde_json::from_str(&json).expect("parse json failed");
                run_for_scenario(&scenario)
                    .unwrap_or_else(|e| panic!("{path:?} regressed: {e}"));
            }
        }
    }
}
