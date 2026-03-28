use crate::clusters::{NodeId, SwimNodeState};

use rand::{Rng, SeedableRng, rngs::StdRng};
use std::collections::VecDeque;
use std::ops::Deref;

// Used to decide who to ping. You don't want to waste network traffic pinging nodes you already know are dead.
pub(super) struct LiveNodeTracker {
    self_id: NodeId,
    nodes: VecDeque<NodeId>,
    rng: StdRng,
}

impl Deref for LiveNodeTracker {
    type Target = VecDeque<NodeId>;

    fn deref(&self) -> &Self::Target {
        &self.nodes
    }
}

impl LiveNodeTracker {
    pub(super) fn new(self_id: NodeId, rng_seed: u64) -> Self {
        Self {
            self_id,
            nodes: VecDeque::new(),
            rng: StdRng::seed_from_u64(rng_seed),
        }
    }

    pub(super) fn add(&mut self, node_id: NodeId) {
        if node_id == self.self_id || self.nodes.contains(&node_id) {
            return;
        }
        let selected = self.rng.gen_range(0..=self.nodes.len());
        self.nodes.insert(selected, node_id);
    }

    pub(super) fn remove(&mut self, node_id: &NodeId) {
        if let Some(index) = self.nodes.iter().position(|x| x == node_id) {
            self.nodes.remove(index);
        }
    }

    // LiveNodeTracker ensures that it never returns the node id it's running on
    pub(super) fn next(&mut self) -> Option<NodeId> {
        let node_id = self.nodes.pop_front()?;
        self.nodes.push_back(node_id.clone());
        Some(node_id)
    }

    pub(crate) fn update(&mut self, node_id: NodeId, state: &SwimNodeState) {
        match state {
            SwimNodeState::Alive => {
                self.add(node_id);
            }
            SwimNodeState::Suspect => {
                self.remove(&node_id);
            }
            SwimNodeState::Dead => {
                self.remove(&node_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(n: u16) -> NodeId {
        NodeId::new(format!("node-{}", n))
    }

    fn tracker() -> LiveNodeTracker {
        LiveNodeTracker::new(node(0), 0)
    }

    #[test]
    fn test_round_robin_basic() {
        let mut nodes = tracker();
        nodes.nodes.push_back(node(1));
        nodes.nodes.push_back(node(2));
        nodes.nodes.push_back(node(3));

        assert_eq!(nodes.next(), Some(node(1)));
        assert_eq!(nodes.next(), Some(node(2)));
        assert_eq!(nodes.next(), Some(node(3)));
        assert_eq!(nodes.next(), Some(node(1)));
    }

    #[test]
    fn test_remove_before_pointer() {
        let mut nodes = tracker();
        nodes.nodes = vec![node(1), node(2), node(3)].into();

        nodes.next();
        nodes.next();

        nodes.remove(&node(1));

        assert_eq!(nodes.nodes.len(), 2);
        assert_eq!(nodes.next(), Some(node(3)));
    }

    #[test]
    fn test_remove_at_pointer() {
        let mut nodes = tracker();
        nodes.nodes = vec![node(1), node(2), node(3)].into();
        nodes.next();

        nodes.remove(&node(2));
        assert_eq!(nodes.next(), Some(node(3)));
    }

    #[test]
    fn test_remove_last_element_wrap() {
        let mut nodes = tracker();
        nodes.nodes = vec![node(1), node(2), node(3)].into();

        nodes.next();
        nodes.next();

        nodes.remove(&node(3));

        assert_eq!(nodes.next(), Some(node(1)));
    }

    #[test]
    fn test_remove_after_pointer() {
        let mut nodes = tracker();
        nodes.nodes = vec![node(1), node(2), node(3)].into();

        nodes.remove(&node(3));

        assert_eq!(nodes.next(), Some(node(1)));
    }
}
