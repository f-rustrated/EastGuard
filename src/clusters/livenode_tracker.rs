use std::{collections::VecDeque, net::SocketAddr, ops::Deref};

use rand::{Rng, SeedableRng, rngs::StdRng};

// Used to decide who to ping. You don't want to waste network traffic pinging nodes you already know are dead.
#[derive(Default)]
pub(super) struct LiveNodeTracker {
    nodes: VecDeque<SocketAddr>,
}

impl Deref for LiveNodeTracker {
    type Target = VecDeque<SocketAddr>;

    fn deref(&self) -> &Self::Target {
        &self.nodes
    }
}

impl LiveNodeTracker {
    pub(super) fn add(&mut self, peer_addr: SocketAddr) {
        let mut rng = StdRng::from_entropy();
        let selected = rng.gen_range(0..=self.nodes.len());
        self.nodes.insert(selected, peer_addr);
    }

    pub(super) fn remove(&mut self, peer_addr: &SocketAddr) {
        // Find the index of the peer_addr
        if let Some(index) = self.nodes.iter().position(|x| x == peer_addr) {
            self.nodes.remove(index);
        }
    }

    pub(super) fn next(&mut self) -> Option<SocketAddr> {
        let peer = self.nodes.pop_front()?;
        self.nodes.push_back(peer);
        Some(peer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{}", port).parse().unwrap()
    }

    #[test]
    fn test_round_robin_basic() {
        let mut nodes = LiveNodeTracker::default();
        // Manually push to control order for this specific test
        nodes.nodes.push_back(addr(1));
        nodes.nodes.push_back(addr(2));
        nodes.nodes.push_back(addr(3));

        assert_eq!(nodes.next(), Some(addr(1))); // ptr becomes 1
        assert_eq!(nodes.next(), Some(addr(2))); // ptr becomes 2
        assert_eq!(nodes.next(), Some(addr(3))); // ptr becomes 0 (wrap)
        assert_eq!(nodes.next(), Some(addr(1)));
    }

    #[test]
    fn test_remove_before_pointer() {
        // Setup: [1, 2, 3], ptr is at 2 (pointing to 3)
        let mut nodes = LiveNodeTracker {
            nodes: vec![addr(1), addr(2), addr(3)].into(),
        };

        nodes.next();
        nodes.next();

        // Remove 1 (index 0).
        // Array becomes [2, 3].
        // 3 is now at index 1. Pointer should decrement to 1.
        nodes.remove(&addr(1));

        assert_eq!(nodes.nodes.len(), 2);
        assert_eq!(nodes.next(), Some(addr(3))); // Should still point to 3
    }

    #[test]
    fn test_remove_at_pointer() {
        // Setup: [1, 2, 3], ptr is at 1 (pointing to 2)
        let mut nodes = LiveNodeTracker {
            nodes: vec![addr(1), addr(2), addr(3)].into(),
        };
        nodes.next();

        // Remove 2.
        // Array becomes [1, 3].
        // Pointer is still 1, which now holds 3. This is correct behavior.
        nodes.remove(&addr(2));
        assert_eq!(nodes.next(), Some(addr(3)));
    }

    #[test]
    fn test_remove_last_element_wrap() {
        // Setup: [1, 2, 3], ptr is at 2 (pointing to 3)
        let mut nodes = LiveNodeTracker {
            nodes: vec![addr(1), addr(2), addr(3)].into(),
        };

        nodes.next();
        nodes.next();

        // Remove 3.
        // Array becomes [1, 2].
        // Pointer (2) is now out of bounds. Should reset to 0.
        nodes.remove(&addr(3));

        assert_eq!(nodes.next(), Some(addr(1)));
    }

    #[test]
    fn test_remove_after_pointer() {
        // Setup: [1, 2, 3], ptr is at 0 (pointing to 1)
        let mut nodes = LiveNodeTracker {
            nodes: vec![addr(1), addr(2), addr(3)].into(),
        };

        // Remove 3.
        // Array becomes [1, 2].
        // Pointer (0) is still valid and pointing to 1.
        nodes.remove(&addr(3));

        assert_eq!(nodes.next(), Some(addr(1)));
    }
}
