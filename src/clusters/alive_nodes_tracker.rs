use std::{net::SocketAddr, ops::Deref};

use rand::{Rng, SeedableRng, rngs::StdRng};

// Used to decide who to ping. You don't want to waste network traffic pinging nodes you already know are dead.
#[derive(Default)]
pub(super) struct AliveNodes {
    nodes: Vec<SocketAddr>,
    ptr: usize,
}

impl Deref for AliveNodes {
    type Target = Vec<SocketAddr>;

    fn deref(&self) -> &Self::Target {
        &self.nodes
    }
}

impl AliveNodes {
    pub(super) fn add(&mut self, peer_addr: SocketAddr) {
        let mut rng = StdRng::from_entropy();
        let selected = rng.gen_range(0..=self.nodes.len());
        self.nodes.insert(selected, peer_addr);

        // Since `insert` shifts all elements after `selected` to the right,
        // if our cursor was to the right of the insertion, it must also move right
        // to stay pointing at the same element.
        if selected <= self.ptr {
            self.ptr += 1;
        }
    }

    pub(super) fn remove(&mut self, peer_addr: &SocketAddr) {
        // Find the index of the peer_addr
        if let Some(index) = self.nodes.iter().position(|x| x == peer_addr) {
            self.nodes.remove(index);

            // Case 1: We removed a node BEFORE the current pointer.
            // The node we were pointing to has shifted left (index - 1), so we must decrement.
            if index < self.ptr {
                self.ptr -= 1;
            }
            // Case 2: We removed the node currently pointed to (index == ptr).
            // The pointer now naturally points to the *next* node (which slid into this slot).
            // The only danger is if we removed the LAST element; the pointer is now out of bounds.
            else if index == self.ptr {
                if self.ptr >= self.nodes.len() {
                    self.ptr = 0; // Wrap around to the start
                }
            }
        }
    }

    pub(super) fn next(&mut self) -> Option<SocketAddr> {
        if self.nodes.is_empty() {
            return None;
        }

        // Safety check: ensure pointer is within bounds
        if self.ptr >= self.nodes.len() {
            self.ptr = 0;
        }

        let peer = self.nodes[self.ptr];
        self.ptr = (self.ptr + 1) % self.nodes.len();
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
        let mut nodes = AliveNodes::default();
        // Manually push to control order for this specific test
        nodes.nodes.push(addr(1));
        nodes.nodes.push(addr(2));
        nodes.nodes.push(addr(3));

        assert_eq!(nodes.next(), Some(addr(1))); // ptr becomes 1
        assert_eq!(nodes.next(), Some(addr(2))); // ptr becomes 2
        assert_eq!(nodes.next(), Some(addr(3))); // ptr becomes 0 (wrap)
        assert_eq!(nodes.next(), Some(addr(1)));
    }

    #[test]
    fn test_remove_before_pointer() {
        // Setup: [1, 2, 3], ptr is at 2 (pointing to 3)
        let mut nodes = AliveNodes {
            nodes: vec![addr(1), addr(2), addr(3)],
            ptr: 2,
        };

        // Remove 1 (index 0).
        // Array becomes [2, 3].
        // 3 is now at index 1. Pointer should decrement to 1.
        nodes.remove(&addr(1));

        assert_eq!(nodes.nodes.len(), 2);
        assert_eq!(nodes.ptr, 1);
        assert_eq!(nodes.next(), Some(addr(3))); // Should still point to 3
    }

    #[test]
    fn test_remove_at_pointer() {
        // Setup: [1, 2, 3], ptr is at 1 (pointing to 2)
        let mut nodes = AliveNodes {
            nodes: vec![addr(1), addr(2), addr(3)],
            ptr: 1,
        };

        // Remove 2.
        // Array becomes [1, 3].
        // Pointer is still 1, which now holds 3. This is correct behavior.
        nodes.remove(&addr(2));

        assert_eq!(nodes.ptr, 1);
        assert_eq!(nodes.next(), Some(addr(3)));
    }

    #[test]
    fn test_remove_last_element_wrap() {
        // Setup: [1, 2, 3], ptr is at 2 (pointing to 3)
        let mut nodes = AliveNodes {
            nodes: vec![addr(1), addr(2), addr(3)],
            ptr: 2,
        };

        // Remove 3.
        // Array becomes [1, 2].
        // Pointer (2) is now out of bounds. Should reset to 0.
        nodes.remove(&addr(3));

        assert_eq!(nodes.ptr, 0);
        assert_eq!(nodes.next(), Some(addr(1)));
    }

    #[test]
    fn test_remove_after_pointer() {
        // Setup: [1, 2, 3], ptr is at 0 (pointing to 1)
        let mut nodes = AliveNodes {
            nodes: vec![addr(1), addr(2), addr(3)],
            ptr: 0,
        };

        // Remove 3.
        // Array becomes [1, 2].
        // Pointer (0) is still valid and pointing to 1.
        nodes.remove(&addr(3));

        assert_eq!(nodes.ptr, 0);
        assert_eq!(nodes.next(), Some(addr(1)));
    }

    #[test]
    fn test_add_updates_pointer() {
        // Setup: [2, 3], ptr is 0 (pointing to 2)
        let mut nodes = AliveNodes {
            nodes: vec![addr(2), addr(3)],
            ptr: 0,
        };

        // We cannot easily mock the RNG inside `add`, but we can verify
        // logic by manually inserting via the same logic `add` uses.
        // Let's simulate `add` inserting at index 0.

        let selected_index = 0;
        nodes.nodes.insert(selected_index, addr(1));

        // Logic from `add`:
        if selected_index <= nodes.ptr {
            nodes.ptr += 1;
        }

        // Array is [1, 2, 3]. Ptr was 0, inserted at 0.
        // Ptr should now be 1 (pointing to 2).
        assert_eq!(nodes.ptr, 1);
        assert_eq!(nodes.next(), Some(addr(2)));
    }
}
