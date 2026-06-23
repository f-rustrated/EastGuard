//! The client's pool of known cluster node addresses — seeded from the bootstrap
//! list and grown as redirects and describe responses reveal more nodes. Append-only
//! and never empty, so picking a node to (re-)resolve against is infallible.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;

pub(crate) struct KnownNodes {
    nodes: ArcSwap<Vec<SocketAddr>>,
    cursor: AtomicUsize,
}

impl KnownNodes {
    /// `bootstrap` must be non-empty (the caller enforces it; an empty pool would mean
    /// the client could never reach the cluster).
    pub(crate) fn new(bootstrap: Vec<SocketAddr>) -> Self {
        Self {
            nodes: ArcSwap::from_pointee(bootstrap),
            cursor: AtomicUsize::new(0),
        }
    }

    /// Round-robin the next node to contact. Infallible — the pool is never empty.
    pub(crate) fn pick(&self) -> SocketAddr {
        let nodes = self.nodes.load();
        let i = self.cursor.fetch_add(1, Ordering::Relaxed) % nodes.len();
        nodes[i]
    }

    /// Record a newly-discovered node; no-op if already known. Grows the pool toward
    /// full cluster coverage so re-resolution isn't limited to the bootstrap set.
    pub(crate) fn remember(&self, addr: SocketAddr) {
        self.nodes.rcu(|current_nodes| {
            if current_nodes.contains(&addr) {
                // If it's already there, return the existing Arc (no allocation)
                return current_nodes.clone();
            }
            // Otherwise, clone the Vec, push the new addr, and swap it in
            let mut new_nodes = (**current_nodes).clone();
            new_nodes.push(addr);
            Arc::new(new_nodes)
        });
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.nodes.load().len()
    }
}
