use crate::clusters::Member;

// How many times a specific update should be gossiped before we stop prioritizing it.
// log(N) is usually a good number, but 3-5 is fine for small/medium clusters.
const BROADCAST_LIMIT: u8 = 3;

// Max number of updates to put in a single packet (to prevent packet fragmentation)
const GOSSIP_SIZE: usize = 6;

#[derive(Debug, Clone)]
struct Broadcast {
    member: Member,
    transmits_remaining: u8,
}
