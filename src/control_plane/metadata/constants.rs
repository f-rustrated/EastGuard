// --- Keyspace Constants ---
// Full partition-key space covered by a topic's ranges. Lexicographic ordering on Vec<u8>.
// [0xFF] is a 1-byte placeholder — production should use &[0xFF; 32] for a 256-bit ring.
pub const KEYSPACE_MIN: &[u8] = &[];
pub const KEYSPACE_MAX: &[u8] = &[0xFF];

// --- Hot Range Detection Constants ---
pub const SPLIT_SEAL_THRESHOLD: u8 = 3;
