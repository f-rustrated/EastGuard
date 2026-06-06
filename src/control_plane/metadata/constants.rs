// --- Keyspace Constants ---
// Full partition-key space covered by a topic's ranges. Lexicographic ordering on Vec<u8>.
// [0xFF] is a 1-byte placeholder — production should use &[0xFF; 32] for a 256-bit ring.
pub const KEYSPACE_MIN: &[u8] = &[];
pub const KEYSPACE_MAX: &[u8] = &[0xFF];

// --- Hot Range Detection Constants ---
pub const SPLIT_SEAL_THRESHOLD: usize = 3;
pub const MEASUREMENT_WINDOW_MS: u64 = 300_000; // 5 min sliding window
pub const SPLIT_COOLDOWN_MS: u64 = 300_000; // 5 min cooldown after a split
pub const MERGE_SEAL_THRESHOLD: usize = 0; // both ranges must be fully idle
