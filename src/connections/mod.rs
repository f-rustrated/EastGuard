pub mod controller;
pub mod protocol;
pub(crate) mod reader;
pub(crate) mod writer;
use writer::*;
const LEN_PREFIX_SIZE: usize = size_of::<u32>();
pub(crate) const REQUEST_ID_SIZE: usize = size_of::<u64>();
/// Maximum frame body, including the request ID and excluding the length prefix.
pub(crate) const MAX_FRAME_SIZE: usize = 4 * 1024 * 1024;
