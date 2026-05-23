#[cfg(any(test, debug_assertions))]
pub trait TAssertInvariant {
    fn assert_invariants(&self);
}
