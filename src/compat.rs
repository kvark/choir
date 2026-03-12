//! Compatibility layer for testing with [Loom](https://github.com/tokio-rs/loom).
//!
//! Under `cfg(loom)`, re-exports synchronization primitives from `loom`.
//! Otherwise, re-exports from `std`.
//!
//! Note: `Arc` is always re-exported from `std` because loom's `Arc` does not
//! support `self: &Arc<Self>` method receivers or `Display`. The concurrency
//! bugs we care about live in the Mutex/Condvar/atomic interactions, not `Arc`.

pub(crate) use std::sync::Arc;

#[cfg(not(loom))]
pub(crate) use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Condvar, Mutex, MutexGuard, RwLock,
};

#[cfg(loom)]
pub(crate) use loom::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Condvar, Mutex, MutexGuard, RwLock,
};

/// `Condvar::wait_while` wrapper — loom's `Condvar` does not provide `wait_while`.
#[cfg(not(loom))]
pub(crate) fn wait_while<'a, T>(
    condvar: &Condvar,
    guard: MutexGuard<'a, T>,
    condition: impl FnMut(&mut T) -> bool,
) -> MutexGuard<'a, T> {
    condvar.wait_while(guard, condition).unwrap()
}

/// `Condvar::wait_while` polyfill for loom.
#[cfg(loom)]
pub(crate) fn wait_while<'a, T>(
    condvar: &Condvar,
    mut guard: MutexGuard<'a, T>,
    mut condition: impl FnMut(&mut T) -> bool,
) -> MutexGuard<'a, T> {
    while condition(&mut *guard) {
        guard = condvar.wait(guard).unwrap();
    }
    guard
}
