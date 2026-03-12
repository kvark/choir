//! Queue abstraction for crossbeam / loom compatibility.
//!
//! Under normal compilation, this re-exports `crossbeam_deque` types.
//! Under `cfg(loom)`, it provides a simple `Mutex<VecDeque>`-based
//! replacement so that loom can model the synchronization.

#[cfg(not(loom))]
pub(crate) use crossbeam_deque::{Injector, Steal};

#[cfg(loom)]
pub(crate) use self::loom_impl::{Injector, Steal};

#[cfg(loom)]
mod loom_impl {
    use loom::sync::Mutex;
    use std::collections::VecDeque;

    pub(crate) struct Injector<T> {
        inner: Mutex<VecDeque<T>>,
    }

    pub(crate) enum Steal<T> {
        Empty,
        Success(T),
        /// Never constructed under loom — exists only for match-arm compatibility.
        #[allow(dead_code)]
        Retry,
    }

    impl<T> Injector<T> {
        pub fn new() -> Self {
            Self {
                inner: Mutex::new(VecDeque::new()),
            }
        }

        pub fn push(&self, item: T) {
            self.inner.lock().unwrap().push_back(item);
        }

        pub fn steal(&self) -> Steal<T> {
            match self.inner.lock().unwrap().pop_front() {
                Some(item) => Steal::Success(item),
                None => Steal::Empty,
            }
        }

        pub fn is_empty(&self) -> bool {
            self.inner.lock().unwrap().is_empty()
        }
    }
}
