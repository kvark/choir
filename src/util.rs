use super::SubIndex;
use std::iter::FromIterator;

#[cfg(loom)]
use loom::cell::UnsafeCell;
#[cfg(not(loom))]
use std::cell::UnsafeCell;

/// Helper data structure for holding per-task data.
/// Each element is expected to only be accessed zero or one time.
pub struct PerTaskData<T> {
    data: Box<[UnsafeCell<Option<T>>]>,
}

unsafe impl<T> Sync for PerTaskData<T> {}

impl<T> PerTaskData<T> {
    /// Take an element at a given index.
    ///
    /// # Safety
    /// Can't be executed more than once for any given index.
    #[cfg(not(feature = "loom"))]
    pub unsafe fn take(&self, index: SubIndex) -> T {
        (*self.data[index as usize].get()).take().unwrap()
    }

    /// Take an element at a given index.
    #[cfg(feature = "loom")]
    pub unsafe fn take(&self, index: SubIndex) -> T {
        self.data[index as usize].get_mut().deref().take().unwrap()
    }

    /// Return the length of the data.
    pub fn len(&self) -> SubIndex {
        self.data.len() as _
    }
}

impl<T> FromIterator<T> for PerTaskData<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self {
            data: iter.into_iter().map(Some).map(UnsafeCell::new).collect(),
        }
    }
}

#[cfg(test)]
#[cfg_attr(not(feature = "loom"), test)]
fn smoke() {
    let mut choir = super::Choir::new();
    let _worker1 = choir.add_worker("P1");

    let data: PerTaskData<u32> = (0..10).collect();
    choir
        .spawn("")
        .init_multi(data.len(), move |_, i| {
            let v = unsafe { data.take(i) };
            println!("v = {}", v);
        })
        .run()
        .join();
}

#[cfg(feature = "loom")]
#[test]
fn loom() {
    loom::model(smoke);
}
