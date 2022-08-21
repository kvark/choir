use std::{
    fmt, mem, ops,
    ptr::NonNull,
    sync::atomic::{AtomicUsize, Ordering},
};

/// Note: `pub(super)` is only needed for a hack
#[derive(Debug)]
pub(super) struct LinearcInner<T: ?Sized> {
    pub(super) ref_count: AtomicUsize,
    pub(super) data: T,
}

/// Linear atomically referenced pointer.
/// Analogous to `Arc` but without `Weak` functionality,
/// and with an extra powers to treat the type as linear.
/// In particular, it supports atomic destruction with the extraction of data.
///
/// See <https://internals.rust-lang.org/t/de-facto-linear-types-and-arc-need-for-an-unwrap-or-drop-api/12939>
/// And <https://github.com/rust-lang/rust/pull/79665>
pub struct Linearc<T: ?Sized> {
    ptr: NonNull<LinearcInner<T>>,
}

unsafe impl<T: ?Sized> Send for Linearc<T> {}
unsafe impl<T: ?Sized> Sync for Linearc<T> {}

impl<T: ?Sized> Linearc<T> {
    #[inline]
    pub(super) fn from_inner(inner: Box<LinearcInner<T>>) -> Self {
        Self {
            ptr: unsafe { NonNull::new_unchecked(Box::into_raw(inner)) },
        }
    }

    /// Clone a given pointer.
    #[inline]
    pub fn clone(arc: &Self) -> Self {
        arc.clone()
    }

    fn into_box(arc: Self) -> Option<Box<LinearcInner<T>>> {
        let count = unsafe { arc.ptr.as_ref() }
            .ref_count
            .fetch_sub(1, Ordering::AcqRel);
        if count == 1 {
            let inner = unsafe { Box::from_raw(arc.ptr.as_ptr()) };
            mem::forget(arc);
            Some(inner)
        } else {
            mem::forget(arc);
            None
        }
    }

    /// Drop a pointer and return true if this was the last instance.
    #[inline]
    pub fn drop_last(arc: Self) -> bool {
        Linearc::into_box(arc).is_some()
    }
}

impl<T> Linearc<T> {
    /// Create a new pointer from data.
    #[inline]
    pub fn new(data: T) -> Self {
        Self::from_inner(Box::new(LinearcInner {
            ref_count: AtomicUsize::new(1),
            data,
        }))
    }

    /// Move out the value of this pointer if it's the last instance.
    pub fn into_inner(arc: Self) -> Option<T> {
        Linearc::into_box(arc).map(|inner| inner.data)
    }
}

impl<T: ?Sized> Clone for Linearc<T> {
    fn clone(&self) -> Self {
        unsafe { self.ptr.as_ref() }
            .ref_count
            .fetch_add(1, Ordering::Release);
        Self { ptr: self.ptr }
    }
}

impl<T: ?Sized> Drop for Linearc<T> {
    fn drop(&mut self) {
        let count = unsafe { self.ptr.as_ref() }
            .ref_count
            .fetch_sub(1, Ordering::AcqRel);
        if count == 1 {
            let _ = unsafe { Box::from_raw(self.ptr.as_ptr()) };
        }
    }
}

impl<T: ?Sized> ops::Deref for Linearc<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        &unsafe { self.ptr.as_ref() }.data
    }
}

impl<T: fmt::Debug> fmt::Debug for Linearc<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        unsafe { self.ptr.as_ref() }.fmt(formatter)
    }
}
