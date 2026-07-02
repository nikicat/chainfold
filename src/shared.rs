//! [`Shared<T>`] — a thin newtype over `Arc<T>` whose [`Shared::share`]
//! method names the shared-ownership bump explicitly.
//!
//! Scattering `.clone()` on `Arc`s through call sites is noisy and easy to
//! misread as a deep clone; `x.share()` says exactly what happens (one more
//! owner of the same value, a refcount bump).
//!
//! Convention: `Shared<T>` is for plain shared-ownership handles (caches,
//! singleflight state). Bare `Arc` remains where its extra abilities are
//! actually used — `Weak` back-references and `Arc<dyn Trait>` coercion.

use std::ops::Deref;
use std::sync::Arc;

/// A reference-counted shared owner of a `T`. Clone-by-`share()`.
#[derive(Debug, Default)]
pub struct Shared<T: ?Sized>(Arc<T>);

impl<T> Shared<T> {
    /// Create the first shared owner of `value`.
    pub fn new(value: T) -> Self {
        Self(Arc::new(value))
    }
}

impl<T: ?Sized> Shared<T> {
    /// Create another shared owner of the same value (an `Arc` refcount
    /// bump).
    #[must_use]
    pub fn share(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T: ?Sized> Clone for Shared<T> {
    fn clone(&self) -> Self {
        self.share()
    }
}

impl<T: ?Sized> Deref for Shared<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}
