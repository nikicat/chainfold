//! [`Share`] — an extension trait naming the `Arc` refcount bump explicitly.
//!
//! Scattering `.clone()` on `Arc`s through call sites is noisy and easy to
//! misread as a deep clone; `x.share()` says exactly what happens (one more
//! owner of the same value, a refcount bump). The `clone_on_ref_ptr` lint
//! (Cargo.toml) keeps call sites on this spelling.

use std::sync::Arc;

/// Names the refcount bump on an `Arc`.
pub trait Share {
    /// Create another shared owner of the same value (a refcount bump).
    #[must_use]
    fn share(&self) -> Self;
}

impl<T: ?Sized> Share for Arc<T> {
    fn share(&self) -> Self {
        Arc::clone(self)
    }
}
