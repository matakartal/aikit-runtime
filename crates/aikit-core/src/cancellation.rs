//! Cooperative cancellation shared by the runtime and high-level Rust APIs.
//!
//! Cancellation is monotonic: once requested, every clone observes it forever. The token uses a
//! small `AtomicBool` + `Notify` pair instead of starting a task or depending on a runtime-specific
//! cancellation crate.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

struct CancellationState {
    cancelled: AtomicBool,
    notify: Notify,
}

/// Cloneable observation token carried by [`RunConfig`](crate::runtime::RunConfig).
#[derive(Clone)]
pub struct CancellationToken {
    inner: Arc<CancellationState>,
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for CancellationToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancellationToken")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

impl CancellationToken {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CancellationState {
                cancelled: AtomicBool::new(false),
                notify: Notify::new(),
            }),
        }
    }

    /// Return a caller-facing handle tied to this token.
    pub fn handle(&self) -> CancellationHandle {
        CancellationHandle {
            token: self.clone(),
        }
    }

    /// Request cancellation. This is idempotent and safe from any thread.
    pub fn cancel(&self) {
        if !self.inner.cancelled.swap(true, Ordering::AcqRel) {
            self.inner.notify.notify_waiters();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    /// Wait until cancellation is requested without losing a notification in the check/register
    /// race. Cancellation is monotonic, so the second check closes that race.
    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }

        let notified = self.inner.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.is_cancelled() {
            return;
        }
        notified.await;
    }
}

/// Cloneable caller-side cancellation control.
#[derive(Clone, Debug)]
pub struct CancellationHandle {
    token: CancellationToken,
}

impl CancellationHandle {
    pub fn cancel(&self) {
        self.token.cancel();
    }

    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancellation_is_monotonic_and_wakes_all_clones() {
        let token = CancellationToken::new();
        let waiter_a = token.clone();
        let waiter_b = token.clone();
        let a = tokio::spawn(async move { waiter_a.cancelled().await });
        let b = tokio::spawn(async move { waiter_b.cancelled().await });

        token.handle().cancel();
        a.await.unwrap();
        b.await.unwrap();
        token.cancelled().await;
        assert!(token.is_cancelled());
    }
}
