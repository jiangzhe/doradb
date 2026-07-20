//! One-shot completion cells for asynchronous storage IO flows.
//!
//! `Completion<T>` stores a `CompletionResult<T>` so IO, redo, and commit
//! fanout paths can move detailed domain reports across thread boundaries
//! without promoting them to the public `Error` type too early. A completion
//! failure stores one cloneable bridge. Fanout clones only its inner `Arc`;
//! final error owners reconstruct independent reports with their own context
//! after leaving the state lock.

use crate::error::CompletionResult;
use event_listener::{Event, listener};
use parking_lot::Mutex;

enum CompletionState<T> {
    Running,
    Completed(CompletionResult<T>),
}

/// Shared terminal-status cell for one asynchronous IO flow.
///
/// Producers call [`Self::complete`] exactly once to publish the final result.
/// Waiters can either poll [`Self::completed_result`] or await
/// [`Self::wait_result`]. The stored value is propagated so multiple waiters can
/// observe equivalent terminal state without rebuilding reports under lock.
pub(crate) struct Completion<T> {
    state: Mutex<CompletionState<T>>,
    ev: Event,
}

impl<T> Completion<T> {
    /// Builds one completion cell in the running state.
    #[inline]
    pub(crate) fn new() -> Self {
        Completion {
            state: Mutex::new(CompletionState::Running),
            ev: Event::new(),
        }
    }

    /// Publishes the terminal result and wakes all current waiters.
    ///
    /// Repeated calls after the first completion are ignored.
    #[inline]
    pub(crate) fn complete(&self, value: CompletionResult<T>) {
        let mut state = self.state.lock();
        if matches!(&*state, CompletionState::Running) {
            *state = CompletionState::Completed(value);
            drop(state);
            self.ev.notify(usize::MAX);
        }
    }

    /// Returns the propagated terminal result if this completion has already
    /// finished.
    #[inline]
    pub(crate) fn completed_result(&self) -> Option<CompletionResult<T>>
    where
        T: Clone,
    {
        let state = self.state.lock();
        match &*state {
            CompletionState::Running => None,
            CompletionState::Completed(value) => Some(propagate_result(value)),
        }
    }

    /// Waits until completion and returns the propagated terminal result.
    #[inline]
    pub(crate) async fn wait_result(&self) -> CompletionResult<T>
    where
        T: Clone,
    {
        loop {
            listener!(self.ev => listener);
            if let Some(value) = self.completed_result() {
                return value;
            }
            listener.await;
        }
    }
}

impl<T> Default for Completion<T> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[inline]
fn propagate_result<T: Clone>(value: &CompletionResult<T>) -> CompletionResult<T> {
    match value {
        Ok(value) => Ok(value.clone()),
        Err(bridge) => Err(bridge.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::Completion;
    use crate::error::{CompletionErrorBridge, IoError, RuntimeError};
    use crate::io::BackendError;
    use error_stack::Report;
    use std::io::{Error as StdIoError, ErrorKind as IoErrorKind};

    #[test]
    fn test_completion_completed_result_is_stable() {
        let completion = Completion::<usize>::new();
        assert!(completion.completed_result().is_none());
        completion.complete(Ok(7));
        assert_eq!(completion.completed_result().unwrap().unwrap(), 7);
        assert_eq!(completion.completed_result().unwrap().unwrap(), 7);
    }

    #[test]
    fn test_completion_waiter_can_observe_precompleted_state() {
        smol::block_on(async {
            let completion = Completion::<usize>::new();
            completion.complete(Ok(11));
            assert_eq!(completion.wait_result().await.unwrap(), 11);
        });
    }

    #[test]
    fn test_completion_error_fanout_clones_bridge_without_reconstructing() {
        let completion = Completion::<usize>::new();
        let bridge = CompletionErrorBridge::capture(
            Report::new(IoError::from(IoErrorKind::BrokenPipe))
                .attach("test send completion: original detail"),
        );
        let identity = bridge.test_identity();
        completion.complete(Err(bridge));

        let first = completion.completed_result().unwrap().unwrap_err();
        let second = completion.completed_result().unwrap().unwrap_err();
        assert_eq!(first.test_identity(), identity);
        assert_eq!(second.test_identity(), identity);
        assert_eq!(first.test_reconstructions(), 0);
        let report = first.replace_context(RuntimeError::BufferPageAccess);
        assert_eq!(second.test_reconstructions(), 1);
        assert_eq!(
            report.downcast_ref::<IoError>().copied().map(IoError::kind),
            Some(IoErrorKind::BrokenPipe)
        );
        let output = format!("{report:?}");
        assert!(output.contains("original detail"));
        assert!(output.contains("test send completion"));
    }

    #[test]
    fn test_completion_error_propagates_backend_attachments() {
        let completion = Completion::<usize>::new();
        let backend_report = BackendError::submit(
            "test_backend",
            StdIoError::from_raw_os_error(libc::EIO),
            2,
            1,
            1,
        )
        .into_report();
        completion.complete(Err(CompletionErrorBridge::capture(
            backend_report.attach("complete test backend write: op_kind=write"),
        )));

        let report = completion
            .completed_result()
            .unwrap()
            .unwrap_err()
            .replace_context(RuntimeError::BufferPageAccess);
        assert!(report.downcast_ref::<BackendError>().is_some());
        let output = format!("{report:?}");
        assert!(output.contains("backend=test_backend"), "{output}");
        assert!(output.contains("op_kind=write"), "{output}");
        assert!(output.contains("complete test backend write"), "{output}");
    }

    #[test]
    fn test_completion_report_unexpected_eof_reports_io() {
        let report = CompletionErrorBridge::capture(
            Report::new(IoError::from(IoErrorKind::UnexpectedEof)).attach(
                "test completion short read: unexpected eof: actual_bytes=17, expected_bytes=4096",
            ),
        )
        .replace_context(RuntimeError::BufferPageAccess);
        assert_eq!(
            report.downcast_ref::<IoError>().copied().map(IoError::kind),
            Some(IoErrorKind::UnexpectedEof)
        );
        let output = format!("{report:?}");
        assert!(output.contains("unexpected eof"));
        assert!(output.contains("actual_bytes=17"));
        assert!(output.contains("expected_bytes=4096"));
        assert!(output.contains("test completion short read"));
    }

    #[test]
    fn test_completion_report_io_attaches_error_detail() {
        let err = StdIoError::new(IoErrorKind::PermissionDenied, "completion io denied");
        let message = format!("{}", err);

        let report = CompletionErrorBridge::capture(
            Report::new(IoError::from(err.kind())).attach(format!("test completion io: {err}")),
        )
        .replace_context(RuntimeError::BufferPageAccess);

        assert_eq!(
            report.downcast_ref::<IoError>().copied().map(IoError::kind),
            Some(IoErrorKind::PermissionDenied)
        );
        let output = format!("{report:?}");
        assert!(output.contains(&message));
        assert!(output.contains("test completion io"));
    }
}
