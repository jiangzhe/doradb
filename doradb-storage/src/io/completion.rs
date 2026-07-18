//! One-shot completion cells for asynchronous storage IO flows.
//!
//! `Completion<T>` stores a `CompletionResult<T>` so IO, redo, and commit
//! fanout paths can move detailed domain reports across thread boundaries
//! without promoting them to the public `Error` type too early. A completion
//! report is not cloneable, so each waiter receives a propagated report with
//! the same top-level `CompletionErrorKind`, structured backend attachments
//! when present, and a propagation attachment. The original stored report keeps
//! any remaining producer-side detail.

use crate::error::{CompletionResult, propagate_completion_report};
use event_listener::{Event, listener};
use parking_lot::Mutex;

const PROPAGATE_ATTACHMENT: &str = "propagate from other threads";

enum CompletionState<T> {
    Running,
    Completed(CompletionResult<T>),
}

/// Shared terminal-status cell for one asynchronous IO flow.
///
/// Producers call [`Self::complete`] exactly once to publish the final result.
/// Waiters can either poll [`Self::completed_result`] or await
/// [`Self::wait_result`]. The stored value is propagated so multiple waiters can
/// observe equivalent terminal state without cloning non-cloneable reports.
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
        Err(report) => Err(propagate_completion_report(report, PROPAGATE_ATTACHMENT)),
    }
}

#[cfg(test)]
mod tests {
    use super::Completion;
    use crate::error::{CompletionErrorKind, IoError};
    use crate::io::{
        IOBackendErrorPhase, IOBackendFailure, IOBackendOperationKind, IOBackendQueueState, IOKind,
        attach_backend_operation_kind,
    };
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
    fn test_completion_error_propagates_context_only() {
        let completion = Completion::<usize>::new();
        completion.complete(Err(CompletionErrorKind::from_send(
            Report::new(IoError::from(IoErrorKind::BrokenPipe)).attach("original detail"),
            "test send completion",
        )));

        let report = completion.completed_result().unwrap().unwrap_err();
        assert_eq!(*report.current_context(), CompletionErrorKind::Send);
        let output = format!("{report:?}");
        assert!(output.contains("propagate from other threads"));
        assert!(!output.contains("original detail"));
    }

    #[test]
    fn test_completion_error_propagates_backend_attachments() {
        let completion = Completion::<usize>::new();
        let backend_report = IOBackendFailure::report(
            "test_backend",
            IOBackendErrorPhase::Submit,
            StdIoError::from_raw_os_error(libc::EIO),
            2,
            IOBackendQueueState::submit(1, 1),
        );
        let backend_report = attach_backend_operation_kind(backend_report, Some(IOKind::Write));
        completion.complete(Err(CompletionErrorKind::from_io(
            IoError::report_backend(&backend_report, "original backend context"),
            "test backend completion",
        )));

        let report = completion.completed_result().unwrap().unwrap_err();
        assert!(report.downcast_ref::<IOBackendFailure>().is_some());
        assert!(report.downcast_ref::<IOBackendOperationKind>().is_some());
        let output = format!("{report:?}");
        assert!(output.contains("backend=test_backend"), "{output}");
        assert!(output.contains("propagate from other threads"), "{output}");
        assert!(!output.contains("original backend context"), "{output}");
    }

    #[test]
    fn test_completion_report_unexpected_eof_reports_io() {
        let report = CompletionErrorKind::from_io(
            Report::new(IoError::from(IoErrorKind::UnexpectedEof))
                .attach("unexpected eof: actual_bytes=17, expected_bytes=4096"),
            "test completion short read",
        );
        assert_eq!(
            *report.current_context(),
            CompletionErrorKind::Io(IoErrorKind::UnexpectedEof)
        );
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

        let report = CompletionErrorKind::from_io(
            Report::new(IoError::from(err.kind())).attach(format!("{err}")),
            "test completion io",
        );

        assert_eq!(
            *report.current_context(),
            CompletionErrorKind::Io(IoErrorKind::PermissionDenied)
        );
        assert_eq!(
            report.downcast_ref::<IoError>().copied().map(IoError::kind),
            Some(IoErrorKind::PermissionDenied)
        );
        let output = format!("{report:?}");
        assert!(output.contains(&message));
        assert!(output.contains("test completion io"));
    }
}
