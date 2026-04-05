use event_listener::{Event, listener};
use parking_lot::Mutex;

/// Shared terminal-status cell for one asynchronous IO flow.
///
/// Producers call [`Self::complete`] exactly once to publish the final result.
/// Waiters can either poll [`Self::completed_result`] or await
/// [`Self::wait_result`]. The stored value is cloned so multiple
/// waiters can observe the same terminal state.
pub struct Completion<T> {
    state: Mutex<CompletionState<T>>,
    ev: Event,
}

enum CompletionState<T> {
    Running,
    Completed(T),
}

impl<T> Completion<T> {
    /// Builds one completion cell in the running state.
    #[inline]
    pub fn new() -> Self {
        Completion {
            state: Mutex::new(CompletionState::Running),
            ev: Event::new(),
        }
    }

    /// Publishes the terminal result and wakes all current waiters.
    ///
    /// Repeated calls after the first completion are ignored.
    #[inline]
    pub fn complete(&self, value: T) {
        let mut state = self.state.lock();
        if matches!(&*state, CompletionState::Running) {
            *state = CompletionState::Completed(value);
            drop(state);
            self.ev.notify(usize::MAX);
        }
    }
}

impl<T> Default for Completion<T> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone> Completion<T> {
    /// Returns the terminal result if this completion has already finished.
    #[inline]
    pub fn completed_result(&self) -> Option<T> {
        let state = self.state.lock();
        match &*state {
            CompletionState::Running => None,
            CompletionState::Completed(value) => Some(value.clone()),
        }
    }

    /// Waits until the completion reaches a terminal state and returns it.
    #[inline]
    pub async fn wait_result(&self) -> T {
        loop {
            listener!(self.ev => listener);
            if let Some(value) = self.completed_result() {
                return value;
            }
            listener.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Completion;

    #[test]
    fn test_completion_completed_result_is_stable() {
        let completion = Completion::new();
        assert!(completion.completed_result().is_none());
        completion.complete(7usize);
        assert_eq!(completion.completed_result(), Some(7));
        assert_eq!(completion.completed_result(), Some(7));
    }

    #[test]
    fn test_completion_waiter_can_observe_precompleted_state() {
        smol::block_on(async {
            let completion = Completion::new();
            completion.complete(11usize);
            assert_eq!(completion.wait_result().await, 11);
        });
    }
}
