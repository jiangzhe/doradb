use event_listener::Event;
use std::ops::Deref;

/// Wrapper on event to notify all waiters when being dropped.
#[repr(transparent)]
pub struct EventNotifyOnDrop(Event);

impl EventNotifyOnDrop {
    #[inline]
    pub fn new() -> Self {
        EventNotifyOnDrop(Event::new())
    }
}

impl Default for EventNotifyOnDrop {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for EventNotifyOnDrop {
    #[inline]
    fn drop(&mut self) {
        self.0.notify(usize::MAX);
    }
}

impl Deref for EventNotifyOnDrop {
    type Target = Event;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use event_listener::Listener;

    #[test]
    fn test_event_notify_on_drop() {
        let ev = EventNotifyOnDrop::default();
        let listener = ev.listen();
        drop(ev);
        // event dropped, so listener.wait() will immediately return.
        listener.wait();
    }
}
