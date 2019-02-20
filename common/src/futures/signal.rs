use futures::prelude::*;
use futures::task::{self, Task};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub fn signal() -> (Sender, Receiver) {
    let inner = Arc::new(Signal::new(PollerImpl));
    (Sender(inner.clone()), Receiver(inner))
}

#[derive(Clone)]
pub struct Sender(Arc<Signal<PollerImpl>>);

impl Sender {
    pub fn is_signalled(&self) -> bool {
        self.0.is_signalled()
    }

    pub fn signal(&self) {
        self.0.signal();
    }
}

#[derive(Clone)]
pub struct Receiver(Arc<Signal<PollerImpl>>);

impl Future for Receiver {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.0.poll()
    }
}

trait Poller {
    fn poll(&self, signalled: &AtomicBool) -> bool;
}

struct PollerImpl;

impl Poller for PollerImpl {
    #[inline]
    fn poll(&self, signalled: &AtomicBool) -> bool {
        signalled.load(Ordering::Relaxed)
    }
}

struct Signal<P> {
    signalled: AtomicBool,
    waiting_receivers: Mutex<Vec<Task>>,
    poller: P,
}

impl<P: Poller> Signal<P> {
    pub fn new(poller: P) -> Self {
        Self {
            signalled: AtomicBool::new(false),
            waiting_receivers: Mutex::new(Vec::new()),
            poller,
        }
    }

    pub fn is_signalled(&self) -> bool {
        self.signalled.load(Ordering::Relaxed)
    }

    pub fn signal(&self) {
        if !self.signalled.compare_and_swap(false, true, Ordering::Relaxed) {
            self.notify();
        }
    }

    pub fn poll(&self) -> Poll<(), ()> {
        let mut registered = false;
        let r = loop {
            if self.poller.poll(&self.signalled) {
                break Async::Ready(());
            }
            if registered {
                break Async::NotReady;
            }
            self.register();
            registered = true;
        };
        if r.is_ready() && registered {
            self.unregister();
        }
        Ok(r)
    }

    fn notify(&self) {
        let mut tasks = self.waiting_receivers.lock();
        for task in tasks.drain(..) {
            task.notify();
        }
    }

    fn register(&self) {
        let mut tasks = self.waiting_receivers.lock();
        if tasks.iter().all(|t| !t.will_notify_current()) {
            tasks.push(task::current());
        }
    }

    fn unregister(&self) {
        let mut tasks = self.waiting_receivers.lock();
        tasks.retain(|t| !t.will_notify_current());
    }
}

#[cfg(test)]
mod test {
    use super::*;

    trait AssertTraits: Send {}
    impl AssertTraits for Sender {}
    impl AssertTraits for Receiver {}
}

pub mod pulse {
    use super::*;

    pub fn pulse() -> (Sender, Receiver) {
        let inner = Arc::new(Signal::new(PollerImpl));
        (Sender(inner.clone()), Receiver(inner))
    }

    #[derive(Clone)]
    pub struct Sender(Arc<Signal<PollerImpl>>);

    impl Sender {
        pub fn is_signalled(&self) -> bool {
            self.0.is_signalled()
        }

        pub fn signal(&self) {
            self.0.signal();
        }
    }

    pub struct Receiver(Arc<Signal<PollerImpl>>);

    impl Stream for Receiver {
        type Item = ();
        type Error = ();

        fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
            // TODO end stream when there are no senders left.
            Ok(self.0.poll()?.map(Option::from))
        }
    }

    struct PollerImpl;

    impl Poller for PollerImpl {
        #[inline]
        fn poll(&self, signalled: &AtomicBool) -> bool {
            signalled.compare_and_swap(true, false, Ordering::Relaxed)
        }
    }

    #[cfg(test)]
    mod test {
        use super::*;

        trait AssertTraits: Send {}
        impl AssertTraits for Sender {}
        impl AssertTraits for Receiver {}
    }
}