use futures::prelude::*;
use futures::task::{self, Task};
use parking_lot::Mutex;
use std::sync::Arc;

#[derive(Clone)]
pub struct Condvar(Arc<Mutex<Vec<Task>>>);

impl Condvar {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    pub fn notify_one(&self) -> bool {
        let mut tasks = self.0.lock();
        if tasks.len() > 0 {
            tasks.swap_remove(0).notify();
            true
        } else {
            false
        }
    }

    pub fn notify_all(&self) -> usize {
        let mut tasks = self.0.lock();
        let len = tasks.len();
        for task in tasks.drain(..) {
            task.notify()
        }
        len
    }

    fn register(&self) -> bool {
        let mut tasks = self.0.lock();
        if tasks.iter().all(|t| !t.will_notify_current()) {
            tasks.push(task::current());
            true
        } else {
            false
        }
    }
}

impl Stream for Condvar {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        Ok(if self.register() {
            Async::Ready(Some(()))
        } else {
            Async::NotReady
        })
    }
}
