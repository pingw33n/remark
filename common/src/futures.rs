pub mod signal;

use futures::prelude::*;

pub use signal::signal;
pub use signal::pulse::pulse;
use futures::Future;

pub type BoxFuture<T, E> = Box<Future<Item=T, Error=E> + Send + 'static>;
pub type BoxStream<T, E> = Box<Stream<Item=T, Error=E> + Send + 'static>;

pub trait FutureExt: Future {
    fn into_box(self)
        -> Box<Future<Item = Self::Item, Error = Self::Error> + Send + 'static>
        where Self: Sized + Send + 'static
    {
        Box::new(self)
    }

    fn inspect_err<F>(self, f: F) -> InspectErr<Self, F>
        where F: FnMut(&Self::Error),
              Self: Sized
    {
        InspectErr {
            future: self,
            f,
        }
    }

    fn map_all_unit(self) -> MapAllUnit<Self> where Self: Sized {
        MapAllUnit(self)
    }
}

impl<T: Future + Sized + Send + 'static> FutureExt for T {}

pub struct MapAllUnit<T>(T);

impl<T: Future> Future for MapAllUnit<T> {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.0.poll() {
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Ok(Async::Ready(_)) => Ok(Async::Ready(())),
            Err(_) => Err(()),
        }
    }
}

pub trait StreamExt: Stream {
    fn into_box(self)
        -> Box<Stream<Item = Self::Item, Error = Self::Error> + Send + 'static>
        where Self: Sized + Send + 'static
    {
        Box::new(self)
    }
}

impl<T: Stream + Send + 'static> StreamExt for T {}

pub struct InspectErr<U, F> {
    future: U,
    f: F,
}

impl<U, F> Future for InspectErr<U, F>
        where U: Future,
              F: FnMut(&U::Error) {
    type Item = U::Item;
    type Error = U::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.future.poll()
            .map_err(|e| {
                (self.f)(&e);
                e
            })
    }
}