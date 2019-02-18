#[macro_export]
macro_rules! clone {
    ($($n:ident),+ => $($p:tt)*) => (
        {
            $( #[allow(unused_mut)] let mut $n = $n.clone(); )+
            $($p)*
        }
    );
}

#[macro_export]
macro_rules! contain {
    ($ty:ty; $($tt:tt)*) => {
        (|| -> $ty {
            $($tt)*
        })()
    };
    ($($tt:tt)*) => {
        (|| {
            $($tt)*
        })()
    };
}