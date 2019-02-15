#[macro_export]
macro_rules! clone {
    ($($n:ident),+ => $($p:tt)*) => (
        {
            $( #[allow(unused_mut)] let mut $n = $n.clone(); )+
            $($p)*
        }
    );
}