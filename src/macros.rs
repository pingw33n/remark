macro_rules! clone {
    ($($n:ident),+ => move || $body:expr) => (
        {
            $( #[allow(unused_mut)] let mut $n = $n.clone(); )+
            move || $body
        }
    );
    ($($n:ident),+ => move |$p:tt| $body:expr) => (
        {
            $( #[allow(unused_mut)] let mut $n = $n.clone(); )+
            move |$p| $body
        }
    );
}