// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

macro_rules! def_ref {
    ($i:ident, $r:tt) => ( def_ref!($i, $r, pub(super)); );
    ($i:ident, $r:tt, $v:vis) => {
        $v struct $r(Arc<Mutex<$i>>);

        impl std::ops::Deref for $r {
            type Target = Mutex<$i>;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl Clone for $r {
            fn clone(&self) -> Self {
                self.lock().unwrap().ref_count += 1;
                Self(self.0.clone())
            }
        }

        impl Drop for $r {
            fn drop(&mut self) {
                let mut inner = self.lock().unwrap();
                if let Some(x) = inner.ref_count.checked_sub(1) {
                    inner.ref_count = x;
                }
                if inner.ref_count == 0 {
                    if let Some(task) = inner.driver.take() {
                        task.wake();
                    }
                }
            }
        }
    };
}

macro_rules! def_driver {
    ($r:ident, $d:tt, $e:ty) => ( def_driver!(pub(super), $r; pub(super), $d; $e); );
    ($rv:vis, $r:ident; $dv:vis, $d:tt; $e:ty) => {
        #[must_use = "$r must be spawned!"]
        $dv struct $d($rv $r);
        impl Future for $d {
            type Output = Result<(), $e>;
            fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
                let mut inner = self.0.lock().unwrap();
                match &inner.driver {
                    Some(w) if w.will_wake(cx.waker()) => (),
                    _ => inner.driver = Some(cx.waker().clone()),
                };
                inner.run_driver(cx)?;
                if inner.ref_count == 0 {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            }
        }
    };
}

macro_rules! def_cs_future {
    ($f:tt, $h:tt, $ok:ty, $e:ty, $d:meta) => {
        def_cs_future!($f, pub(self), $h, pub(self), $ok, $e, $d);
    };
    ($f:tt, $vh:vis, $h:tt, $vf:vis, $ok:ty, $e:ty, $d:meta) => {
        $vh type $h = oneshot::Sender<Result<$ok, $e>>;
        #[$d]
        pub struct $f($vf oneshot::Receiver<Result<$ok, $e>>);
        def_flat_future!($f, $ok, $e);
    };
}

macro_rules! def_flat_future {
    ($f:ty, $ok:ty, $e:ty) => {
        def_flat_future!($f, $ok, $e, Canceled, 0);
    };
    ($f:ty, $ok:ty, $e:ty, $v:tt, $fld:tt) => {
        impl Future for $f {
            type Output = Result<$ok, $e>;
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
                match self.$fld.poll_unpin(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Err(e)) => Poll::Ready(Err(<$e>::$v(e))),
                    Poll::Ready(Ok(res)) => Poll::Ready(res),
                }
            }
        }
    };
}

macro_rules! def_into_error {
    ($e: ty) => {
        impl From<$e> for std::io::Error {
            fn from(err: $e) -> Self {
                std::io::Error::new(std::io::ErrorKind::Other, err)
            }
        }
    };
}
