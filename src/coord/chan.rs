use super::CoordEvent;
use crate::consts::MAX_LOOPS;
use crate::types::{ConecConnection, CtrlStream};

use futures::channel::mpsc;
use futures::prelude::*;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

pub(super) struct CoordChanInner {
    conn: ConecConnection,
    ctrl: CtrlStream,
    peer: String,
    coord: mpsc::UnboundedSender<CoordEvent>,
    ref_count: usize,
    driver: Option<Waker>,
    driver_lost: bool,
}

impl CoordChanInner {
    /// read the next message from the recv channel
    fn drive_recv(&mut self, cx: &mut Context) -> io::Result<bool> {
        let mut recvd = 0;
        loop {
            match self.ctrl.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => {
                    // return error, which kills driver loop
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionReset,
                        "peer closed",
                    ));
                }
                Poll::Ready(Some(msg)) => {
                    let msg = match msg {
                        Err(e) => return Err(e),
                        Ok(msg) => msg,
                    };
                    println!("{:?}", msg);
                }
            }
            recvd += 1;
            if recvd >= MAX_LOOPS {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

pub(super) struct CoordChanRef(Arc<Mutex<CoordChanInner>>);

impl std::ops::Deref for CoordChanRef {
    type Target = Mutex<CoordChanInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Clone for CoordChanRef {
    fn clone(&self) -> Self {
        self.lock().unwrap().ref_count += 1;
        Self(self.0.clone())
    }
}

impl Drop for CoordChanRef {
    fn drop(&mut self) {
        let inner = &mut *self.0.lock().unwrap();
        if let Some(x) = inner.ref_count.checked_sub(1) {
            inner.ref_count = x;
            if x == 0 {
                if let Some(task) = inner.driver.take() {
                    task.wake();
                }
            }
        }
    }
}

impl CoordChanRef {
    pub(super) fn new(
        conn: ConecConnection,
        ctrl: CtrlStream,
        peer: String,
        coord: mpsc::UnboundedSender<CoordEvent>,
    ) -> Self {
        Self(Arc::new(Mutex::new(CoordChanInner {
            conn,
            ctrl,
            peer,
            coord,
            ref_count: 0,
            driver: None,
            driver_lost: false,
        })))
    }
}

pub(super) struct CoordChan {
    pub(super) inner: CoordChanRef,
}

#[must_use = "CoordChanDriver must be spawned!"]
pub(super) struct CoordChanDriver(pub(super) CoordChanRef);

impl Future for CoordChanDriver {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let inner = &mut *self.0.lock().unwrap();
        match &inner.driver {
            Some(w) if w.will_wake(cx.waker()) => (),
            _ => {
                inner.driver = Some(cx.waker().clone());
            }
        };
        loop {
            let mut keep_going = false;
            keep_going |= inner.drive_recv(cx)?;
            /*
            inner.handle_events();
            keep_going |= inner.drive_send()?;
            */
            if !keep_going {
                break;
            }
        }
        if inner.ref_count == 0 {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

impl Drop for CoordChanDriver {
    fn drop(&mut self) {
        let mut inner = self.0.lock().unwrap();
        // mark driver lost in case anyone is still holding a ref to this channel
        inner.driver_lost = true;
        // tell the coordinator that this channel is dead
        inner
            .coord
            .unbounded_send(CoordEvent::ChanClose(inner.peer.clone()))
            .ok();
    }
}
