use super::CoordEvent;
use crate::consts::MAX_LOOPS;
use crate::types::{ConecConn, ControlMsg, CtrlStream};
use crate::util;

use err_derive::Error;
use futures::channel::mpsc;
use futures::prelude::*;
use quinn::IncomingUniStreams;
use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

#[derive(Debug, Error)]
pub enum CoordChanError {
    #[error(display = "Peer closed connection")]
    PeerClosed,
    #[error(display = "Stream poll: {:?}", _0)]
    StreamPoll(#[error(source, no_from)] io::Error),
    #[error(display = "Control sink: {:?}", _0)]
    Sink(#[error(source, no_from)] util::SinkError),
}

pub(super) struct CoordChanInner {
    conn: ConecConn,
    ctrl: CtrlStream,
    iuni: IncomingUniStreams,
    peer: String,
    coord: mpsc::UnboundedSender<CoordEvent>,
    ref_count: usize,
    driver: Option<Waker>,
    driver_lost: bool,
    to_send: VecDeque<ControlMsg>,
    flushing: bool,
}

impl CoordChanInner {
    // read the next message from the recv channel
    fn drive_ctrl_recv(&mut self, cx: &mut Context) -> Result<bool, CoordChanError> {
        let mut recvd = 0;
        loop {
            match self.ctrl.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => Err(CoordChanError::PeerClosed),
                Poll::Ready(Some(msg)) => msg.map_err(CoordChanError::StreamPoll),
            }?;
            recvd += 1;
            if recvd >= MAX_LOOPS {
                return Ok(true);
            }
        }
        Ok(false)
    }

    // send something on the send channel
    fn drive_ctrl_send(&mut self, cx: &mut Context) -> Result<bool, CoordChanError> {
        util::drive_ctrl_send(cx, &mut self.flushing, &mut self.ctrl, &mut self.to_send)
            .map_err(CoordChanError::Sink)
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
        conn: ConecConn,
        ctrl: CtrlStream,
        iuni: IncomingUniStreams,
        peer: String,
        coord: mpsc::UnboundedSender<CoordEvent>,
    ) -> Self {
        let mut to_send = VecDeque::new();
        // send hello at startup
        to_send.push_back(ControlMsg::CoHello);
        Self(Arc::new(Mutex::new(CoordChanInner {
            conn,
            ctrl,
            iuni,
            peer,
            coord,
            ref_count: 0,
            driver: None,
            driver_lost: false,
            to_send,
            flushing: false,
        })))
    }
}

#[must_use = "CoordChanDriver must be spawned!"]
pub(super) struct CoordChanDriver(pub(super) CoordChanRef);

impl Future for CoordChanDriver {
    type Output = Result<(), CoordChanError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let inner = &mut *self.0.lock().unwrap();
        match &inner.driver {
            Some(w) if w.will_wake(cx.waker()) => (),
            _ => inner.driver = Some(cx.waker().clone()),
        };
        loop {
            let mut keep_going = false;
            keep_going |= inner.drive_ctrl_recv(cx)?;
            if !inner.to_send.is_empty() || inner.flushing {
                keep_going |= inner.drive_ctrl_send(cx)?;
            }
            /*
            inner.handle_events();
            */
            if !keep_going {
                break;
            }
        }
        if inner.ref_count == 0 {
            // driver is the only one left holding a ref to CoordChan; kill driver
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

pub(super) struct CoordChan(pub(super) CoordChanRef);
