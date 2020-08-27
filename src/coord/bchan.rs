// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use super::CoordEvent;
use crate::consts::{BCAST_QUEUE, MAX_LOOPS};
use crate::types::{InStream, OutStream};

use bytes::Bytes;
use err_derive::Error;
use futures::{channel::mpsc, prelude::*, stream::futures_unordered::FuturesUnordered};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use tokio::task::{JoinError, JoinHandle};

#[derive(Debug, Error)]
pub(super) enum BroadcastChanError {
    #[error(display = "Coord gone unexpectedly")]
    Coord(#[source] mpsc::SendError),
    #[error(display = "No one receiving")]
    NoReceivers,
    #[error(display = "forwarding failed")]
    Forward(#[source] JoinError),
    #[error(display = "BroadcastForward on empty BroadcastNew")]
    Empty,
    #[error(display = "error receiving from client")]
    ClientRecv(#[source] std::io::Error),
    #[error(display = "error sending to broadcast")]
    BcastSend(#[source] async_channel::SendError<Bytes>),
    #[error(display = "error receiving from broadcast")]
    BcastRecv(#[source] async_channel::RecvError),
}

pub(super) enum BroadcastChanEvent {
    RecvClose,
    RecvOpen(String, u32),
}

pub(super) struct BroadcastChanInner {
    chan: String,
    coord: mpsc::UnboundedSender<CoordEvent>,
    sender: mpsc::UnboundedSender<BroadcastChanEvent>,
    events: mpsc::UnboundedReceiver<BroadcastChanEvent>,
    send: BcastSend,
    recv: BcastRecv,
    driver: Option<Waker>,
    ref_count: usize,
    recv_count: usize,
}

pub(super) type BcastSend = async_channel::Sender<Bytes>;
pub(super) type BcastRecv = async_channel::Receiver<Bytes>;

impl BroadcastChanInner {
    fn handle_events(&mut self, cx: &mut Context) -> Result<bool, BroadcastChanError> {
        use BroadcastChanEvent::*;
        let mut accepted = 0;
        loop {
            let event = match self.events.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => unreachable!("BroadcastChanInner owns a sender; something is wrong"),
                Poll::Ready(Some(event)) => event,
            };
            match event {
                RecvClose => {
                    if let Some(x) = self.recv_count.checked_sub(1) {
                        self.recv_count = x;
                    }
                    if self.recv_count == 0 {
                        Err(BroadcastChanError::NoReceivers)
                    } else {
                        Ok(())
                    }
                }
                RecvOpen(from, sid) => {
                    let res = BroadcastNew(Some((self.sender.clone(), self.send.clone(), self.recv.clone())));
                    self.recv_count += 1;
                    self.coord
                        .unbounded_send(CoordEvent::NewBroadcastRes(from, sid, res))
                        .map_err(|e| e.into_send_error().into())
                }
            }?;
            accepted += 1;
            if accepted >= MAX_LOOPS {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn run_driver(&mut self, cx: &mut Context) -> Result<(), BroadcastChanError> {
        loop {
            if !self.handle_events(cx)? {
                return Ok(());
            }
        }
    }
}

def_ref!(BroadcastChanInner, BroadcastChanRef);
impl BroadcastChanRef {
    pub(super) fn new(
        chan: String,
        coord: mpsc::UnboundedSender<CoordEvent>,
    ) -> (Self, mpsc::UnboundedSender<BroadcastChanEvent>) {
        let (send, recv) = async_channel::bounded(BCAST_QUEUE);
        let (sender, events) = mpsc::unbounded();
        (
            Self(Arc::new(Mutex::new(BroadcastChanInner {
                chan,
                coord,
                sender: sender.clone(),
                events,
                send,
                recv,
                driver: None,
                ref_count: 0,
                recv_count: 0,
            }))),
            sender,
        )
    }
}

def_driver!(BroadcastChanRef, BroadcastChanDriver, BroadcastChanError);
impl Drop for BroadcastChanDriver {
    fn drop(&mut self) {
        let mut inner = self.0.lock().unwrap();
        // tell coordinator this channel is dead
        inner
            .coord
            .unbounded_send(CoordEvent::BroadcastClose(inner.chan.clone()))
            .ok();

        // take down channel without killing the world
        inner.coord.disconnect();
        inner.sender.close_channel();
        inner.events.close();
        inner.send.close();
        inner.recv.close();
    }
}

pub(super) struct BroadcastChan {
    #[allow(dead_code)]
    pub(super) inner: BroadcastChanRef,
    pub(super) sender: mpsc::UnboundedSender<BroadcastChanEvent>,
}

impl BroadcastChan {
    pub(super) fn new_broadcast(&self, to: String, sid: u32) -> Option<()> {
        self.sender.unbounded_send(BroadcastChanEvent::RecvOpen(to, sid)).ok()
    }
}

pub(super) struct BroadcastNew(Option<(mpsc::UnboundedSender<BroadcastChanEvent>, BcastSend, BcastRecv)>);

impl Drop for BroadcastNew {
    fn drop(&mut self) {
        if let Some((sender, _, _)) = self.0.take() {
            sender.unbounded_send(BroadcastChanEvent::RecvClose).ok();
        }
    }
}

type BcastJoinHandle = JoinHandle<Result<(), BroadcastChanError>>;
pub(super) struct BroadcastForward {
    future: futures::future::Join<BcastJoinHandle, BcastJoinHandle>,
    sender: mpsc::UnboundedSender<BroadcastChanEvent>,
}

impl BroadcastForward {
    pub(super) fn new(
        mut n_send: OutStream,
        mut n_recv: InStream,
        mut bn: BroadcastNew,
    ) -> Result<Self, BroadcastChanError> {
        match bn.0.take() {
            None => Err(BroadcastChanError::Empty),
            Some((sender, send, recv)) => {
                let fsend = tokio::spawn(async move {
                    loop {
                        send.send(match n_recv.try_next().await? {
                            None => return Ok(()),
                            Some(b) => b.freeze(),
                        })
                        .await?;
                    }
                });
                let frecv = tokio::spawn(async move {
                    loop {
                        n_send.send(recv.recv().await?).await?;
                    }
                });
                let future = futures::future::join(fsend, frecv);
                Ok(Self { future, sender })
            }
        }
    }
}

impl Drop for BroadcastForward {
    fn drop(&mut self) {
        self.sender.unbounded_send(BroadcastChanEvent::RecvClose).ok();
    }
}

impl Future for BroadcastForward {
    type Output = Result<(), BroadcastChanError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.future.poll_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready((f1, f2)) => match (f1, f2) {
                (Err(e), _) | (_, Err(e)) => Poll::Ready(Err(e.into())),
                (Ok(v1), Ok(v2)) => Poll::Ready(v1.and(v2)),
            },
        }
    }
}

/* *************************************************************************** */

struct BcastSendWritable(Option<mpsc::Sender<Bytes>>);
impl Future for BcastSendWritable {
    type Output = Result<mpsc::Sender<Bytes>, <mpsc::Sender<Bytes> as Sink<Bytes>>::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        if self.0.is_none() {
            panic!("awaited future twice");
        }

        match self.0.as_mut().unwrap().poll_ready_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => Poll::Ready(Ok(self.0.take().unwrap())),
        }
    }
}

struct BcastFanout {
    recv: mpsc::Receiver<Bytes>,                        // should be bounded to 32ish
    ready: Vec<mpsc::Sender<Bytes>>,                    // should be bounded to 1
    waiting: FuturesUnordered<BcastSendWritable>,
    buf: Option<Bytes>,
    driver: Option<Waker>,
    flushing: bool,
}

impl BcastFanout {
    fn new(recv: mpsc::Receiver<Bytes>, send: mpsc::Sender<Bytes>) -> Self {
        let ready = vec!(send);
        Self {
            recv,
            ready,
            waiting: FuturesUnordered::new(),
            buf: None,
            driver: None,
            flushing: false,
        }
    }

    fn push(&mut self, it: mpsc::Sender<Bytes>) {
        self.waiting.push(BcastSendWritable(Some(it)));
        if let Some(task) = self.driver.take() {
            task.wake();
        }
    }
}

impl Future for BcastFanout {
    type Output = Result<(), BroadcastChanError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match &self.driver {
            Some(w) if w.will_wake(cx.waker()) => (),
            _ => self.driver = Some(cx.waker().clone()),
        };

        loop {
            if self.buf.is_some() {
                // check whether we can write; if not, just break
                // otherwise, write the value
            }

            // now need to flush

            // now need to check for next incoming value on recv
        }

        Poll::Pending
    }
}
