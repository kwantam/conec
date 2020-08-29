// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use super::CoordEvent;
use crate::consts::MAX_LOOPS;
use crate::types::{InStream, OutStream};

use bytes::Bytes;
use err_derive::Error;
use futures::{
    channel::mpsc,
    prelude::*,
    stream::{self, futures_unordered::FuturesUnordered},
};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

#[derive(Debug, Error)]
pub(super) enum BroadcastChanError {
    #[error(display = "Coord gone unexpectedly")]
    Coord(#[source] mpsc::SendError),
    #[error(display = "No one receiving")]
    NoReceivers,
    #[error(display = "error receiving from client")]
    ClientRecv(#[source] std::io::Error),
    #[error(display = "error sending to broadcast")]
    BcastSend(#[source] async_channel::SendError<Bytes>),
    #[error(display = "error receiving from broadcast")]
    BcastRecv(#[source] async_channel::RecvError),
}

pub(super) type BroadcastChanEvent = (OutStream, InStream);

pub(super) struct BroadcastChanInner {
    chan: String,
    coord: mpsc::UnboundedSender<CoordEvent>,
    sender: mpsc::UnboundedSender<BroadcastChanEvent>,
    events: mpsc::UnboundedReceiver<BroadcastChanEvent>,
    fanout: BcastFanout,
    driver: Option<Waker>,
    ref_count: usize,
}

impl BroadcastChanInner {
    fn handle_events(&mut self, cx: &mut Context) -> Result<bool, BroadcastChanError> {
        let mut accepted = 0;
        loop {
            let (send, recv) = match self.events.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => unreachable!("BroadcastChanInner owns a sender; something is wrong"),
                Poll::Ready(Some(event)) => event,
            };
            self.fanout.push(send, recv);
            accepted += 1;
            if accepted >= MAX_LOOPS {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn drive_fanout(&mut self, cx: &mut Context) -> Result<(), BroadcastChanError> {
        match self.fanout.poll_unpin(cx) {
            Poll::Pending => Ok(()),
            Poll::Ready(e @ Err(_)) => e,
            Poll::Ready(Ok(())) => Err(BroadcastChanError::NoReceivers),
        }
    }

    fn run_driver(&mut self, cx: &mut Context) -> Result<(), BroadcastChanError> {
        loop {
            let keep_going = self.handle_events(cx)?;
            self.drive_fanout(cx)?;
            if !keep_going {
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
        send: OutStream,
        recv: InStream,
    ) -> (Self, mpsc::UnboundedSender<BroadcastChanEvent>) {
        let (sender, events) = mpsc::unbounded();
        let fanout = BcastFanout::new(send, recv);
        (
            Self(Arc::new(Mutex::new(BroadcastChanInner {
                chan,
                coord,
                sender: sender.clone(),
                events,
                fanout,
                driver: None,
                ref_count: 0,
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
    }
}

pub(super) struct BroadcastChan {
    #[allow(dead_code)]
    pub(super) inner: BroadcastChanRef,
    pub(super) sender: mpsc::UnboundedSender<BroadcastChanEvent>,
}

impl BroadcastChan {
    pub(super) fn new_broadcast(&self, send: OutStream, recv: InStream) {
        self.sender.unbounded_send((send, recv)).ok();
    }
}

struct BcastSendReady(Option<OutStream>, bool);
impl Future for BcastSendReady {
    type Output = Result<(OutStream, bool), <OutStream as Sink<Bytes>>::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        if self.0.is_none() {
            panic!("awaited future twice");
        }

        // if we're flushing, keep flushing
        if self.1 {
            match self.0.as_mut().unwrap().poll_flush_unpin(cx) {
                Poll::Pending => (),
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => self.1 = false,
            }
        }

        // poll until it's ready to go
        match self.0.as_mut().unwrap().poll_ready_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => Poll::Ready(Ok((self.0.take().unwrap(), self.1))),
        }
    }
}

struct BcastSendClose(Option<OutStream>);
impl Future for BcastSendClose {
    type Output = Result<(), <OutStream as Sink<Bytes>>::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        if self.0.is_none() {
            Poll::Ready(Ok(()))
        } else {
            self.0.as_mut().unwrap().poll_close_unpin(cx)
        }
    }
}

// stream setup
// [ incoming from clients ] -> SelectAll -> BcastFanout -> [ outgoing to clients ]
type BcastFanin = stream::SelectAll<InStream>;
struct BcastFanout {
    recv: BcastFanin,
    ready: Vec<(OutStream, bool)>,
    waiting: FuturesUnordered<BcastSendReady>,
    closing: Option<FuturesUnordered<BcastSendClose>>,
    buf: Option<Bytes>,
}

impl BcastFanout {
    fn new(send: OutStream, recv: InStream) -> Self {
        let recv = {
            let mut tmp = stream::SelectAll::new();
            tmp.push(recv);
            tmp
        };
        let ready = vec![(send, false)];
        Self {
            recv,
            ready,
            waiting: FuturesUnordered::new(),
            closing: None,
            buf: None,
        }
    }

    fn push(&mut self, send: OutStream, recv: InStream) {
        self.waiting.push(BcastSendReady(Some(send), false));
        self.recv.push(recv);
    }

    fn get_ready_waiting(&mut self) -> (&mut Vec<(OutStream, bool)>, &FuturesUnordered<BcastSendReady>) {
        (&mut self.ready, &self.waiting)
    }

    fn get_ready_closing(
        &mut self,
    ) -> (
        &mut Vec<(OutStream, bool)>,
        &mut Option<FuturesUnordered<BcastSendClose>>,
    ) {
        (&mut self.ready, &mut self.closing)
    }
}

impl Future for BcastFanout {
    type Output = Result<(), BroadcastChanError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        // done when there's no one left listening
        if self.ready.is_empty() && self.waiting.is_empty() && self.closing.as_ref().map_or(true, |c| c.is_empty())
        {
            // all done
            return Poll::Ready(Ok(()));
        }

        let mut need_wait_poll: bool;
        'outer: loop {
            if self.closing.is_some() {
                // push all the ready sinks into the closer
                let (ready, closing) = self.get_ready_closing();
                let closing = closing.as_mut().unwrap();
                ready.drain(..).for_each(|(s, _)| closing.push(BcastSendClose(Some(s))));

                // empty the closer of finished sinks
                let mut closed = 0;
                loop {
                    match self.closing.as_mut().unwrap().poll_next_unpin(cx) {
                        Poll::Pending => break 'outer,
                        Poll::Ready(None) => return Poll::Ready(Ok(())),
                        Poll::Ready(Some(_)) => (),
                    };
                    closed += 1;
                    if closed >= MAX_LOOPS {
                        // yield but immediately reschedule
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                }
            }

            // slurp up all the writable sinks
            let mut readied = 0;
            loop {
                need_wait_poll = false;
                let sink = match self.waiting.poll_next_unpin(cx) {
                    Poll::Pending => break 'outer,         // waiting for sinks to be writable
                    Poll::Ready(None) => break,            // all sinks are writable now
                    Poll::Ready(Some(Err(_))) => continue, // err means this sink is gone
                    Poll::Ready(Some(Ok(s))) => s,         // ok means this sink is ready
                };
                self.ready.push(sink);
                readied += 1;
                if readied >= MAX_LOOPS {
                    // yield but immediately reschedule
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }

            if self.closing.is_none() {
                // if we get here, sink is ready for writing
                if let Some(item) = self.buf.take() {
                    let (ready, waiting) = self.get_ready_waiting();
                    for (mut sink, _) in ready.drain(..) {
                        // drop the sink if there was an error, otherwise wait on it again
                        if sink.start_send_unpin(item.clone()).is_ok() {
                            // mark this sink as needing a flush, too
                            waiting.push(BcastSendReady(Some(sink), true));
                            need_wait_poll = true; // need to call poll_next on self.waiting
                        }
                    }
                }

                match self.recv.poll_next_unpin(cx) {
                    Poll::Pending if need_wait_poll => (),
                    Poll::Pending => break,
                    Poll::Ready(Some(Err(_))) => (),
                    Poll::Ready(Some(Ok(item))) => {
                        self.buf.replace(item.freeze());
                    }
                    Poll::Ready(None) => {
                        // nothing is waiting and stream is closed, so close all the sinks
                        let (ready, closing) = self.get_ready_closing();
                        closing.replace(ready.drain(..).map(|(s, _)| BcastSendClose(Some(s))).collect());
                    }
                }
            }
        }

        let mut errs = Vec::new();
        for (idx, (ref mut sink, ref mut flushing)) in self.ready.iter_mut().enumerate() {
            if *flushing {
                match sink.poll_flush_unpin(cx) {
                    Poll::Pending => (),
                    Poll::Ready(Err(_)) => errs.push(idx),
                    Poll::Ready(Ok(())) => *flushing = false,
                }
            }
        }
        Poll::Pending
    }
}
