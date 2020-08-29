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

struct BcastSendFlushClose {
    send: Option<OutStream>,
    closing: bool,
    driver: Option<Waker>,
}

impl BcastSendFlushClose {
    fn new(send: OutStream, closing: bool) -> Self {
        Self {
            send: Some(send),
            closing,
            driver: None,
        }
    }

    fn take(&mut self) -> Option<OutStream> {
        if let Some(task) = self.driver.take() {
            task.wake();
        }
        self.send.take()
    }
}

impl Future for BcastSendFlushClose {
    type Output = Result<Option<OutStream>, <OutStream as Sink<Bytes>>::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match &self.driver {
            Some(w) if w.will_wake(cx.waker()) => (),
            _ => self.driver = Some(cx.waker().clone()),
        };

        if self.send.is_none() {
            Poll::Ready(Ok(None))
        } else if self.closing {
            match self.send.as_mut().unwrap().poll_close_unpin(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => Poll::Ready(Ok(None)),
            }
        } else {
            match self.send.as_mut().unwrap().poll_flush_unpin(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => Poll::Ready(Ok(self.send.take())),
            }
        }
    }
}

// stream setup
// [ incoming from clients ] -> SelectAll -> BcastFanout -> [ outgoing to clients ]
type BcastFanin = stream::SelectAll<InStream>;
struct BcastFanout {
    recv: BcastFanin,
    ready: Vec<OutStream>,
    waiting: FuturesUnordered<BcastSendReady>,
    flush_close: FuturesUnordered<BcastSendFlushClose>,
    buf: Option<Bytes>,
    closing: bool,
}

impl BcastFanout {
    fn new(send: OutStream, recv: InStream) -> Self {
        let recv = {
            let mut tmp = stream::SelectAll::new();
            tmp.push(recv);
            tmp
        };
        let ready = vec![send];
        Self {
            recv,
            ready,
            waiting: FuturesUnordered::new(),
            flush_close: FuturesUnordered::new(),
            buf: None,
            closing: false,
        }
    }

    fn push(&mut self, send: OutStream, recv: InStream) {
        self.waiting.push(BcastSendReady(Some(send), false));
        self.recv.push(recv);
    }

    fn get_rwf(
        &mut self,
    ) -> (
        &mut Vec<OutStream>,
        &FuturesUnordered<BcastSendReady>,
        &mut FuturesUnordered<BcastSendFlushClose>,
    ) {
        (&mut self.ready, &self.waiting, &mut self.flush_close)
    }
}

impl Future for BcastFanout {
    type Output = Result<(), BroadcastChanError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        // done when there's no one left listening
        if self.ready.is_empty() && self.waiting.is_empty() && self.flush_close.is_empty() {
            // all done
            return Poll::Ready(Ok(()));
        }

        let mut need_flush: bool;
        let mut need_wait: bool;
        loop {
            // push all the ready sinks into the closer
            if self.closing {
                let (ready, _, flush_close) = self.get_rwf();
                ready
                    .drain(..)
                    .for_each(|s| flush_close.push(BcastSendFlushClose::new(s, true)));
            }

            // empty the flusher / closer of finished sinks
            let mut closed = 0;
            loop {
                need_flush = false;
                match self.flush_close.poll_next_unpin(cx) {
                    Poll::Pending if self.closing => return Poll::Pending,
                    Poll::Ready(None) if self.closing => return Poll::Ready(Ok(())),
                    Poll::Pending | Poll::Ready(None) => break,
                    Poll::Ready(Some(Err(_))) => continue,
                    Poll::Ready(Some(Ok(None))) => (),
                    Poll::Ready(Some(Ok(Some(s)))) => self.ready.push(s),
                };
                closed += 1;
                if closed >= MAX_LOOPS {
                    // yield but immediately reschedule
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }

            // slurp up all the writable sinks
            let mut readied = 0;
            loop {
                need_wait = false;
                let (sink, flushing) = match self.waiting.poll_next_unpin(cx) {
                    Poll::Pending => return Poll::Pending, // waiting for sinks to be writable
                    Poll::Ready(None) => break,            // all sinks are writable now
                    Poll::Ready(Some(Err(_))) => continue, // err means this sink is gone
                    Poll::Ready(Some(Ok(sf))) => sf,       // ok means this sink is ready
                };
                if self.closing {
                    self.flush_close.push(BcastSendFlushClose::new(sink, true));
                    need_flush = true;
                } else if flushing {
                    self.flush_close.push(BcastSendFlushClose::new(sink, false));
                    need_flush = true;
                } else {
                    self.ready.push(sink);
                }
                readied += 1;
                if readied >= MAX_LOOPS {
                    // yield but immediately reschedule
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }

            if !self.closing {
                // if we get here, sink is ready for writing
                if let Some(item) = self.buf.take() {
                    let (ready, waiting, flush_close) = self.get_rwf();
                    // drain flushers into ready --- we're about to write to them again
                    for fc in flush_close.iter_mut() {
                        if let Some(sink) = fc.take() {
                            ready.push(sink);
                        }
                    }
                    for mut sink in ready.drain(..) {
                        // drop the sink if there was an error, otherwise wait on it again
                        if sink.start_send_unpin(item.clone()).is_ok() {
                            // mark this sink as needing a flush, too
                            // XXX in principle we could wait to flush until just before we return...
                            waiting.push(BcastSendReady(Some(sink), true));
                            need_wait = true; // need to call poll_next on self.waiting
                        }
                    }
                }

                match self.recv.poll_next_unpin(cx) {
                    Poll::Pending if need_wait || need_flush => (),
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Some(Err(_))) => (),
                    Poll::Ready(Some(Ok(item))) => {
                        self.buf.replace(item.freeze());
                    }
                    Poll::Ready(None) => self.closing = true,
                }
            }
        }
    }
}
