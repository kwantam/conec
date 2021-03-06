// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use super::CoordEvent;
use crate::consts::{BCAST_SWEEP_SECS, MAX_LOOPS};
use crate::types::{tagstream::TaggedInStream, OutStream};

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
use tokio::time::{interval, Duration, Interval};

#[derive(Debug, Error)]
pub(super) enum BroadcastChanError {
    #[error(display = "Coord gone unexpectedly")]
    Coord(#[source] mpsc::SendError),
    #[error(display = "No one receiving")]
    NoReceivers,
    #[error(display = "error receiving from client")]
    ClientRecv(#[source] std::io::Error),
    #[error(display = "Event channel closed")]
    EventsClosed,
    #[error(display = "Sweep timer disappeared unexpectedly")]
    SweepTimer,
}
def_into_error!(BroadcastChanError);

pub(super) enum BroadcastChanEvent {
    New(OutStream, TaggedInStream),
    Count(String, u64),
}

pub(super) struct BroadcastChanInner {
    chan: String,
    coord: mpsc::UnboundedSender<CoordEvent>,
    sender: mpsc::UnboundedSender<BroadcastChanEvent>,
    events: mpsc::UnboundedReceiver<BroadcastChanEvent>,
    fanout: BcastFanout,
    ref_count: usize,
    driver: Option<Waker>,
    sweep: Option<Interval>,
}

impl BroadcastChanInner {
    fn handle_events(&mut self, cx: &mut Context) -> Result<bool, BroadcastChanError> {
        match self
            .sweep
            .as_mut()
            .ok_or(BroadcastChanError::SweepTimer)?
            .poll_next_unpin(cx)
        {
            Poll::Pending => Ok(()),
            Poll::Ready(None) => Err(BroadcastChanError::SweepTimer),
            Poll::Ready(Some(_)) => {
                self.fanout.sweep = true;
                while self.sweep.as_mut().unwrap().poll_next_unpin(cx).is_ready() {}
                Ok(())
            }
        }?;

        use BroadcastChanEvent::*;
        let mut recvd = 0;
        loop {
            let event = match self.events.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => Err(BroadcastChanError::EventsClosed),
                Poll::Ready(Some(event)) => Ok(event),
            }?;
            match event {
                New(send, recv) => {
                    self.fanout.push(send, recv);
                    Ok(())
                }
                Count(from, sid) => self
                    .coord
                    .unbounded_send(CoordEvent::BroadcastCountRes(from, sid, self.fanout.len()))
                    .map_err(|e| BroadcastChanError::Coord(e.into_send_error())),
            }?;
            recvd += 1;
            if recvd >= MAX_LOOPS {
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
        let mut iters = 0;
        loop {
            let keep_going = self.handle_events(cx)?;
            self.drive_fanout(cx)?;
            if !keep_going {
                break;
            }
            iters += 1;
            if iters >= MAX_LOOPS {
                // break to let other threads run, but reschedule
                cx.waker().wake_by_ref();
                break;
            }
        }
        Ok(())
    }
}

def_ref!(BroadcastChanInner, BroadcastChanRef);
impl BroadcastChanRef {
    pub(super) fn new(
        chan: String,
        coord: mpsc::UnboundedSender<CoordEvent>,
        send: OutStream,
        recv: TaggedInStream,
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
                ref_count: 0,
                driver: None,
                sweep: None,
            }))),
            sender,
        )
    }
}

def_driver!(pub(self), BroadcastChanRef; pub(super), BroadcastChanDriver; BroadcastChanError);
impl BroadcastChanDriver {
    pub(super) fn new(inner: BroadcastChanRef) -> Self {
        {
            let mut inner_locked = inner.lock().unwrap();
            inner_locked
                .sweep
                .replace(interval(Duration::from_secs(BCAST_SWEEP_SECS)));
        }
        Self(inner)
    }
}

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
    pub(super) fn send(&self, msg: BroadcastChanEvent) {
        self.sender.unbounded_send(msg).ok();
    }
}

struct BcastSendReady {
    send: Option<OutStream>,
    flushing: bool,
    driver: Option<Waker>,
}

impl BcastSendReady {
    fn new(send: OutStream) -> Self {
        Self {
            send: Some(send),
            flushing: false,
            driver: None,
        }
    }

    fn flush(&mut self) {
        self.flushing = true;
        if let Some(task) = self.driver.take() {
            task.wake();
        }
    }
}

impl Future for BcastSendReady {
    type Output = Result<(OutStream, bool), <OutStream as Sink<Bytes>>::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        if self.send.is_none() {
            panic!("awaited future twice");
        }
        // will replace if we return Poll::Pending
        let driver = self.driver.take();

        // if we're flushing, keep flushing
        if self.flushing {
            match self.send.as_mut().unwrap().poll_flush_unpin(cx) {
                Poll::Pending => (),
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => self.flushing = false,
            }
        }

        // poll until it's ready to go
        match self.send.as_mut().unwrap().poll_ready_unpin(cx) {
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => Poll::Ready(Ok((self.send.take().unwrap(), self.flushing))),
            Poll::Pending => {
                self.driver.replace(match driver {
                    Some(w) if w.will_wake(cx.waker()) => w,
                    _ => cx.waker().clone(),
                });
                Poll::Pending
            }
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
        // will replace if we return Poll::Pending
        let driver = self.driver.take();

        let ret = if self.send.is_none() {
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
        };

        if let Poll::Pending = ret {
            self.driver.replace(match driver {
                Some(w) if w.will_wake(cx.waker()) => w,
                _ => cx.waker().clone(),
            });
        }
        ret
    }
}

// this is like a futures::stream::SelectAll, but it drops incoming streams when they produce an error
struct BcastFanin(FuturesUnordered<stream::StreamFuture<TaggedInStream>>);

impl BcastFanin {
    fn new() -> Self {
        Self(FuturesUnordered::new())
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn push(&mut self, recv: TaggedInStream) {
        self.0.push(recv.into_future())
    }
}

impl Stream for BcastFanin {
    type Item = <TaggedInStream as Stream>::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        loop {
            match self.0.poll_next_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some((None, _))) => (),
                Poll::Ready(Some((Some(item), remaining))) => {
                    if item.is_ok() {
                        self.push(remaining);
                    }
                    return Poll::Ready(Some(item));
                }
            }
        }
    }
}

struct BcastFanout {
    recv: BcastFanin,
    ready: Vec<OutStream>,
    waiting: FuturesUnordered<BcastSendReady>,
    flush_close: FuturesUnordered<BcastSendFlushClose>,
    buf: Option<Bytes>,
    closing: bool,
    sweep: bool,
}

impl BcastFanout {
    fn new(send: OutStream, recv: TaggedInStream) -> Self {
        let recv = {
            let mut tmp = BcastFanin::new();
            tmp.push(recv);
            tmp
        };
        let waiting = {
            let tmp = FuturesUnordered::new();
            tmp.push(BcastSendReady::new(send));
            tmp
        };
        Self {
            recv,
            ready: Vec::new(),
            waiting,
            flush_close: FuturesUnordered::new(),
            buf: None,
            closing: false,
            sweep: false,
        }
    }

    fn push(&mut self, send: OutStream, recv: TaggedInStream) {
        self.waiting.push(BcastSendReady::new(send));
        self.recv.push(recv);
    }

    fn len(&self) -> (usize, usize) {
        (
            self.recv.len(),
            self.ready.len() + self.waiting.len() + self.flush_close.len(),
        )
    }

    fn get_rwf(
        &mut self,
    ) -> (
        &mut Vec<OutStream>,
        &mut FuturesUnordered<BcastSendReady>,
        &mut FuturesUnordered<BcastSendFlushClose>,
    ) {
        (&mut self.ready, &mut self.waiting, &mut self.flush_close)
    }
}

impl Future for BcastFanout {
    type Output = Result<(), BroadcastChanError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        // XXX(broadcast hack)
        // Sending an empty frame is a hack to detect closed receivers.
        // We can get rid of this one we update to a newer version of quinn,
        // which will expose poll_stopped().
        //
        // Once we fix this hack, also need to remove the corresponding hack from
        // NonblockingInStream, TaggedBroadcastInStream, and TaglessBroadcastInStream.
        if self.sweep {
            self.sweep = false;
            self.buf.get_or_insert(Bytes::new());
        }

        // done when there's no one left listening
        if self.ready.is_empty() && self.waiting.is_empty() && self.flush_close.is_empty() {
            // all done
            return Poll::Ready(Ok(()));
        }

        let mut need_flush = false;
        let mut need_wait = false;
        let mut wrote = false;
        let mut returning = false;
        'outer: loop {
            // if closing, push all the ready sinks into the closer
            if self.closing {
                let (ready, _, flush_close) = self.get_rwf();
                ready
                    .drain(..)
                    .for_each(|s| flush_close.push(BcastSendFlushClose::new(s, true)));
            }
            // if returning, flush all sinks if we wrote, else return
            if returning {
                if wrote {
                    let (ready, waiting, flush_close) = self.get_rwf();
                    ready
                        .drain(..)
                        .for_each(|s| flush_close.push(BcastSendFlushClose::new(s, false)));
                    waiting.iter_mut().for_each(|w| w.flush());
                    wrote = false;
                } else if !need_wait && !need_flush {
                    return Poll::Pending;
                }
            }

            // empty the flusher / closer of finished sinks
            {
                need_flush = false;
                let ready_for_close = self.closing && self.ready.is_empty() && self.waiting.is_empty();
                let mut closed = 0;
                loop {
                    match self.flush_close.poll_next_unpin(cx) {
                        Poll::Pending if ready_for_close => return Poll::Pending,
                        Poll::Ready(None) if ready_for_close => return Poll::Ready(Ok(())),
                        Poll::Pending | Poll::Ready(None) => break,
                        Poll::Ready(Some(Err(_))) | Poll::Ready(Some(Ok(None))) => (),
                        Poll::Ready(Some(Ok(Some(s)))) => self.ready.push(s),
                    };
                    closed += 1;
                    if closed >= MAX_LOOPS {
                        // yield but immediately reschedule
                        cx.waker().wake_by_ref();
                        returning = true;
                        continue 'outer;
                    }
                }
            } // closed, ready_for_close go out of scope

            // slurp up all the writable sinks
            if !returning || need_wait {
                need_wait = false;
                let mut readied = 0;
                loop {
                    let (sink, flushing) = match self.waiting.poll_next_unpin(cx) {
                        Poll::Ready(None) => break,      // all sinks are writable now
                        Poll::Ready(Some(Ok(sf))) => sf, // ok means this sink is ready
                        Poll::Pending => {
                            // waiting for sinks to be writable
                            returning = true;
                            continue 'outer;
                        }
                        Poll::Ready(Some(Err(_))) => {
                            // err means this sink is gone
                            readied += 1;
                            continue;
                        }
                    };
                    if self.closing {
                        self.flush_close.push(BcastSendFlushClose::new(sink, true));
                        need_flush = true; // need to call poll_next on self.flush_close
                    } else if flushing {
                        self.flush_close.push(BcastSendFlushClose::new(sink, false));
                        need_flush = true; // need to call poll_next on self.flush_close
                    } else {
                        self.ready.push(sink);
                    }
                    readied += 1;
                    if readied >= MAX_LOOPS {
                        // yield but immediately reschedule
                        cx.waker().wake_by_ref();
                        returning = true;
                        continue 'outer;
                    }
                }
            } // readied goes out of scope

            // write something if we've got it buffered, then try to buffer something else
            if !returning && !self.closing {
                // if we get here, sink is ready for writing
                if let Some(item) = self.buf.take() {
                    wrote = true;
                    let (ready, waiting, flush_close) = self.get_rwf();
                    // write to all sinks in flush_close (guaranteed only flushers!) and ready
                    for mut sink in flush_close
                        .iter_mut()
                        .map(|f| f.take())
                        .flatten()
                        .chain(ready.drain(..))
                    {
                        // drop the sink if there was an error, otherwise wait on it again
                        if sink.start_send_unpin(item.clone()).is_ok() {
                            waiting.push(BcastSendReady::new(sink));
                            need_wait = true; // need to call poll_next on self.waiting
                        }
                    }
                }

                let mut errors = 0;
                loop {
                    match self.recv.poll_next_unpin(cx) {
                        Poll::Pending => returning = true,
                        Poll::Ready(None) => self.closing = true,
                        Poll::Ready(Some(Ok(item))) => {
                            self.buf.replace(item.freeze());
                        }
                        Poll::Ready(Some(Err(_))) => {
                            errors += 1;
                            if errors >= MAX_LOOPS {
                                // yield but immediately reschedule
                                cx.waker().wake_by_ref();
                                returning = true;
                            } else {
                                continue;
                            }
                        }
                    };
                    break;
                }
            }
        }
    }
}
