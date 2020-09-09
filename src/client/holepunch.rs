// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use crate::consts::{HOLEPUNCH_MILLIS, HOLEPUNCH_NPKTS, MAX_LOOPS};

use err_derive::Error;
use futures::{
    channel::mpsc,
    prelude::*,
    stream::{repeat, Repeat, SelectAll, Take, Zip},
};
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use tokio::time::{interval, Duration, Interval};

// messages we will be receiving from chan driver
pub(super) type HolepunchEvent = SocketAddr;

/// Holepunch driver errors
#[derive(Debug, Error)]
pub(super) enum HolepunchError {
    /// Events channel closed
    #[error(display = "Events channel closed")]
    EventsClosed,
    /// Sending on socket
    #[error(display = "Sending on socket: {:?}", _0)]
    Socket(#[source] io::Error),
}
def_into_error!(HolepunchError);

pub(super) struct HolepunchInner {
    socket: UdpSocket,
    sender: mpsc::UnboundedSender<HolepunchEvent>,
    events: mpsc::UnboundedReceiver<HolepunchEvent>,
    timers: SelectAll<Take<Zip<Interval, Repeat<SocketAddr>>>>,
    ref_count: usize,
    driver: Option<Waker>,
}

impl HolepunchInner {
    fn handle_events(&mut self, cx: &mut Context) -> Result<bool, HolepunchError> {
        let mut handled = 0;
        loop {
            let event = match self.events.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => Err(HolepunchError::EventsClosed),
                Poll::Ready(Some(event)) => Ok(event),
            }?;
            let timer = interval(Duration::from_millis(HOLEPUNCH_MILLIS))
                .zip(repeat(event))
                .take(HOLEPUNCH_NPKTS);
            self.timers.push(timer);
            handled += 1;
            if handled >= MAX_LOOPS {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn drive_timer(&mut self, cx: &mut Context) -> Result<bool, HolepunchError> {
        let mut handled = 0;
        loop {
            let addr = match self.timers.poll_next_unpin(cx) {
                Poll::Pending | Poll::Ready(None) => break,
                Poll::Ready(Some((_, addr))) => addr,
            };
            self.socket.send_to(b"knock knock it's conec", addr)?;
            handled += 1;
            if handled >= MAX_LOOPS {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn run_driver(&mut self, cx: &mut Context) -> Result<(), HolepunchError> {
        loop {
            let mut keep_going = false;
            keep_going |= self.handle_events(cx)?;
            keep_going |= self.drive_timer(cx)?;
            if !keep_going {
                return Ok(());
            }
        }
    }
}

def_ref!(HolepunchInner, HolepunchRef);
impl HolepunchRef {
    pub(super) fn new(socket: UdpSocket) -> (Self, mpsc::UnboundedSender<HolepunchEvent>) {
        let (sender, events) = mpsc::unbounded();
        (
            Self(Arc::new(Mutex::new(HolepunchInner {
                socket,
                sender: sender.clone(),
                events,
                timers: SelectAll::new(),
                ref_count: 0,
                driver: None,
            }))),
            sender,
        )
    }
}

def_driver!(HolepunchRef, HolepunchDriver, HolepunchError);
impl Drop for HolepunchDriver {
    fn drop(&mut self) {
        // if this dies, it takes everything with it
        let mut inner = self.0.lock().unwrap();
        inner.sender.close_channel();
        inner.events.close();
    }
}

// hold onto a ref so that when we drop the driver drops too
pub(super) struct Holepunch(pub(super) HolepunchRef);
