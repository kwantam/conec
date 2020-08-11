use super::CoordEvent;
use crate::consts::MAX_LOOPS;
use crate::types::{ConecConn, ControlMsg, CtrlStream, FramedRecvStream};
use crate::util;

use err_derive::Error;
use futures::channel::mpsc;
use futures::prelude::*;
use quinn::{ConnectionError, IncomingUniStreams, SendStream};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use tokio_serde::{formats::SymmetricalBincode, SymmetricallyFramed};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

#[derive(Debug, Error)]
pub enum CoordChanError {
    #[error(display = "Peer closed connection")]
    PeerClosed,
    #[error(display = "Stream poll: {:?}", _0)]
    StreamPoll(#[error(source, no_from)] io::Error),
    #[error(display = "Control sink: {:?}", _0)]
    Sink(#[error(source, no_from)] util::SinkError),
    #[error(display = "Unexpected message from coordinator")]
    WrongMessage(ControlMsg),
    #[error(display = "Sending CoordEvent: {:?}", _0)]
    SendCoordEvent(#[source] mpsc::SendError),
    #[error(display = "Unexpected end of Uni stream")]
    EndOfUniStream,
    #[error(display = "Accepting Uni stream: {:?}", _0)]
    AcceptUniStream(#[source] ConnectionError),
}

pub(super) struct CoordChanInner {
    conn: ConecConn,
    ctrl: CtrlStream,
    iuni: IncomingUniStreams,
    peer: String,
    coord: mpsc::UnboundedSender<CoordEvent>,
    sender: mpsc::UnboundedSender<CoordChanEvent>,
    events: mpsc::UnboundedReceiver<CoordChanEvent>,
    ref_count: usize,
    driver: Option<Waker>,
    driver_lost: bool,
    to_send: VecDeque<ControlMsg>,
    flushing: bool,
    new_streams: HashMap<u32, SendStream>,
}

pub(super) enum CoordChanEvent {
    NSErr(u32),
    NSReq(String, u32),
    NSRes(u32, Result<SendStream, ConnectionError>),
    IUni(u32, FramedRecvStream),
}

impl CoordChanInner {
    // read the next message from the recv channel
    fn drive_ctrl_recv(&mut self, cx: &mut Context) -> Result<bool, CoordChanError> {
        let mut recvd = 0;
        loop {
            let msg = match self.ctrl.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => Err(CoordChanError::PeerClosed),
                Poll::Ready(Some(Err(e))) => Err(CoordChanError::StreamPoll(e)),
                Poll::Ready(Some(Ok(msg))) => Ok(msg),
            }?;
            match msg {
                ControlMsg::NewStreamReq(to, sid) => self
                    .coord
                    .unbounded_send(CoordEvent::NewStreamReq(self.peer.clone(), to, sid))
                    .map_err(|e| CoordChanError::SendCoordEvent(e.into_send_error())),
                _ => Err(CoordChanError::WrongMessage(msg)),
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

    fn handle_events(&mut self, cx: &mut Context) -> bool {
        use CoordChanEvent::*;
        let mut accepted = 0;
        loop {
            match self.events.poll_next_unpin(cx) {
                Poll::Ready(Some(event)) => match event {
                    NSErr(sid) => self.to_send.push_back(ControlMsg::NewStreamErr(sid)),
                    NSRes(sid, res) => match res {
                        Err(_) => {
                            self.to_send.push_back(ControlMsg::NewStreamErr(sid));
                        }
                        Ok(strm) => {
                            self.to_send.push_back(ControlMsg::NewStreamOk(sid));
                            self.new_streams.insert(sid, strm);
                        }
                    },
                    NSReq(to, sid) => {
                        let coord = self.coord.clone();
                        let uni = self.conn.open_uni();
                        tokio::spawn(async move {
                            coord
                                .unbounded_send(CoordEvent::NewStreamRes(to, sid, uni.await))
                                .ok();
                        });
                    }
                    IUni(sid, recv) => {
                        if let Some(send) = self.new_streams.remove(&sid) {
                            let from = self.peer.clone();
                            tokio::spawn(async move {
                                let mut write_stream = SymmetricallyFramed::new(
                                    FramedWrite::new(send, LengthDelimitedCodec::new()),
                                    SymmetricalBincode::<(String, u32)>::default(),
                                );

                                // send (from, sid) and flush
                                if write_stream.send((from, sid)).await.is_err() {
                                    return;
                                }
                                if write_stream.flush().await.is_err() {
                                    return;
                                }
                                let send = write_stream.into_inner();

                                // forward all messages from sender to receiver
                                recv.map(|b| b.map(|bb| bb.freeze()))
                                    .forward(send)
                                    .await
                                    .ok();
                            });
                        } else {
                            self.to_send.push_back(ControlMsg::NewStreamErr(sid));
                        }
                    }
                },
                _ => break,
            }
            accepted += 1;
            if accepted >= MAX_LOOPS {
                return true;
            }
        }
        false
    }

    fn drive_iuni_recv(&mut self, cx: &mut Context) -> Result<bool, CoordChanError> {
        let mut recvd = 0;
        loop {
            let recv = match self.iuni.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => Err(CoordChanError::EndOfUniStream),
                Poll::Ready(Some(r)) => r.map_err(CoordChanError::AcceptUniStream),
            }?;
            let sender = self.sender.clone();
            tokio::spawn(async move {
                let mut read_stream = SymmetricallyFramed::new(
                    FramedRead::new(recv, LengthDelimitedCodec::new()),
                    SymmetricalBincode::<u32>::default(),
                );
                let sid = match read_stream.try_next().await {
                    Err(e) => {
                        tracing::warn!("drive_iuni_recv: {:?}", e);
                        return;
                    }
                    Ok(msg) => match msg {
                        Some(sid) => sid,
                        None => {
                            tracing::warn!("drive_iuni_recv: unexpected end of stream");
                            return;
                        }
                    },
                };
                let recv = read_stream.into_inner();
                sender.unbounded_send(CoordChanEvent::IUni(sid, recv)).ok();
            });
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
        conn: ConecConn,
        ctrl: CtrlStream,
        iuni: IncomingUniStreams,
        peer: String,
        coord: mpsc::UnboundedSender<CoordEvent>,
    ) -> (Self, mpsc::UnboundedSender<CoordChanEvent>) {
        let mut to_send = VecDeque::new();
        // send hello at startup
        to_send.push_back(ControlMsg::CoHello);
        let (sender, events) = mpsc::unbounded();
        (
            Self(Arc::new(Mutex::new(CoordChanInner {
                conn,
                ctrl,
                iuni,
                peer,
                coord,
                sender: sender.clone(),
                events,
                ref_count: 0,
                driver: None,
                driver_lost: false,
                to_send,
                flushing: false,
                new_streams: HashMap::new(),
            }))),
            sender,
        )
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
            keep_going |= inner.handle_events(cx);
            if !inner.to_send.is_empty() || inner.flushing {
                keep_going |= inner.drive_ctrl_send(cx)?;
            }
            keep_going |= inner.drive_iuni_recv(cx)?;
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

pub(super) struct CoordChan {
    pub(super) inner: CoordChanRef,
    pub(super) sender: mpsc::UnboundedSender<CoordChanEvent>,
}

impl CoordChan {
    pub(super) fn send(&self, event: CoordChanEvent) {
        // ignore errors: can't be full because it's unbounded,
        // and if the receiver disappeared then the driver must
        // have sent a ChanClose event to the Coord
        self.sender.unbounded_send(event).ok();
    }
}