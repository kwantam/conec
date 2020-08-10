use crate::consts::MAX_LOOPS;
use crate::types::{ConecConn, ControlMsg, CtrlStream, OutStream};
use crate::util;

use err_derive::Error;
use futures::channel::oneshot;
use futures::prelude::*;
use quinn::ConnectionError;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use tokio_serde::{formats::SymmetricalBincode, SymmetricallyFramed};
use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};

#[derive(Debug, Error)]
pub enum OutStreamError {
    #[error(display = "Coordinator responded with error")]
    Coord,
    #[error(display = "Sending initial message: {:?}", _0)]
    InitMsg(#[error(source, no_from)] io::Error),
    #[error(display = "Flushing init message: {:?}", _0)]
    Flush(#[error(source, no_from)] io::Error),
    #[error(display = "Opening unidirectional channel: {:?}", _0)]
    OpenUni(#[source] ConnectionError),
}

pub type ConnectingOutStream = oneshot::Receiver<Result<OutStream, OutStreamError>>;
type ConnectingOutStreamHandle = oneshot::Sender<Result<OutStream, OutStreamError>>;

#[derive(Debug, Error)]
pub enum ClientChanError {
    #[error(display = "Peer closed connection")]
    PeerClosed,
    #[error(display = "Stream poll: {:?}", _0)]
    StreamPoll(#[error(source, no_from)] io::Error),
    #[error(display = "Control sink: {:?}", _0)]
    Sink(#[error(source, no_from)] util::SinkError),
    #[error(display = "New stream peer:sid must be unique")]
    StreamNameInUse,
    #[error(display = "Unexpected message from coordinator")]
    WrongMessage(ControlMsg),
    #[error(display = "Coord response about nonexistent channel {}:{}", _0, _1)]
    NonexistentStream(String, u32),
    #[error(display = "Coord response about stale channel {}:{}", _0, _1)]
    StaleStream(String, u32),
    #[error(display = "Incoming driver hung up")]
    IncomingDriverHup,
}

pub(super) struct ClientChanInner {
    conn: ConecConn,
    ctrl: CtrlStream,
    id: String,
    incs_bye_in: oneshot::Receiver<()>,
    incs_bye_out: Option<oneshot::Sender<()>>,
    ref_count: usize,
    driver: Option<Waker>,
    driver_lost: bool,
    to_send: VecDeque<ControlMsg>,
    new_streams: HashMap<u32, Option<ConnectingOutStreamHandle>>,
    flushing: bool,
}

impl ClientChanInner {
    fn wake(&mut self) {
        if let Some(task) = self.driver.take() {
            task.wake();
        }
    }

    fn handle_events(&mut self, cx: &mut Context) -> Result<bool, ClientChanError> {
        match self.incs_bye_in.poll_unpin(cx) {
            Poll::Pending => Ok(()),
            _ => Err(ClientChanError::IncomingDriverHup),
        }?;
        Ok(false)
    }

    fn get_new_stream(
        &mut self,
        sid: u32,
        peer: &String,
    ) -> Result<ConnectingOutStreamHandle, ClientChanError> {
        if let Some(chan) = self.new_streams.get_mut(&sid) {
            if let Some(chan) = chan.take() {
                Ok(chan)
            } else {
                Err(ClientChanError::StaleStream(peer.clone(), sid))
            }
        } else {
            Err(ClientChanError::NonexistentStream(peer.clone(), sid))
        }
    }

    fn drive_ctrl_recv(&mut self, cx: &mut Context) -> Result<bool, ClientChanError> {
        let mut recvd = 0;
        loop {
            use ClientChanError::*;
            use ControlMsg::*;
            match self.ctrl.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => Err(PeerClosed),
                Poll::Ready(Some(Err(e))) => Err(StreamPoll(e)),
                Poll::Ready(Some(Ok(msg))) => match msg {
                    NewStreamOk(peer, sid) => {
                        let chan = self.get_new_stream(sid, &peer)?;
                        let client_id = self.id.clone();
                        let uni = self.conn.open_uni();
                        tokio::spawn(async move {
                            // get the unidirectional channel
                            let send = match uni.await {
                                Ok(send) => send,
                                Err(e) => {
                                    chan.send(Err(OutStreamError::OpenUni(e))).ok();
                                    return;
                                }
                            };

                            // write our name and channel id to it
                            let mut write_stream = SymmetricallyFramed::new(
                                FramedWrite::new(send, LengthDelimitedCodec::new()),
                                SymmetricalBincode::<(String, u32)>::default(),
                            );
                            if let Err(e) = write_stream.send((client_id, sid)).await {
                                chan.send(Err(OutStreamError::InitMsg(e))).ok();
                                return;
                            }
                            if let Err(e) = write_stream.flush().await {
                                chan.send(Err(OutStreamError::Flush(e))).ok();
                                return;
                            };

                            // send resulting OutStream to the receiver
                            let outstream = OutStream::from_framed(write_stream.into_inner());
                            chan.send(Ok(outstream)).ok();
                        });
                        Ok(())
                    }
                    NewStreamErr(peer, sid) => {
                        let chan = self.get_new_stream(sid, &peer)?;
                        chan.send(Err(OutStreamError::Coord)).ok();
                        Ok(())
                    }
                    _ => Err(WrongMessage(msg)),
                },
            }?;
            recvd += 1;
            if recvd >= MAX_LOOPS {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn drive_ctrl_send(&mut self, cx: &mut Context) -> Result<bool, ClientChanError> {
        util::drive_ctrl_send(cx, &mut self.flushing, &mut self.ctrl, &mut self.to_send)
            .map_err(ClientChanError::Sink)
    }
}

pub(super) struct ClientChanRef(Arc<Mutex<ClientChanInner>>);

impl std::ops::Deref for ClientChanRef {
    type Target = Mutex<ClientChanInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Clone for ClientChanRef {
    fn clone(&self) -> Self {
        self.lock().unwrap().ref_count += 1;
        Self(self.0.clone())
    }
}

impl Drop for ClientChanRef {
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

impl ClientChanRef {
    pub(super) fn new(
        conn: ConecConn,
        ctrl: CtrlStream,
        id: String,
    ) -> (Self, oneshot::Sender<()>, oneshot::Receiver<()>) {
        let (i_client, incs_bye_in) = oneshot::channel();
        let (incs_bye_out, i_bye) = oneshot::channel();
        (
            Self(Arc::new(Mutex::new(ClientChanInner {
                conn,
                ctrl,
                id,
                incs_bye_in,
                incs_bye_out: Some(incs_bye_out),
                ref_count: 0,
                driver: None,
                driver_lost: false,
                to_send: VecDeque::new(),
                new_streams: HashMap::new(),
                flushing: false,
            }))),
            i_client,
            i_bye,
        )
    }
}

#[must_use = "ClientChanDriver must be spawned!"]
pub(super) struct ClientChanDriver(pub(super) ClientChanRef);

impl Future for ClientChanDriver {
    type Output = Result<(), ClientChanError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let inner = &mut *self.0.lock().unwrap();
        match &inner.driver {
            Some(w) if w.will_wake(cx.waker()) => (),
            _ => inner.driver = Some(cx.waker().clone()),
        };
        loop {
            let mut keep_going = false;
            keep_going |= inner.handle_events(cx)?;
            keep_going |= inner.drive_ctrl_recv(cx)?;
            if !inner.to_send.is_empty() || inner.flushing {
                keep_going |= inner.drive_ctrl_send(cx)?;
            }
            if !keep_going {
                break;
            }
        }
        if inner.ref_count == 0 {
            // driver is lonely
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

impl Drop for ClientChanDriver {
    fn drop(&mut self) {
        let mut inner = &mut *self.0.lock().unwrap();
        // anyone still holding a ref to this channel?
        inner.driver_lost = true;
        // tell the incoming stream driver that we died
        inner.incs_bye_out.take().unwrap().send(()).ok();
    }
}

pub(super) struct ClientChan(pub(super) ClientChanRef);

impl ClientChan {
    pub(super) fn new_stream(
        &self,
        to: String,
        sid: u32,
    ) -> Result<ConnectingOutStream, ClientChanError> {
        // the new stream future is a channel that will contain the resulting channel
        let (sender, receiver) = oneshot::channel();
        let inner = &mut *self.0.lock().unwrap();

        // make sure this stream hasn't already been used
        let new_stream_req = ControlMsg::NewStreamReq(to.clone(), sid);
        if inner.new_streams.get(&sid).is_some() {
            return Err(ClientChanError::StreamNameInUse);
        }

        // send the coordinator a request and record the send side of the channel
        inner.to_send.push_back(new_stream_req);
        inner.new_streams.insert(sid, Some(sender));

        // make sure the driver wakes up
        inner.wake();

        Ok(receiver)
    }
}
