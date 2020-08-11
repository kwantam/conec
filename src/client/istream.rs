use crate::consts::MAX_LOOPS;
use crate::types::InStream;

use err_derive::Error;
use futures::channel::oneshot;
use futures::prelude::*;
use quinn::{ConnectionError, IncomingUniStreams};
use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use tokio_serde::{formats::SymmetricalBincode, SymmetricallyFramed};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

#[derive(Debug, Error)]
pub enum InStreamError {
    #[error(display = "Reading InitMsg from stream: {:?}", _0)]
    StreamRead(#[source] io::Error),
    #[error(display = "Deserializing InitMsg")]
    InitMsg,
}

pub type NewInStream = (String, u32, InStream);

pub type ConnectingInStream = oneshot::Receiver<Result<NewInStream, InStreamError>>;

#[derive(Debug, Error)]
pub enum IncomingStreamsError {
    #[error(display = "Unexpected end of Uni stream")]
    EndOfUniStream,
    #[error(display = "Client is gone")]
    ClientClosed,
    #[error(display = "Accepting Uni stream: {:?}", _0)]
    AcceptUniStream(#[source] ConnectionError),
}

pub(super) struct IncomingStreamsInner {
    client: Option<oneshot::Sender<()>>,
    bye: oneshot::Receiver<()>,
    streams: IncomingUniStreams,
    incoming: VecDeque<ConnectingInStream>,
    ref_count: usize,
    driver: Option<Waker>,
    driver_lost: bool,
    incoming_reader: Option<Waker>,
}

impl IncomingStreamsInner {
    fn drive_streams_recv(&mut self, cx: &mut Context) -> Result<bool, IncomingStreamsError> {
        match self.bye.poll_unpin(cx) {
            Poll::Pending => Ok(()),
            _ => Err(IncomingStreamsError::ClientClosed),
        }?;

        let mut recvd = 0;
        loop {
            let recv = match self.streams.poll_next_unpin(cx) {
                Poll::Pending => break,
                Poll::Ready(None) => Err(IncomingStreamsError::EndOfUniStream),
                Poll::Ready(Some(r)) => r.map_err(IncomingStreamsError::AcceptUniStream),
            }?;
            let (sender, receiver) = oneshot::channel();
            tokio::spawn(async move {
                let mut read_stream = SymmetricallyFramed::new(
                    FramedRead::new(recv, LengthDelimitedCodec::new()),
                    SymmetricalBincode::<(String, u32)>::default(),
                );
                let (peer, chanid) = match read_stream.try_next().await {
                    Err(e) => {
                        sender.send(Err(InStreamError::StreamRead(e))).ok();
                        return;
                    }
                    Ok(msg) => match msg {
                        Some(peer_chanid) => peer_chanid,
                        None => {
                            sender.send(Err(InStreamError::InitMsg)).ok();
                            return;
                        }
                    },
                };
                let instream = InStream::from_framed(read_stream.into_inner());
                sender.send(Ok((peer, chanid, instream))).ok();
            });
            self.incoming.push_back(receiver);
            if let Some(task) = self.incoming_reader.take() {
                task.wake();
            }
            recvd += 1;
            if recvd >= MAX_LOOPS {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

pub(super) struct IncomingStreamsRef(Arc<Mutex<IncomingStreamsInner>>);

impl std::ops::Deref for IncomingStreamsRef {
    type Target = Mutex<IncomingStreamsInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Clone for IncomingStreamsRef {
    fn clone(&self) -> Self {
        self.lock().unwrap().ref_count += 1;
        Self(self.0.clone())
    }
}

impl Drop for IncomingStreamsRef {
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

impl IncomingStreamsRef {
    pub(super) fn new(
        client: oneshot::Sender<()>,
        bye: oneshot::Receiver<()>,
        streams: IncomingUniStreams,
    ) -> Self {
        Self(Arc::new(Mutex::new(IncomingStreamsInner {
            client: Some(client),
            bye,
            streams,
            incoming: VecDeque::new(),
            ref_count: 0,
            driver: None,
            driver_lost: false,
            incoming_reader: None,
        })))
    }
}

#[must_use = "IncomingStreamsDriver must be spawned!"]
pub(super) struct IncomingStreamsDriver(pub(super) IncomingStreamsRef);

impl Future for IncomingStreamsDriver {
    type Output = Result<(), IncomingStreamsError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let inner = &mut *self.0.lock().unwrap();
        match &inner.driver {
            Some(w) if w.will_wake(cx.waker()) => (),
            _ => inner.driver = Some(cx.waker().clone()),
        };
        loop {
            if !inner.drive_streams_recv(cx)? {
                break;
            }
        }
        if inner.ref_count == 0 {
            // driver is alone now; die
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

impl Drop for IncomingStreamsDriver {
    fn drop(&mut self) {
        let mut inner = self.0.lock().unwrap();
        inner.driver_lost = true;
        inner.client.take().unwrap().send(()).ok();
        if let Some(task) = inner.incoming_reader.take() {
            task.wake();
        }
    }
}

pub struct IncomingStreams(pub(super) IncomingStreamsRef);

impl Stream for IncomingStreams {
    type Item = ConnectingInStream;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let inner = &mut *self.0.lock().unwrap();
        if let Some(inc) = inner.incoming.pop_front() {
            Poll::Ready(Some(inc))
        } else if inner.driver_lost {
            Poll::Ready(None)
        } else {
            inner.incoming_reader = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
