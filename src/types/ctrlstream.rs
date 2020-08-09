use crate::consts::VERSION;
use super::ControlMsg;
use super::iostream::{FramedSendStream, FramedRecvStream, to_framed_send, to_framed_recv};

use err_derive::Error;
use futures::prelude::*;
use quinn::{CertificateChain, RecvStream, SendStream, WriteError};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_serde::formats::SymmetricalBincode;
use tokio_serde::SymmetricallyFramed;

#[derive(Debug, Error)]
pub enum CtrlStreamError {
    #[error(display = "Recv CoNonce: {:?}", _0)]
    NonceRecv(#[error(source, no_from)] io::Error),
    #[error(display = "Unexpected end of control stream")]
    EndOfCtrlStream,
    #[error(display = "Wrong message: got {:?}, expected {:?}", _0, _1)]
    WrongMessage(ControlMsg, ControlMsg),
    #[error(display = "Version mismatch: got {:?}, expected {:?}", _0, VERSION)]
    VersionMismatch(String),
    #[error(display = "Recv ClHello: {:?}", _0)]
    RecvClHello(#[error(source, no_from)] io::Error),
    #[error(display = "Send ClHello: {:?}", _0)]
    SendClHello(#[error(source, no_from)] io::Error),
    #[error(display = "Recv CoHello: {:?}", _0)]
    RecvCoHello(#[error(source, no_from)] io::Error),
    #[error(display = "Send CoHello: {:?}", _0)]
    SendCoHello(#[error(source, no_from)] io::Error),
    #[error(display = "HelloError from peer: {:?}", _0)]
    PeerHelloError(String),
    #[error(display = "Send CoNonce: {:?}", _0)]
    NonceSend(#[error(source, no_from)] io::Error),
    #[error(display = "Bad client auth message")]
    BadClientAuth,
    #[error(display = "Sink flush: {:?}", _0)]
    SinkFlush(#[error(source, no_from)] io::Error),
    #[error(display = "Sink finish: {:?}", _0)]
    SinkFinish(#[error(source, no_from)] WriteError),
}

type CtrlRecvStream =
    SymmetricallyFramed<FramedRecvStream, ControlMsg, SymmetricalBincode<ControlMsg>>;
type CtrlSendStream =
    SymmetricallyFramed<FramedSendStream, ControlMsg, SymmetricalBincode<ControlMsg>>;

pub(crate) struct CtrlStream {
    s_send: CtrlSendStream,
    s_recv: CtrlRecvStream,
}

impl CtrlStream {
    pub(super) fn new(s: SendStream, r: RecvStream) -> Self {
        CtrlStream {
            s_send: SymmetricallyFramed::new(
                to_framed_send(s),
                SymmetricalBincode::<ControlMsg>::default(),
            ),
            s_recv: SymmetricallyFramed::new(
                to_framed_recv(r),
                SymmetricalBincode::<ControlMsg>::default(),
            ),
        }
    }

    pub(super) async fn send_hello(
        &mut self,
        id: String,
        certs: CertificateChain,
    ) -> Result<(), CtrlStreamError> {
        use ControlMsg::*;
        use CtrlStreamError::*;

        // first, get the nonce from the server
        let nonce = match self.try_next().await.map_err(NonceRecv)? {
            Some(CoNonce(n)) => Ok(n),
            Some(msg) => Err(WrongMessage(msg, CoNonce("".to_string()))),
            None => Err(EndOfCtrlStream),
        }?;
        // check that version info in nonce matches our version
        if &nonce[..VERSION.len()] != VERSION {
            return Err(VersionMismatch((&nonce[..VERSION.len()]).to_string()));
        }
        // append certificate to nonce
        // XXX here we should be signing under our client key and sending back
        let new_nonce = format!("{}::{:?}", nonce, certs);

        // next, send back the hello
        self.send(ClHello(id, new_nonce))
            .await
            .map_err(SendClHello)?;

        // finally, get CoHello (or maybe an Error)
        match self.try_next().await.map_err(RecvCoHello)? {
            Some(CoHello) => Ok(()),
            Some(HelloError(e)) => Err(PeerHelloError(e)),
            Some(msg) => Err(WrongMessage(msg, CoHello)),
            None => Err(EndOfCtrlStream),
        }
    }

    pub(super) async fn send_nonce(&mut self, nonce: String) -> Result<(), CtrlStreamError> {
        self.send(ControlMsg::CoNonce(nonce))
            .await
            .map_err(CtrlStreamError::NonceSend)
    }

    pub(super) async fn recv_hello(
        &mut self,
        nonce: &str,
        certs: CertificateChain,
    ) -> Result<String, CtrlStreamError> {
        use ControlMsg::*;
        use CtrlStreamError::*;

        let (pid, sig) = match self.try_next().await.map_err(RecvClHello)? {
            Some(ClHello(pid, sig)) => Ok((pid, sig)),
            Some(msg) => Err(WrongMessage(msg, ClHello("".to_string(), "".to_string()))),
            None => Err(EndOfCtrlStream),
        }?;

        // for channel binding, append expected server cert chain to nonce
        // XXX here we should be checking a signature and maybe a certificate
        let nonce_expect = format!("{}::{:?}", nonce, certs);
        if sig != nonce_expect {
            self.send(HelloError("nonce mismatch".to_string()))
                .await
                .ok();
            self.finish().await.ok();
            Err(BadClientAuth)
        } else {
            Ok(pid)
        }
    }

    pub(crate) async fn finish(&mut self) -> Result<(), CtrlStreamError> {
        self.s_send
            .flush()
            .await
            .map_err(CtrlStreamError::SinkFlush)?;
        self.s_send
            .get_mut()
            .get_mut()
            .finish()
            .await
            .map_err(CtrlStreamError::SinkFinish)
    }
}

impl Stream for CtrlStream {
    type Item = io::Result<ControlMsg>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.s_recv.poll_next_unpin(cx)
    }
}

impl Sink<ControlMsg> for CtrlStream {
    type Error = io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.s_send.poll_ready_unpin(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: ControlMsg) -> Result<(), Self::Error> {
        self.s_send.start_send_unpin(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.s_send.poll_flush_unpin(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.s_send.poll_close_unpin(cx)
    }
}

