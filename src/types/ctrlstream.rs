// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use super::{InStream, OutStream};
use crate::consts::VERSION;

use err_derive::Error;
use futures::prelude::*;
use quinn::{RecvStream, SendStream, WriteError};
use serde::{Deserialize, Serialize};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_serde::{formats::SymmetricalBincode, SymmetricallyFramed};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use webpki::{DNSNameRef, EndEntityCert};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum ControlMsg {
    ClHello(String, String),
    CoHello,
    HelloError(String),
    NewStreamReq(String, u32),
    NewStreamOk(u32),
    NewStreamErr(u32),
    KeepAlive,
    NewChannelReq(String, u32),
    NewChannelErr(u32),
    NewChannelOk(u32, SocketAddr, Vec<u8>),
    CertReq(String, u32, Vec<u8>),
    CertNok(String, u32),
    CertOk(String, u32),
    NewBroadcastReq(String, u32),
    NewBroadcastErr(u32),
    NewBroadcastOk(u32),
}

#[derive(Debug, Error)]
pub enum CtrlStreamError {
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
    #[error(display = "HelloError from peer: {:?}", _0)]
    PeerHelloError(String),
    #[error(display = "Sink flush: {:?}", _0)]
    SinkFlush(#[error(source, no_from)] io::Error),
    #[error(display = "Sink finish: {:?}", _0)]
    SinkFinish(#[error(source, no_from)] WriteError),
    #[error(display = "No certificate chain available")]
    CertificateChain,
    #[error(display = "Client certificate does not match name")]
    ClientCertName(#[source] webpki::Error),
    #[error(display = "Bad client auth message")]
    ClientName(#[source] webpki::InvalidDNSNameError),
}

type CtrlRecvStream = SymmetricallyFramed<InStream, ControlMsg, SymmetricalBincode<ControlMsg>>;
type CtrlSendStream = SymmetricallyFramed<OutStream, ControlMsg, SymmetricalBincode<ControlMsg>>;

pub(crate) struct CtrlStream {
    s_send: CtrlSendStream,
    s_recv: CtrlRecvStream,
}

impl CtrlStream {
    pub(super) fn new(s: SendStream, r: RecvStream) -> Self {
        CtrlStream {
            s_send: SymmetricallyFramed::new(
                FramedWrite::new(s, LengthDelimitedCodec::new()),
                SymmetricalBincode::<ControlMsg>::default(),
            ),
            s_recv: SymmetricallyFramed::new(
                FramedRead::new(r, LengthDelimitedCodec::new()),
                SymmetricalBincode::<ControlMsg>::default(),
            ),
        }
    }

    pub(super) async fn send_clhello(&mut self, id: String) -> Result<(), CtrlStreamError> {
        use ControlMsg::*;
        use CtrlStreamError::*;

        // next, send the hello
        self.send(ClHello(id, VERSION.to_string())).await.map_err(SendClHello)?;

        // finally, get CoHello (or maybe an Error)
        match self.try_next().await.map_err(RecvCoHello)? {
            Some(CoHello) => Ok(()),
            Some(HelloError(e)) => Err(PeerHelloError(e)),
            Some(msg) => Err(WrongMessage(msg, CoHello)),
            None => Err(EndOfCtrlStream),
        }
    }

    pub(super) async fn recv_clhello(&mut self, cert_bytes: &[u8]) -> Result<String, CtrlStreamError> {
        use ControlMsg::*;
        use CtrlStreamError::*;

        match self.try_next().await.map_err(RecvClHello)? {
            Some(ClHello(_, version)) if &version[..] != VERSION => Err(VersionMismatch(version)),
            Some(ClHello(peer, _)) => {
                let clt_cert = EndEntityCert::from(cert_bytes)?;
                clt_cert.verify_is_valid_for_dns_name(DNSNameRef::try_from_ascii_str(&peer)?)?;
                Ok(peer)
            }
            Some(msg) => Err(WrongMessage(msg, ClHello("".to_string(), "".to_string()))),
            None => Err(EndOfCtrlStream),
        }
    }

    pub(crate) async fn finish(&mut self) -> Result<(), CtrlStreamError> {
        self.s_send.flush().await.map_err(CtrlStreamError::SinkFlush)?;
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
