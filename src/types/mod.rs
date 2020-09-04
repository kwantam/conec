// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

//! Common types

mod conn;
mod ctrlstream;

pub(crate) use conn::ConecConn;
pub use conn::ConecConnError;
pub(crate) use ctrlstream::CtrlStream;
pub use ctrlstream::{ControlMsg, CtrlStreamError};

use err_derive::Error;
use futures::{channel::oneshot, prelude::*};
use quinn::{ConnectionError, RecvStream, SendStream};
use serde::{Deserialize, Serialize};
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_serde::{formats::SymmetricalBincode, SymmetricallyFramed};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

/// Receiving end of a data stream: a [Stream](futures::stream::Stream) of [BytesMut](bytes::BytesMut).
pub type InStream = FramedRead<RecvStream, LengthDelimitedCodec>;

/// Sending end of a data stream: a [Sink](futures::sink::Sink) that accepts [Bytes](bytes::Bytes).
pub type OutStream = FramedWrite<SendStream, LengthDelimitedCodec>;

pub(crate) async fn outstream_init(
    send: SendStream,
    peer: StreamPeer,
    sid: u32,
) -> Result<OutStream, OutStreamError> {
    let mut write_stream = SymmetricallyFramed::new(
        FramedWrite::new(send, LengthDelimitedCodec::new()),
        SymmetricalBincode::<(StreamPeer, u32)>::default(),
    );
    // send (from, sid) and flush
    write_stream.send((peer, sid)).await?;
    write_stream.flush().await?;
    Ok(write_stream.into_inner())
}

#[derive(Clone, Debug)]
pub(crate) enum ConecConnAddr {
    Portnum(u16),
    Sockaddr(SocketAddr),
}

impl From<SocketAddr> for ConecConnAddr {
    fn from(addr: SocketAddr) -> Self {
        ConecConnAddr::Sockaddr(addr)
    }
}

impl From<u16> for ConecConnAddr {
    fn from(port: u16) -> Self {
        ConecConnAddr::Portnum(port)
    }
}

impl ConecConnAddr {
    pub(crate) fn is_sockaddr(&self) -> bool {
        match self {
            Self::Portnum(_) => false,
            Self::Sockaddr(_) => true,
        }
    }

    pub(crate) fn get_port(&self) -> Option<u16> {
        match self {
            Self::Portnum(p) => Some(*p),
            Self::Sockaddr(_) => None,
        }
    }

    pub(crate) fn get_addr(&self) -> Option<&SocketAddr> {
        match self {
            Self::Portnum(_) => None,
            Self::Sockaddr(ref s) => Some(s),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) enum StreamTo {
    Broadcast(u32),
    Client(u32),
    Coord(u32),
}

/// Error variant output by [ConnectingOutStream] future
#[derive(Debug, Error)]
pub enum OutStreamError {
    /// Error injecting event
    #[error(display = "Could not send event")]
    Event,
    /// Coordinator sent us an error
    #[error(display = "Coordinator responded with error")]
    Coord,
    /// Reused stream id
    #[error(display = "Reused stream id")]
    StreamId,
    /// Failed to send initial message
    #[error(display = "Sending initial message: {:?}", _0)]
    InitMsg(#[error(source, no_from)] io::Error),
    /// Failed to flush initial message
    #[error(display = "Flushing init message: {:?}", _0)]
    Flush(#[error(source, no_from)] io::Error),
    /// Failed to open bidirectional channel
    #[error(display = "Opening bidirectional channel: {:?}", _0)]
    OpenBi(#[source] ConnectionError),
    /// Opening channel was canceled
    #[error(display = "Outgoing connection canceled: {:?}", _0)]
    Canceled(#[source] oneshot::Canceled),
    /// OutStream requested for invalid peer name
    #[error(display = "No such peer: {:?}", _0)]
    NoSuchPeer(String),
    /// Failed to initialize stream
    #[error(display = "Stream initialization: {:?}", _0)]
    InitStream(#[source] io::Error),
}

def_cs_future!(
    ConnectingOutStream,
    pub(crate),
    ConnectingOutStreamHandle,
    pub(crate),
    (OutStream, InStream),
    OutStreamError,
    doc = "An outgoing stream that is connecting"
);

/// The other end of this stream: Coordinator, another Client, or a Broadcast channel
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum StreamPeer {
    /// The other endpoint is the coordinator
    Coord,
    /// The other endpoint is a client
    Client(String),
    /// The other endpoint is a broadcast stream
    Broadcast(String),
}

impl From<String> for StreamPeer {
    fn from(s: String) -> Self {
        Self::Client(s)
    }
}

impl From<Option<String>> for StreamPeer {
    fn from(s: Option<String>) -> Self {
        s.map_or(Self::Coord, Self::Client)
    }
}

impl StreamPeer {
    /// Returns true just when this value represents the Coordinator
    pub fn is_coord(&self) -> bool {
        matches!(self, Self::Coord)
    }

    /// Returns true just when this value represents a Client
    pub fn is_client(&self) -> bool {
        matches!(self, Self::Client(_))
    }

    /// Returns true just when this value represents a Broadcast stream
    pub fn is_broadcast(&self) -> bool {
        matches!(self, Self::Broadcast(_))
    }

    /// Converts StreamPeer into Option<String>, where None is Coordinator
    pub fn into_id(self) -> Option<String> {
        match self {
            Self::Coord => None,
            Self::Client(id) => Some(id),
            Self::Broadcast(id) => Some(id),
        }
    }
}
