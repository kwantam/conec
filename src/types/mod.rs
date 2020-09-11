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
pub(crate) mod nbistream;
pub(crate) mod tagstream;

use crate::client::ConnectingStreamError;
pub(crate) use conn::ConecConn;
pub use conn::ConecConnError;
pub(crate) use ctrlstream::CtrlStream;
pub use ctrlstream::{ControlMsg, CtrlStreamError};

use futures::prelude::*;
use quinn::{RecvStream, SendStream};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio_serde::{formats::SymmetricalBincode, SymmetricallyFramed};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

/// Receiving end of a data stream: a [Stream](futures::stream::Stream) of [BytesMut](bytes::BytesMut).
pub type InStream = FramedRead<RecvStream, LengthDelimitedCodec>;

/// Sending end of a data stream: a [Sink](futures::sink::Sink) that accepts [Bytes](bytes::Bytes).
pub type OutStream = FramedWrite<SendStream, LengthDelimitedCodec>;

pub(crate) async fn outstream_init(
    send: SendStream,
    from: String,
    sid: u64,
) -> Result<OutStream, ConnectingStreamError> {
    let mut write_stream = SymmetricallyFramed::new(
        FramedWrite::new(send, LengthDelimitedCodec::new()),
        SymmetricalBincode::<(String, u64)>::default(),
    );
    // send (from, sid) and flush
    write_stream.send((from, sid)).await?;
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
    Broadcast(u64),
    Client(u64),
}
