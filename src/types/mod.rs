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

use quinn::{RecvStream, SendStream};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

///! Receiving end of a data stream: a [Stream](futures::stream::Stream) of [BytesMut](bytes::BytesMut).
pub type InStream = FramedRead<RecvStream, LengthDelimitedCodec>;

///! Sending end of a data stream that accepts [Bytes](bytes::Bytes).
pub type OutStream = FramedWrite<SendStream, LengthDelimitedCodec>;

#[derive(Clone, Debug)]
pub(crate) enum ConecConnAddr {
    Portnum(u16),
    Sockaddr(SocketAddr),
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
    Client(u32),
    Coord(u32),
}
