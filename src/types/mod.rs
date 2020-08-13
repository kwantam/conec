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
pub use ctrlstream::{CtrlStreamError, ControlMsg};

use quinn::{RecvStream, SendStream};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

///! Receiving end of a data stream: a [Stream](futures::stream::Stream) of [BytesMut](bytes::BytesMut).
pub type InStream = FramedRead<RecvStream, LengthDelimitedCodec>;

///! Sending end of a data stream that accepts [Bytes](bytes::Bytes).
pub type OutStream = FramedWrite<SendStream, LengthDelimitedCodec>;
