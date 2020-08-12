// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

mod conn;
mod ctrlstream;
mod iostream;

pub(crate) use conn::ConecConn;
pub use conn::ConecConnError;
pub(crate) use ctrlstream::CtrlStream;
pub use ctrlstream::CtrlStreamError;
pub(crate) use iostream::FramedRecvStream;
pub use iostream::{InOutStream, InStream, OutStream};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum ControlMsg {
    CoNonce(String),
    CoHello,
    ClHello(String, String),
    HelloError(String),
    NewStreamReq(String, u32),
    NewStreamOk(u32),
    NewStreamErr(u32),
}
