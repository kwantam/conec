mod conn;
mod ctrlstream;
mod iostream;

pub(crate) use conn::ConecConn;
pub use conn::ConecConnError;
pub(crate) use ctrlstream::CtrlStream;
pub use ctrlstream::CtrlStreamError;
pub use iostream::{InOutStream, InStream, OutStream};

use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

// messages starting with "Co" are sent by Coordinator
// messages starting with "Cl" are sent by Client
// others can be sent by either
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum ControlMsg {
    CoNonce(String),
    CoHello,
    ClHello(String, String),
    HelloError(String),
    NewChanReq(String),
    NewChan(SocketAddr),
    NewInStream(Option<String>),
    NewOutStream(Option<String>),
}
