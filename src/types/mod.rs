mod conn;
mod ctrlstream;
mod iostream;

pub(crate) use conn::ConecConn;
pub use conn::ConecConnError;
pub(crate) use ctrlstream::CtrlStream;
pub use ctrlstream::CtrlStreamError;
pub use iostream::{InOutStream, InStream, OutStream};

use serde::{Deserialize, Serialize};

// messages starting with "Co" are sent by Coordinator
// messages starting with "Cl" are sent by Client
// others can be sent by either
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum ControlMsg {
    CoNonce(String),
    CoHello,
    ClHello(String, String),
    HelloError(String),
    NewStreamReq(String, u32),
    NewStreamOk(String, u32),
    NewStreamErr(String, u32),
}
