use futures::prelude::*;
use futures::sink::Send as SinkSend;
use futures::stream::TryNext;
use quinn::{
    Connection, Datagrams, Endpoint, IncomingBiStreams, IncomingUniStreams, NewConnection,
    RecvStream, SendStream,
};
use serde::{Deserialize, Serialize};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use tokio_serde::formats::SymmetricalBincode;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

pub struct ConecConnection {
    connection: Connection,
    iu_streams: IncomingUniStreams,
    ib_streams: IncomingBiStreams,
    datagrams: Datagrams,
}

impl ConecConnection {
    pub async fn connect(endpoint: &mut Endpoint, caddr: &str, cport: u16) -> io::Result<Self> {
        // only attempt to connect to an address of the same type as the endpoint's local socket
        let use_ipv4 = endpoint.local_addr()?.is_ipv4();
        for coord_addr in (caddr, cport)
            .to_socket_addrs()?
            .filter(|x| use_ipv4 == x.is_ipv4())
        {
            match endpoint
                .connect(&coord_addr, caddr)
                .map_err(|e| io::Error::new(io::ErrorKind::ConnectionAborted, e))?
                .await
            {
                Err(_) => continue,
                Ok(c) => return Ok(Self::new(c)),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "could not connect to coordinator",
        ))
    }

    pub fn new(nc: NewConnection) -> Self {
        let NewConnection {
            connection: conn,
            uni_streams: u_str,
            bi_streams: b_str,
            datagrams: dgrams,
            ..
        } = nc;
        Self {
            connection: conn,
            iu_streams: u_str,
            ib_streams: b_str,
            datagrams: dgrams,
        }
    }

    pub async fn connect_ctrl(&mut self, id: String) -> io::Result<(CtrlStream, Option<String>)> {
        let (cc_send, cc_recv) = self.connection.open_bi().await.map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("connect_ctrl failed: {}", e))
        })?;
        let mut ctrl_stream = CtrlStream::new(cc_send, cc_recv);
        let peer = ctrl_stream.send_hello(id).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("connect_ctrl error sending hello: {}", e),
            )
        })?;
        Ok((ctrl_stream, peer))
    }

    pub async fn accept_ctrl(&mut self, id: Option<String>) -> io::Result<(CtrlStream, String)> {
        let (cc_send, cc_recv) = self
            .ib_streams
            .next()
            .await
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "accept_ctrl failed: unexpected end of stream",
                )
            })?
            .map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("accept_ctrl failed: {}", e))
            })?;
        let mut ctrl_stream = CtrlStream::new(cc_send, cc_recv);
        let peer = ctrl_stream.recv_hello(id).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("accept_ctrl error getting hello: {}", e),
            )
        })?;
        Ok((ctrl_stream, peer))
    }
}

pub type FramedRecvStream = FramedRead<RecvStream, LengthDelimitedCodec>;
pub type FramedSendStream = FramedWrite<SendStream, LengthDelimitedCodec>;

fn to_framed_recv(r: RecvStream) -> FramedRecvStream {
    FramedRead::new(r, LengthDelimitedCodec::new())
}

fn to_framed_send(s: SendStream) -> FramedSendStream {
    FramedWrite::new(s, LengthDelimitedCodec::new())
}

/*
pub struct InStream {
    s_recv: FramedRecvStream,
}

impl InStream {
    pub fn new(r: RecvStream) -> Self {
        InStream {
            s_recv: to_framed_recv(r),
        }
    }
}

pub struct OutStream {
    s_send: FramedSendStream,
}

impl OutStream {
    pub fn new(s: SendStream) -> Self {
        OutStream {
            s_send: to_framed_send(s),
        }
    }
}

pub struct InOutStream {
    s_send: OutStream,
    s_recv: InStream,
}

impl InOutStream {
    pub fn new(s: SendStream, r: RecvStream) -> Self {
        InOutStream {
            s_send: OutStream::new(s),
            s_recv: InStream::new(r),
        }
    }
}
*/

// messages starting with "Co" are sent by Coordinator
// messages starting with "Cl" are sent by Client
// others can be sent by either
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub enum ControlMsg {
    CoHello,
    ClHello(String),
    NewChanReq(String),
    NewChan(SocketAddr),
    NewInStream(Option<String>),
    NewOutStream(Option<String>),
    Error(String),
}
/*
   Concept: client can ask for a proxied channel via coord or a direct connection

   - StreamSend(None)           Coord  output stream
   - StreamRecv(None)           Coord  input  stream
   - StreamSend(Some("asdf"))   "asdf" output stream
   - StreamRecv(Some("asdf"))   "asdf" input  stream

   Note: coord can send these, too!

*/

type CtrlRecvStream =
    SymmetricallyFramed<FramedRecvStream, ControlMsg, SymmetricalBincode<ControlMsg>>;
type CtrlSendStream =
    SymmetricallyFramed<FramedSendStream, ControlMsg, SymmetricalBincode<ControlMsg>>;

pub struct CtrlStream {
    s_send: CtrlSendStream,
    s_recv: CtrlRecvStream,
}

impl CtrlStream {
    pub fn new(s: SendStream, r: RecvStream) -> Self {
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

    pub async fn send_hello(&mut self, id: String) -> io::Result<Option<String>> {
        use ControlMsg::*;
        self.send(ClHello(id)).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("send_hello: could not send: {}", e),
            )
        })?;
        match self.recv().await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("send_hello: could not recv: {}", e),
            )
        })? {
            Some(CoHello) => Ok(None),
            Some(ClHello(pid)) => Ok(Some(pid)),
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                "send_hello: expected hello, got something else",
            )),
        }
    }

    pub async fn recv_hello(&mut self, id: Option<String>) -> io::Result<String> {
        use ControlMsg::*;
        let peer = match self.recv().await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("recv_hello: could not recv: {}", e),
            )
        })? {
            Some(ClHello(pid)) => pid,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "recv_hello: expected hello, got something else",
                ));
            }
        };

        let hello_msg = if let Some(mid) = id {
            ClHello(mid)
        } else {
            CoHello
        };

        self.send(hello_msg).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("recv_hello: could not send: {}", e),
            )
        })?;
        Ok(peer)
    }

    pub fn send(&mut self, msg: ControlMsg) -> SinkSend<CtrlSendStream, ControlMsg> {
        self.s_send.send(msg)
    }

    pub fn recv(&mut self) -> TryNext<CtrlRecvStream> {
        self.s_recv.try_next()
    }
}
