use anyhow::{anyhow, Result};
use futures::prelude::*;
use futures::sink::Send as SinkSend;
use futures::stream::TryNext;
use quinn::{
    ClientConfig, Connection, Datagrams, Endpoint, Incoming, IncomingBiStreams, IncomingUniStreams, NewConnection, RecvStream,
    SendStream,
};
use serde::{Deserialize, Serialize};
use std::net::ToSocketAddrs;
use tokio_serde::formats::SymmetricalBincode;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

pub struct ConecConnection {
    endpoint: Endpoint,
    connection: Connection,
    iu_streams: IncomingUniStreams,
    ib_streams: IncomingBiStreams,
    datagrams: Datagrams,
}

impl ConecConnection {
    pub async fn connect(ccfg: ClientConfig, caddr: &str, cport: u16) -> Result<(Self, Incoming)> {
        // build the QUIC endpoint
        let mut endpoint = Endpoint::builder();
        endpoint.default_client_config(ccfg);
        let (endpoint, incoming) = endpoint.bind(&"0.0.0.0:0".parse().unwrap())?;

        // connect to the coordinator
        let coord_addr = (caddr, cport)
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| anyhow!("connect: could not resolve coordinator address"))?;
        let nc =
            endpoint
                .connect(&coord_addr, caddr)?
                .await
                .map_err(|e| anyhow!("failed to connect: {}", e))?;
        let ccon = ConecConnection::new(nc, endpoint);
        Ok((ccon, incoming))
    }

    pub fn new(nc: NewConnection, ep: Endpoint) -> Self {
        let NewConnection {
            connection: conn,
            uni_streams: u_str,
            bi_streams: b_str,
            datagrams: dgrams,
            ..
        } = nc;
        ConecConnection {
            endpoint: ep,
            connection: conn,
            iu_streams: u_str,
            ib_streams: b_str,
            datagrams: dgrams,
        }
    }

    /*
    pub async fn accept(&mut self) -> Result<NewConnection> {
        if let Some(conn) = self.incoming.next().await {
            Ok(conn.await.map_err(|e| anyhow!("accept failed: {}", e))?)
        } else {
            Err(anyhow!("accept failed: unexpected end of stream"))
        }
    }
    */

    pub fn get_connection(&self) -> &Connection {
        &self.connection
    }

    pub fn clone_endpoint(&self) -> Endpoint {
        self.endpoint.clone()
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
    /*  can we just close control stream, i.e., does peer learn about closed streams?
    Close,
    CloseAck,
    */
    CoData(String),
    ClData(String),
    Error(String),
}

type CtrlRecvStream =
    SymmetricallyFramed<FramedRecvStream, ControlMsg, SymmetricalBincode<ControlMsg>>;
type CtrlSendStream =
    SymmetricallyFramed<FramedSendStream, ControlMsg, SymmetricalBincode<ControlMsg>>;

pub struct CtrlStream {
    s_send: CtrlSendStream,
    s_recv: CtrlRecvStream,
    peer: Option<String>,
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
            peer: None,
        }
    }

    pub async fn send_hello(&mut self, id: String) -> Result<()> {
        use ControlMsg::*;
        self.send(ClHello(id))
            .await
            .map_err(|e| anyhow!("send_hello: could not send: {}", e))?;
        match self.recv().await.map_err(|e| anyhow!("send_hello: could not recv: {}", e))? {
            Some(CoHello) => {
                self.peer = None;
                Ok(())
            },
            Some(ClHello(pid)) => {
                self.peer = Some(pid);
                Ok(())
            },
            _ => Err(anyhow!("send_hello: expected hello, got something else")),
        }
    }

    pub async fn recv_hello(&mut self, id: Option<String>) -> Result<()> {
        use ControlMsg::*;
        match self.recv().await.map_err(|e| anyhow!("recv_hello: could not recv: {}", e))? {
            Some(ClHello(pid)) => {
                self.peer = Some(pid);
                Ok(())
            },
            _ => Err(anyhow!("recv_hello: expected hello, got something else")),
        }?;

        let hello_msg = if let Some(mid) = id {
            ClHello(mid)
        } else {
            CoHello
        };

        self.send(hello_msg).await.map_err(|e| anyhow!("recv_hello: could not send: {}", e))
    }

    pub fn send(&mut self, msg: ControlMsg) -> SinkSend<CtrlSendStream, ControlMsg> {
        self.s_send.send(msg)
    }

    pub fn recv(&mut self) -> TryNext<CtrlRecvStream> {
        self.s_recv.try_next()
    }
}
