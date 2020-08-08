use super::consts::VERSION;

use futures::prelude::*;
use quinn::{
    CertificateChain, Connection, Endpoint, IncomingBiStreams, IncomingUniStreams, NewConnection,
    RecvStream, SendStream,
};
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::SystemTime;
use tokio_serde::formats::SymmetricalBincode;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

pub struct ConecConnection {
    connection: Connection,
    iu_streams: IncomingUniStreams,
    ib_streams: IncomingBiStreams,
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
            ..
        } = nc;
        Self {
            connection: conn,
            iu_streams: u_str,
            ib_streams: b_str,
        }
    }

    pub async fn accept_ctrl(&mut self, id: String) -> io::Result<CtrlStream> {
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

        let certs = self
            .connection
            .authentication_data()
            .peer_certificates
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Other,
                    "accept_ctrl failed: no certificate chain available",
                )
            })?;
        ctrl_stream.send_hello(id, certs).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("connect_ctrl error sending hello: {}", e),
            )
        })?;
        Ok(ctrl_stream)
    }

    pub async fn connect_ctrl(
        &mut self,
        certs: CertificateChain,
    ) -> io::Result<(CtrlStream, String)> {
        // open a new control stream to newly connected client
        let (cc_send, cc_recv) = self.connection.open_bi().await.map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("connect_ctrl failed: {}", e))
        })?;
        let mut ctrl_stream = CtrlStream::new(cc_send, cc_recv);

        // compute a nonce and send it to the client
        let nonce = {
            // version string
            let mut tmp = VERSION.to_string();
            // remote address
            tmp += &self.connection.remote_address().to_string();
            tmp += "::";
            // time
            tmp += &SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("clock error")
                .as_nanos()
                .to_string();
            tmp += "::";
            // randomness
            tmp += &thread_rng().gen::<u128>().to_string();
            tmp
        };
        ctrl_stream.send_nonce(nonce.clone()).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("accept_ctrl error sending nonce: {}", e),
            )
        })?;

        // expect the client's hello back
        let peer = ctrl_stream.recv_hello(&nonce, certs).await.map_err(|e| {
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
    CoNonce(String),
    CoHello,
    ClHello(String, String),
    HelloError(String),
    NewChanReq(String),
    NewChan(SocketAddr),
    NewInStream(Option<String>),
    NewOutStream(Option<String>),
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

    pub async fn send_hello(&mut self, id: String, certs: CertificateChain) -> io::Result<()> {
        use ControlMsg::*;

        // first, get the nonce from the server
        let nonce = match self.try_next().await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("send_hello: could not recv CoNonce: {}", e),
            )
        })? {
            Some(CoNonce(n)) => Ok(n),
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                "send_hello: expected nonce, got something else",
            )),
        }?;
        // check that version info in nonce matches our version
        if &nonce[..VERSION.len()] != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "send_hello: version mismatch",
            ));
        }
        // append certificate to nonce
        // XXX here we should be signing under our client key and sending back
        let new_nonce = format!("{}::{:?}", nonce, certs);

        // next, send back the hello
        self.send(ClHello(id, new_nonce)).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("send_hello: could not send ClHello: {}", e),
            )
        })?;

        // finally, get CoHello (or maybe an Error)
        match self.try_next().await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("send_hello: could not recv CoHello: {}", e),
            )
        })? {
            Some(CoHello) => Ok(()),
            Some(HelloError(e)) => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("send_hello: HelloError from peer: {}", e),
            )),
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                "send_hello: got unexpected message from peer",
            )),
        }
    }

    pub async fn send_nonce(&mut self, nonce: String) -> io::Result<()> {
        self.send(ControlMsg::CoNonce(nonce)).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("send_nonce: could not send: {}", e),
            )
        })
    }

    pub async fn recv_hello(&mut self, nonce: &str, certs: CertificateChain) -> io::Result<String> {
        let (pid, sig) = match self.try_next().await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("recv_hello: could not recv: {}", e),
            )
        })? {
            Some(ControlMsg::ClHello(pid, sig)) => (pid, sig),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "recv_hello: expected hello, got something else",
                ));
            }
        };

        // for channel binding, append expected server cert chain to nonce
        // XXX here we should be checking a signature and maybe a certificate
        let nonce_expect = format!("{}::{:?}", nonce, certs);
        if sig != nonce_expect {
            self.send(ControlMsg::HelloError("nonce mismatch".to_string()))
                .await
                .ok();
            self.finish().await.ok();
            Err(io::Error::new(
                io::ErrorKind::Other,
                "recv_hello: nonce mismatch",
            ))
        } else {
            Ok(pid)
        }
    }

    pub async fn finish(&mut self) -> io::Result<()> {
        self.s_send
            .flush()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.s_send
            .get_mut()
            .get_mut()
            .finish()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
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
