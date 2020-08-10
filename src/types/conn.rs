use super::{CtrlStream, CtrlStreamError};
use crate::consts::VERSION;

use err_derive::Error;
use futures::prelude::*;
use quinn::{
    CertificateChain, ConnectError, Connection, ConnectionError, Endpoint, IncomingBiStreams,
    IncomingUniStreams, NewConnection,
};
use rand::{thread_rng, Rng};
use std::io;
use std::net::ToSocketAddrs;
use std::time::SystemTime;

#[derive(Debug, Error)]
pub enum ConecConnError {
    #[error(display = "Local socket error: {:?}", _0)]
    SocketLocal(#[error(source)] io::Error),
    #[error(display = "Local connection error: {:?}", _0)]
    ConnectLocal(#[error(source)] ConnectError),
    #[error(display = "Could not connect to coordinator")]
    CouldNotConnect,
    #[error(display = "Could not resolve coordinator hostname")]
    NameResolution,
    #[error(display = "Unexpected end of BiDi stream")]
    EndOfBidiStream,
    #[error(display = "Accepting BiDi stream: {:?}", _0)]
    AcceptBidiStream(#[error(source, no_from)] ConnectionError),
    #[error(display = "No certificate chain available")]
    CertificateChain,
    #[error(display = "send_hello error: {:?}", _0)]
    SendHello(#[error(source, no_from)] CtrlStreamError),
    #[error(display = "Opening BiDi stream: {:?}", _0)]
    OpenBidiStream(#[error(source, no_from)] ConnectionError),
    #[error(display = "Sending nonce: {:?}", _0)]
    NonceSend(#[error(source, no_from)] CtrlStreamError),
    #[error(display = "Receiving hello: {:?}", _0)]
    RecvHello(#[error(source, no_from)] CtrlStreamError),
}

pub(crate) struct ConecConn {
    connection: Connection,
    ib_streams: IncomingBiStreams,
}

impl ConecConn {
    pub async fn connect(
        endpoint: &mut Endpoint,
        caddr: &str,
        cport: u16,
    ) -> Result<(Self, IncomingUniStreams), ConecConnError> {
        // only attempt to connect to an address of the same type as the endpoint's local socket
        let use_ipv4 = endpoint.local_addr()?.is_ipv4();
        let mut resolved = false;
        for coord_addr in (caddr, cport)
            .to_socket_addrs()?
            .filter(|x| use_ipv4 == x.is_ipv4())
        {
            resolved = true;
            match endpoint.connect(&coord_addr, caddr)?.await {
                Err(_) => continue,
                Ok(c) => return Ok(Self::new(c)),
            }
        }
        if resolved {
            Err(ConecConnError::CouldNotConnect)
        } else {
            Err(ConecConnError::NameResolution)
        }
    }

    pub fn new(nc: NewConnection) -> (Self, IncomingUniStreams) {
        let NewConnection {
            connection: conn,
            uni_streams: u_str,
            bi_streams: b_str,
            ..
        } = nc;
        (
            Self {
                connection: conn,
                ib_streams: b_str,
            },
            u_str,
        )
    }

    pub(crate) async fn accept_ctrl(&mut self, id: String) -> Result<CtrlStream, ConecConnError> {
        let (cc_send, cc_recv) = self
            .ib_streams
            .next()
            .await
            .ok_or(ConecConnError::EndOfBidiStream)?
            .map_err(ConecConnError::AcceptBidiStream)?;
        let mut ctrl_stream = CtrlStream::new(cc_send, cc_recv);

        let certs = self
            .connection
            .authentication_data()
            .peer_certificates
            .ok_or(ConecConnError::CertificateChain)?;
        ctrl_stream
            .send_hello(id, certs)
            .await
            .map_err(ConecConnError::SendHello)?;
        Ok(ctrl_stream)
    }

    pub(crate) async fn connect_ctrl(
        &mut self,
        certs: CertificateChain,
    ) -> Result<(CtrlStream, String), ConecConnError> {
        // open a new control stream to newly connected client
        let (cc_send, cc_recv) = self
            .connection
            .open_bi()
            .await
            .map_err(ConecConnError::OpenBidiStream)?;
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
                .expect("fatal clock error (should never happen)")
                .as_nanos()
                .to_string();
            tmp += "::";
            // randomness
            tmp += &thread_rng().gen::<u128>().to_string();
            tmp
        };
        ctrl_stream
            .send_nonce(nonce.clone())
            .await
            .map_err(ConecConnError::NonceSend)?;

        // expect the client's hello back
        let peer = ctrl_stream
            .recv_hello(&nonce, certs)
            .await
            .map_err(ConecConnError::RecvHello)?;
        Ok((ctrl_stream, peer))
    }
}
