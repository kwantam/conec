// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use super::{ControlMsg, CtrlStream, CtrlStreamError};
use crate::types::ConecConnAddr;

use err_derive::Error;
use futures::prelude::*;
use quinn::{
    ClientConfig, ConnectError, Connecting, Connection, ConnectionError, Endpoint,
    IncomingBiStreams, NewConnection, OpenBi,
};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};

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
    #[error(display = "send_hello error: {:?}", _0)]
    SendHello(#[error(source, no_from)] CtrlStreamError),
    #[error(display = "Opening BiDi stream: {:?}", _0)]
    OpenBidiStream(#[error(source, no_from)] ConnectionError),
    #[error(display = "Sending version: {:?}", _0)]
    VersionSend(#[error(source, no_from)] CtrlStreamError),
    #[error(display = "Receiving hello: {:?}", _0)]
    RecvHello(#[error(source, no_from)] CtrlStreamError),
}

pub(crate) struct ConecConn(Connection);

fn connect_with_option(
    endpoint: &Endpoint,
    addr: &SocketAddr,
    name: &str,
    config: Option<ClientConfig>,
) -> Result<Connecting, ConnectError> {
    if let Some(cfg) = config {
        endpoint.connect_with(cfg, addr, name)
    } else {
        endpoint.connect(addr, name)
    }
}

impl ConecConn {
    pub(crate) async fn connect(
        endpoint: &mut Endpoint,
        cname: &str,
        caddr: ConecConnAddr,
        config: Option<ClientConfig>,
    ) -> Result<(Self, IncomingBiStreams), ConecConnError> {
        // no name resolution: explicit SocketAddr given
        if caddr.is_sockaddr() {
            return match connect_with_option(endpoint, caddr.get_addr().unwrap(), cname, config)?
                .await
            {
                Err(_) => Err(ConecConnError::CouldNotConnect),
                Ok(c) => Ok(Self::new(c)),
            };
        }
        // name resolution
        // only attempt to connect to an address of the same type as the endpoint's local socket
        let mut resolved = false;
        let use_ipv4 = endpoint.local_addr()?.is_ipv4();
        for coord_addr in (cname, caddr.get_port().unwrap())
            .to_socket_addrs()?
            .filter(|x| use_ipv4 == x.is_ipv4())
        {
            resolved = true;
            match connect_with_option(endpoint, &coord_addr, cname, config.clone())?.await {
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

    pub(crate) fn new(nc: NewConnection) -> (Self, IncomingBiStreams) {
        let NewConnection {
            connection: conn,
            bi_streams: b_str,
            ..
        } = { nc };
        (Self(conn), b_str)
    }

    pub(crate) async fn connect_ctrl(&mut self, id: String) -> Result<CtrlStream, ConecConnError> {
        // open a new control stream to newly connected client
        let (cc_send, cc_recv) = self
            .0
            .open_bi()
            .await
            .map_err(ConecConnError::OpenBidiStream)?;
        let mut ctrl_stream = CtrlStream::new(cc_send, cc_recv);

        ctrl_stream
            .send_clhello(id)
            .await
            .map_err(ConecConnError::SendHello)?;
        Ok(ctrl_stream)
    }

    pub(crate) async fn accept_ctrl(
        &mut self,
        ibi: &mut IncomingBiStreams,
    ) -> Result<(CtrlStream, String), ConecConnError> {
        let (cc_send, cc_recv) = ibi
            .next()
            .await
            .ok_or(ConecConnError::EndOfBidiStream)?
            .map_err(ConecConnError::AcceptBidiStream)?;
        let mut ctrl_stream = CtrlStream::new(cc_send, cc_recv);

        // expect the client's hello back, check cert name, otherwise try to send client an error
        let peer_certs = self.0.authentication_data().peer_certificates;
        match ctrl_stream.recv_clhello(peer_certs).await {
            Ok(peer) => Ok((ctrl_stream, peer)),
            Err(e) => {
                ctrl_stream
                    .send(ControlMsg::HelloError(format!("{:?}", e)))
                    .await
                    .ok();
                ctrl_stream.finish().await.ok();
                Err(ConecConnError::RecvHello(e))
            }
        }
    }

    pub(crate) fn open_bi(&mut self) -> OpenBi {
        self.0.open_bi()
    }

    pub(crate) fn remote_addr(&self) -> SocketAddr {
        self.0.remote_address()
    }
}
