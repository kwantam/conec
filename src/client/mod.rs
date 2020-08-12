// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

/*!
This module defines the Client entity and associated functionality.

See [library documentation](../index.html) for more info on how to instantiate a Client.
*/

mod chan;
pub(crate) mod config;
mod istream;

use super::consts::ALPN_CONEC;
use super::types::{ConecConn, ConecConnError};
use chan::{ClientChan, ClientChanDriver, ClientChanRef};
pub use chan::{ClientChanError, ConnectingOutStream, OutStreamError};
use config::ClientConfig;
pub use istream::{
    ConnectingInStream, InStreamError, IncomingStreams, IncomingStreamsError, NewInStream,
};
use istream::{IncomingStreamsDriver, IncomingStreamsRef};

use err_derive::Error;
use quinn::{ClientConfigBuilder, Endpoint, EndpointError};

///! Client::new constructor errors
#[derive(Debug, Error)]
pub enum ClientError {
    ///! Adding certificate authority failed
    #[error(display = "Adding certificate authority: {:?}", _0)]
    CertificateAuthority(#[source] webpki::Error),
    ///! Binding port failed
    #[error(display = "Binding port: {:?}", _0)]
    Bind(#[source] EndpointError),
    ///! Connecting to Coordinator failed
    #[error(display = "Connecting to coordinator: {:?}", _0)]
    Connect(#[source] ConecConnError),
    ///! Accepting control stream from Coordinator failed
    #[error(display = "Accepting control stream from coordinator: {:?}", _0)]
    AcceptCtrl(#[error(source, no_from)] ConecConnError),
}

///! The Client end of a connection to the Coordinator
///
/// See [library documentation](../index.html) for an example of constructing a Client.
pub struct Client {
    _endpoint: Endpoint,
    coord: ClientChan,
}

impl Client {
    ///! Construct a Client and connect to the Coordinator
    pub async fn new(config: ClientConfig) -> Result<(Self, IncomingStreams), ClientError> {
        // build the client configuration
        let mut qcc = ClientConfigBuilder::default();
        qcc.protocols(ALPN_CONEC);
        if config.keylog {
            qcc.enable_keylog();
        }
        if let Some(ca) = config.extra_ca {
            qcc.add_certificate_authority(ca)?;
        }

        // build the QUIC endpoint
        let mut endpoint = Endpoint::builder();
        endpoint.default_client_config(qcc.build());
        let (mut endpoint, _incoming) = endpoint.bind(&config.srcaddr)?;

        // set up the network endpoint and connect to the coordinator
        let (mut conn, iuni) =
            ConecConn::connect(&mut endpoint, &config.coord[..], config.port).await?;

        // set up the control stream with the coordinator
        let ctrl = conn
            .accept_ctrl(config.id)
            .await
            .map_err(ClientError::AcceptCtrl)?;

        // set up the client-coordinator channel and spawn its driver
        let (inner, i_client, i_bye) = ClientChanRef::new(conn, ctrl);
        let driver = ClientChanDriver(inner.clone());
        tokio::spawn(async move { driver.await });
        let coord = ClientChan(inner);

        // set up the incoming streams listener
        let istrms = {
            let inner = IncomingStreamsRef::new(i_client, i_bye, iuni);
            let driver = IncomingStreamsDriver(inner.clone());
            tokio::spawn(async move { driver.await });
            IncomingStreams(inner)
        };

        Ok((Self { _endpoint: endpoint, coord }, istrms))
    }

    ///! Open a new stream to another client, proxied through the Coordinator
    pub fn new_stream(&self, to: String, cid: u32) -> Result<ConnectingOutStream, ClientChanError> {
        self.coord.new_stream(to, cid)
    }
}
