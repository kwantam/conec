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

mod cchan;
pub(crate) mod chan;
pub(crate) mod config;
mod ichan;
mod istream;
mod tls;

use crate::consts::ALPN_CONEC;
use crate::types::{ConecConn, ConecConnError, ConnectingOutStream};
use crate::Coord;
use chan::{ClientChan, ClientChanDriver, ClientChanRef};
pub use chan::{ClientChanError, ConnectingChannel};
use config::{CertGenError, ClientConfig};
pub use ichan::IncomingChannelsError;
use ichan::{IncomingChannelsDriver, IncomingChannelsRef};
pub use istream::{IncomingStreams, NewInStream};
use istream::{IncomingStreamsDriver, IncomingStreamsRef};

use err_derive::Error;
use futures::channel::mpsc;
use quinn::{crypto::rustls::TLSError, ClientConfigBuilder, Endpoint, EndpointError, ParseError};

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
    #[error(display = "Connecting control stream to coordinator: {:?}", _0)]
    Control(#[error(source, no_from)] ConecConnError),
    ///! Generating certificate for client failed
    #[error(display = "Generating certificate for client: {:?}", _0)]
    CertificateGen(#[source] CertGenError),
    ///! Error setting up certificate chain
    #[error(display = "Certificate chain: {:?}", _0)]
    CertificateChain(#[source] TLSError),
    ///! Error parsing client ephemeral cert
    #[error(display = "Ephemeral cert: {:?}", _0)]
    CertificateParse(#[source] ParseError),
    ///! Error starting new stream
    #[error(display = "Starting new stream: {:?}", _0)]
    NewStream(#[source] ClientChanError),
}

///! The target of a call to [Client::new_stream]: either the coordinator or another client.
pub enum StreamPeer {
    /// The other endpoint is the coordinator
    Coord,
    /// The other endpoint is a client
    Client(String),
}

impl StreamPeer {
    fn is_coord(&self) -> bool {
        matches!(self, Self::Coord)
    }

    fn into_id(self) -> Option<String> {
        match self {
            Self::Coord => None,
            Self::Client(id) => Some(id),
        }
    }
}

impl From<String> for StreamPeer {
    fn from(s: String) -> Self {
        Self::Client(s)
    }
}

impl From<Option<String>> for StreamPeer {
    fn from(s: Option<String>) -> Self {
        if let Some(s) = s {
            Self::Client(s)
        } else {
            Self::Coord
        }
    }
}

///! The Client end of a connection to the Coordinator
///
/// See [library documentation](../index.html) for an example of constructing a Client.
pub struct Client {
    #[allow(dead_code)]
    endpoint: Option<Endpoint>,
    #[allow(dead_code)]
    in_streams: IncomingStreamsRef,
    #[allow(dead_code)]
    in_channels: Option<IncomingChannelsRef>,
    #[allow(dead_code)]
    coord: ClientChan,
    ctr: u32,
}

impl Client {
    ///! Construct a Client and connect to the Coordinator
    pub async fn new(config: ClientConfig) -> Result<(Self, IncomingStreams), ClientError> {
        // generate client certificates
        let config = {
            let mut config = config;
            config.gen_certs()?;
            config
        };
        // unwrap is safe because gen_certs suceeded above
        let (cert, privkey, key) = config.cert_and_key.unwrap();
        // build the client configuration
        let mut qccb = ClientConfigBuilder::new({
            let mut qcc = quinn::ClientConfig::default();
            let clt_cert = cert.iter().next().unwrap().0.clone();
            qcc.crypto = tls::build_rustls_client_config(clt_cert, key)?;
            qcc
        });
        qccb.protocols(ALPN_CONEC);
        if config.keylog {
            qccb.enable_keylog();
        }
        if let Some(ca) = config.extra_ca {
            qccb.add_certificate_authority(ca)?;
        }

        // build the QUIC endpoint
        let mut endpoint = Endpoint::builder();
        endpoint.default_client_config(qccb.build());
        if config.listen {
            let qsc = Coord::build_config(
                config.stateless_retry,
                config.keylog,
                cert,
                privkey,
                config.client_ca,
            )?;
            endpoint.listen(qsc);
        }
        let (mut endpoint, incoming) = endpoint.bind(&config.srcaddr)?;

        // set up the network endpoint and connect to the coordinator
        let (mut conn, ibi) =
            ConecConn::connect(&mut endpoint, &config.coord, config.addr, None).await?;

        // set up the control stream with the coordinator
        let ctrl = conn
            .connect_ctrl(config.id.clone())
            .await
            .map_err(ClientError::Control)?;

        // IPC
        let (stream_sender, incoming_streams) = mpsc::unbounded();

        // incoming channels listener
        let (in_channels, cert_sender, endpoint) = if config.listen {
            let (in_channels, cert_sender) = IncomingChannelsRef::new(
                endpoint,
                config.id,
                config.keepalive,
                incoming,
                stream_sender.clone(),
            );
            let driver = IncomingChannelsDriver(in_channels.clone());
            tokio::spawn(async move { driver.await });
            (Some(in_channels), Some(cert_sender), None)
        } else {
            (None, None, Some(endpoint))
        };

        // client-coordinator channel
        let coord = ClientChanRef::new(conn, ctrl, cert_sender);
        let driver = ClientChanDriver::new(coord.clone(), config.keepalive);
        tokio::spawn(async move { driver.await });
        let coord = ClientChan(coord);

        // set up the incoming streams listener
        let in_streams = IncomingStreamsRef::new(ibi, stream_sender);
        let driver = IncomingStreamsDriver(in_streams.clone());
        tokio::spawn(async move { driver.await });

        // set up the incoming channels listener

        Ok((
            Self {
                endpoint,
                in_streams,
                in_channels,
                coord,
                ctr: 1u32 << 31,
            },
            incoming_streams,
        ))
    }

    ///! Open a new stream to another client, proxied through the Coordinator
    pub fn new_stream<T: Into<StreamPeer>>(&mut self, to: T) -> ConnectingOutStream {
        let ctr = self.ctr;
        self.ctr += 1;
        self.new_stream_with_id(to, ctr)
    }

    ///! Open a new proxied stream to another client with an explicit stream-id
    ///
    /// The `sid` argument must be different for every call to this function for a given Client object.
    /// If mixing calls to this function with calls to [new_stream](Client::new_stream), avoid using
    /// `sid >= 1<<31`: those values are used automatically by that function.
    pub fn new_stream_with_id<T: Into<StreamPeer>>(&self, to: T, sid: u32) -> ConnectingOutStream {
        self.coord.new_stream(to.into(), sid)
    }

    ///! Open a new channel directly to another client
    pub fn new_channel(&mut self, to: String) -> ConnectingChannel {
        let ctr = self.ctr;
        self.ctr += 1;
        self.new_channel_with_id(to, ctr)
    }

    ///! Open a new channel directly to another client with an explicit channel-id
    ///
    /// The `cid` argument follows the same rules as the `sid` argument to [Client::new_stream_with_id].
    pub fn new_channel_with_id(&self, to: String, cid: u32) -> ConnectingChannel {
        self.coord.new_channel(to, cid)
    }
}
