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
pub use crate::types::StreamPeer;
use crate::types::{ConecConn, ConecConnError, ConnectingOutStream};
use crate::Coord;
use chan::{ClientChan, ClientChanDriver, ClientChanRef};
pub use chan::{ClientChanError, ConnectingChannel};
use config::{CertGenError, ClientConfig};
pub use ichan::{ClosingChannel, IncomingChannelsError, NewChannelError};
use ichan::{IncomingChannels, IncomingChannelsDriver, IncomingChannelsRef};
pub use istream::{IncomingStreams, NewInStream, StreamId};
use istream::{IncomingStreamsDriver, IncomingStreamsRef};

use err_derive::Error;
use futures::channel::mpsc;
use quinn::{crypto::rustls::TLSError, ClientConfigBuilder, Endpoint, EndpointError, ParseError};

/// Client::new constructor errors
#[derive(Debug, Error)]
pub enum ClientError {
    /// Adding certificate authority failed
    #[error(display = "Adding certificate authority: {:?}", _0)]
    CertificateAuthority(#[source] webpki::Error),
    /// Binding port failed
    #[error(display = "Binding port: {:?}", _0)]
    Bind(#[source] EndpointError),
    /// Connecting to Coordinator failed
    #[error(display = "Connecting to coordinator: {:?}", _0)]
    Connect(#[source] ConecConnError),
    /// Accepting control stream from Coordinator failed
    #[error(display = "Connecting control stream to coordinator: {:?}", _0)]
    Control(#[error(source, no_from)] ConecConnError),
    /// Generating certificate for client failed
    #[error(display = "Generating certificate for client: {:?}", _0)]
    CertificateGen(#[source] CertGenError),
    /// Error setting up certificate chain
    #[error(display = "Certificate chain: {:?}", _0)]
    CertificateChain(#[source] TLSError),
    /// Error parsing client ephemeral cert
    #[error(display = "Ephemeral cert: {:?}", _0)]
    CertificateParse(#[source] ParseError),
    /// Error starting new stream
    #[error(display = "Starting new stream: {:?}", _0)]
    NewStream(#[source] ClientChanError),
}

/// The Client end of a connection to the Coordinator
///
/// See [library documentation](../index.html) for an example of constructing a Client.
pub struct Client {
    #[allow(dead_code)]
    in_streams: IncomingStreamsRef,
    in_channels: IncomingChannels,
    coord: ClientChan,
    ctr: u32,
}

impl Client {
    /// Construct a Client and connect to the Coordinator
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
        let qcc = qccb.build();

        // build the QUIC endpoint
        let mut endpoint = Endpoint::builder();
        endpoint.default_client_config(qcc.clone());
        if config.listen {
            let qsc = Coord::build_config(
                config.stateless_retry,
                config.keylog,
                cert,
                privkey,
                config.client_ca.clone(),
            )?;
            endpoint.listen(qsc);
        }
        let (mut endpoint, incoming) = endpoint.bind(&config.srcaddr)?;

        // set up the network endpoint and connect to the coordinator
        let (mut conn, ibi) = ConecConn::connect(&mut endpoint, &config.coord, config.addr, None).await?;

        // set up the control stream with the coordinator
        let ctrl = conn
            .connect_ctrl(config.id.clone())
            .await
            .map_err(ClientError::Control)?;

        // IPC
        let (stream_sender, incoming_streams) = mpsc::unbounded();

        // incoming channels listener
        let (in_channels, ichan_sender) = {
            let (inner, sender) = IncomingChannelsRef::new(
                endpoint,
                config.id,
                config.keepalive,
                incoming,
                stream_sender.clone(),
                qcc,
                config.client_ca,
            );
            let driver = IncomingChannelsDriver(inner.clone());
            tokio::spawn(async move { driver.await });
            (IncomingChannels::new(inner, sender.clone()), sender)
        };

        // client-coordinator channel
        let coord = {
            let coord = ClientChanRef::new(conn, ctrl, ichan_sender, config.listen);
            let driver = ClientChanDriver::new(coord.clone(), config.keepalive);
            tokio::spawn(async move { driver.await });
            ClientChan(coord)
        };

        // set up the incoming streams listener
        let in_streams = IncomingStreamsRef::new(ibi, stream_sender);
        let driver = IncomingStreamsDriver(in_streams.clone());
        tokio::spawn(async move { driver.await });

        // set up the incoming channels listener

        Ok((
            Self {
                in_streams,
                in_channels,
                coord,
                ctr: 1u32 << 31,
            },
            incoming_streams,
        ))
    }

    /// Open a new stream to another client, proxied through the Coordinator
    pub fn new_proxied_stream<T: Into<StreamPeer>>(&mut self, to: T) -> ConnectingOutStream {
        self.new_x_stream(to.into(), StreamId::Proxied)
    }

    /// Open a new stream to another client directly
    ///
    /// It is only possible to open another stream to a client for which there is
    /// an open channel, either because that client connected to this one or because
    /// this client called [Client::new_channel].
    pub fn new_direct_stream<T: Into<StreamPeer>>(&mut self, to: T) -> ConnectingOutStream {
        self.new_x_stream(to.into(), StreamId::Direct)
    }

    fn new_x_stream<F>(&mut self, to: StreamPeer, as_id: F) -> ConnectingOutStream
    where
        F: FnOnce(u32) -> StreamId,
    {
        let ctr = self.ctr;
        self.ctr += 1;
        self.new_stream_with_id(to, as_id(ctr))
    }

    /// Open a new proxied stream to another client with an explicit stream-id
    ///
    /// The `sid` argument must be different for every call to this function for a given Client object.
    /// If mixing calls to this function with calls to [Client::new_proxied_stream] or
    /// [Client::new_direct_stream], avoid using `sid >= 1<<31`, since these values are
    /// used automatically by those functions.
    pub fn new_stream_with_id<T: Into<StreamPeer>>(&self, to: T, sid: StreamId) -> ConnectingOutStream {
        let to = to.into();
        match sid {
            StreamId::Proxied(sid) => self.coord.new_stream(to, sid),
            StreamId::Direct(sid) if to.is_coord() => self.coord.new_stream(to, sid),
            StreamId::Direct(sid) => self.in_channels.new_stream(to, sid),
        }
    }

    /// Open a new channel directly to another client
    ///
    /// Note that a client that is not listening for new channels can nevertheless
    /// open a new channel to one that is listening.
    pub fn new_channel(&mut self, to: String) -> ConnectingChannel {
        let ctr = self.ctr;
        self.ctr += 1;
        self.new_channel_with_id(to, ctr)
    }

    /// Open a new channel directly to another client with an explicit channel-id
    ///
    /// The `cid` argument follows the same rules as the `sid` argument to [Client::new_stream_with_id].
    pub fn new_channel_with_id(&self, to: String, cid: u32) -> ConnectingChannel {
        self.coord.new_channel(to, cid)
    }

    /// Close an open channel
    ///
    /// Currently, attempting to open a channel after closing it causes what appears
    /// to be a transport error. XXX(#1)
    pub fn close_channel(&self, peer: String) -> ClosingChannel {
        self.in_channels.close_channel(peer)
    }
}
