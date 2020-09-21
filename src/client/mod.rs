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
mod connstream;
mod holepunch;
mod ichan;
mod istream;
mod tls;

use crate::consts::ALPN_CONEC;
use crate::types::{ConecConn, ConecConnError};
use crate::Coord;
pub use chan::{BroadcastCounting, BroadcastCountingError, ClientChanError};
use chan::{ClientChan, ClientChanDriver, ClientChanRef};
use config::{CertGenError, ClientConfig};
use connstream::ConnectingStreamHandle;
pub use connstream::{ConnectingStream, ConnectingStreamError};
use holepunch::{Holepunch, HolepunchDriver, HolepunchEvent, HolepunchRef};
pub use ichan::{ClosingChannel, ConnectingChannel, IncomingChannelsError, NewChannelError};
use ichan::{IncomingChannels, IncomingChannelsDriver, IncomingChannelsRef};
pub use istream::{IncomingStreams, NewInStream, StreamId};
use istream::{IncomingStreamsDriver, IncomingStreamsRef};

use err_derive::Error;
use futures::channel::mpsc;
use quinn::{crypto::rustls::TLSError, ClientConfigBuilder, Endpoint, EndpointError, ParseError};
use std::net::UdpSocket;

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
def_into_error!(ClientError);

/// The Client end of a connection to the Coordinator
///
/// See [library documentation](../index.html) for an example of constructing a Client.
pub struct Client {
    #[allow(dead_code)]
    in_streams: IncomingStreamsRef,
    in_channels: IncomingChannels,
    #[allow(dead_code)]
    holepunch: Option<Holepunch>,
    coord: ClientChan,
    ctr: u64,
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
        let (socket, (mut endpoint, incoming)) = {
            let socket = UdpSocket::bind(&config.srcaddr).map_err(EndpointError::Socket)?;
            let socket2 = socket.try_clone().map_err(EndpointError::Socket)?;
            (socket, endpoint.with_socket(socket2)?)
        };

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
        let (chan_sender, chan_events) = mpsc::unbounded(); // ClientChan events channel
        let (in_channels, ichan_sender) = {
            let (inner, sender) = IncomingChannelsRef::new(
                endpoint,
                config.id,
                config.keepalive,
                incoming,
                stream_sender.clone(),
                qcc,
                config.client_ca,
                chan_sender.clone(),
            );
            let driver = IncomingChannelsDriver(inner.clone());
            tokio::spawn(async move { driver.await });
            (IncomingChannels::new(inner, sender.clone()), sender)
        };

        let (holepunch, holepunch_sender) = if config.holepunch && config.listen {
            let (inner, sender) = HolepunchRef::new(socket);
            let driver = HolepunchDriver(inner.clone());
            tokio::spawn(async move { driver.await });
            (Some(Holepunch(inner)), Some(sender))
        } else {
            (None, None)
        };

        // client-coordinator channel
        let coord = {
            let inner = ClientChanRef::new(conn, ctrl, ichan_sender, holepunch_sender, chan_events, config.listen);
            let driver = ClientChanDriver::new(inner.clone(), config.keepalive);
            tokio::spawn(async move { driver.await });
            ClientChan::new(inner, chan_sender)
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
                holepunch,
                coord,
                ctr: 1u64 << 63,
            },
            incoming_streams,
        ))
    }

    /// Open a new stream to another client, proxied through the Coordinator
    pub fn new_proxied_stream(&mut self, to: String) -> ConnectingStream {
        self.new_x_stream(to, StreamId::Proxied)
    }

    /// Open a new stream to another client via a direct channel.
    ///
    /// It is only possible to open another stream to a client for which there is
    /// an open channel, either because that client connected to this one or because
    /// this client called [Client::new_channel].
    pub fn new_direct_stream(&mut self, to: String) -> ConnectingStream {
        self.new_x_stream(to, StreamId::Direct)
    }

    fn new_x_stream<F>(&mut self, to: String, as_id: F) -> ConnectingStream
    where
        F: FnOnce(u64) -> StreamId,
    {
        let sid = as_id(self.ctr);
        self.ctr += 1;
        self.new_stream_with_id(to, sid)
    }

    /// Open a new proxied stream to another client with an explicit stream-id. This can
    /// be useful for coordination in applications where peers share multiple data streams
    /// (e.g., clients might agree that sid 1 is for values of type T1, sid 2 is for values
    /// of type T2, etc.).
    ///
    /// The `sid` argument must be different for every call to this function for a given Client object.
    /// If mixing calls to this function with calls to [Client::new_proxied_stream] or
    /// [Client::new_direct_stream], avoid using `sid >= 1<<63`, since these values are
    /// used automatically by those functions.
    pub fn new_stream_with_id(&self, to: String, sid: StreamId) -> ConnectingStream {
        match sid {
            StreamId::Proxied(sid) => self.coord.new_stream(to, sid),
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
        self.in_channels.new_channel(to, ctr)
    }

    /// Close an open channel
    ///
    /// Currently, attempting to re-open a channel after closing causes what appears
    /// to be a transport error. XXX(#1)
    pub fn close_channel(&self, peer: String) -> ClosingChannel {
        self.in_channels.close_channel(peer)
    }

    /// Open or connect to a broadcast stream
    ///
    /// A broadcast stream is a many-to-many stream proxied through the Coordinator.
    /// Any Client who knows the stream's name can send to and receive from it.
    ///
    /// Broadcast streams may suffer from the slow receiver problem: senders cannot
    /// make progress until the slowest receiver drains its incoming buffer. The
    /// [NonblockingInStream](crate::NonblockingInStream) adapter may help to address
    /// this issue.
    pub fn new_broadcast(&mut self, chan: String) -> ConnectingStream {
        let ctr = self.ctr;
        self.ctr += 1;
        self.coord.new_broadcast(chan, ctr)
    }

    /// Open a new stream to another client
    ///
    /// This function first attempts to open a direct stream to the client and then,
    /// if that fails, falls back to a proxied stream through the Coordinator.
    pub fn new_stream(&mut self, to: String) -> ConnectingStream {
        let csnd = self.coord.get_sender();
        let isnd = self.in_channels.get_sender();
        let conn_chan = self.new_channel(to.clone());
        let sid = self.ctr;
        self.ctr += 1;
        ConnectingStream::new(Some((conn_chan, csnd, isnd, to, sid))).0
    }

    /// Count the current members of a broadcast channel
    ///
    /// Request from Coordinator the current count of senders and receivers on
    /// a given broadcast channel. The result is a future that, when forced,
    /// returns either an error or the tuple `(#senders, #receivers)`.
    pub fn get_broadcast_count(&mut self, chan: String) -> BroadcastCounting {
        let ctr = self.ctr;
        self.ctr += 1;
        self.coord.get_broadcast_count(chan, ctr)
    }
}
