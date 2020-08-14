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
use config::{CertGenError, ClientConfig};
pub use istream::{
    ConnectingInStream, InStreamError, IncomingStreams, IncomingStreamsError, NewInStream,
};
use istream::{IncomingStreamsDriver, IncomingStreamsRef};

use err_derive::Error;
use quinn::{
    crypto::rustls::TLSError, Certificate, ClientConfigBuilder, Endpoint, EndpointError,
    ParseError, ServerConfigBuilder,
};
use ring::signature::{EcdsaKeyPair, ECDSA_P256_SHA256_ASN1_SIGNING};

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
    ///! Generating certificate for client failed
    #[error(display = "Generating certificate for client: {:?}", _0)]
    CertificateGen(#[source] CertGenError),
    ///! Error setting up certificate chain
    #[error(display = "Certificate chain: {:?}", _0)]
    CertificateChain(#[source] TLSError),
    ///! Error with client ephemeral key
    #[error(display = "Ephemeral key: {:?}", _0)]
    PrivateKey(#[error(from)] ring::error::KeyRejected),
    ///! Error parsing client ephemeral cert
    #[error(display = "Ephemeral cert: {:?}", _0)]
    CertificateParse(#[source] ParseError),
}

///! The Client end of a connection to the Coordinator
///
/// See [library documentation](../index.html) for an example of constructing a Client.
pub struct Client {
    endpoint: Endpoint,
    coord: ClientChan,
    ctr: u32,
    pub(crate) _cert: Certificate,
    pub(crate) _key: EcdsaKeyPair,
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
        let clt_cert = Certificate::from_der(cert.iter().next().unwrap().as_ref())?;
        let clt_key = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &key)?;
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
        if config.listen {
            // build a server configuration, too
            let mut qsc = ServerConfigBuilder::default();
            qsc.protocols(ALPN_CONEC);
            qsc.use_stateless_retry(config.stateless_retry);
            if config.keylog {
                qsc.enable_keylog();
            }
            qsc.certificate(cert, privkey)?;
            // set server config on endpoint
            endpoint.listen(qsc.build());
        }
        let (mut endpoint, _incoming) = endpoint.bind(&config.srcaddr)?;

        // set up the network endpoint and connect to the coordinator
        let (mut conn, iuni) =
            ConecConn::connect(&mut endpoint, &config.coord, config.addr, None).await?;

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

        Ok((
            Self {
                endpoint,
                coord,
                ctr: 1u32 << 31,
                _cert: clt_cert,
                _key: clt_key,
            },
            istrms,
        ))
    }

    ///! Open a new stream to another client, proxied through the Coordinator
    pub fn new_stream(&mut self, to: String) -> Result<ConnectingOutStream, ClientChanError> {
        let ctr = self.ctr;
        self.ctr += 1;
        self.coord.new_stream(to, ctr)
    }

    ///! Open a new proxied stream to another client with an explicit stream-id
    ///
    /// The `sid` argument must be different for every call to this function for a given Client object.
    /// If mixing calls to this function with calls to [new_strema], avoid using sid larger than 1<<31:
    /// those values are used automatically by that function.
    pub fn new_stream_with_sid(
        &self,
        to: String,
        sid: u32,
    ) -> Result<ConnectingOutStream, ClientChanError> {
        self.coord.new_stream(to, sid)
    }

    ///! Return the local address that Client is bound to
    pub fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.endpoint.local_addr()
    }
}
