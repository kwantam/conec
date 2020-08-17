// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use crate::consts::DFLT_PORT;
use crate::types::ConecConnAddr;
use crate::util::{get_cert, get_cert_and_key, CertReadError};

use err_derive::Error;
use quinn::{Certificate, CertificateChain, ParseError, PrivateKey};
use rcgen::{generate_simple_self_signed, RcgenError};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;

///! Client configuration struct
///
/// See [library documentation](index.html) for usage example.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub(super) id: String,
    pub(super) coord: String,
    pub(super) addr: ConecConnAddr,
    pub(super) keylog: bool,
    pub(super) extra_ca: Option<Certificate>,
    pub(super) client_ca: Option<Certificate>,
    pub(super) srcaddr: SocketAddr,
    pub(super) cert_and_key: Option<(CertificateChain, PrivateKey, Vec<u8>)>,
    pub(super) stateless_retry: bool,
    pub(super) listen: bool,
}

///! Error during Client certificate generation
#[derive(Debug, Error)]
pub enum CertGenError {
    #[error(display = "Generating self-signed cert: {:?}", _0)]
    Generating(#[source] RcgenError),
    #[error(display = "Parsing secret key: {:?}", _0)]
    Parse(#[source] ParseError),
}

impl ClientConfig {
    ///! Construct new ClientConfig
    ///
    /// - `id` is the Client's name, which will be used to identify it to other clients.
    /// - `coord` is the hostname of the coordinator. The coordinator's TLS certificate must match this name.
    ///
    /// By default, Client will attempt to resolve the hostname `coord` and connect
    /// on the default port. Use [set_port](ClientConfig::set_port) to change the port number, or use
    /// [set_addr](ClientConfig::set_addr) to specify a [SocketAddr](std::net::SocketAddr) rather than
    /// relying on name resolution.
    ///
    /// In all cases, the Client will ensure that the Coordinator's TLS certificate
    /// matches the hostname specified as `coord`.
    pub fn new(id: String, coord: String) -> Self {
        Self {
            id,
            coord,
            addr: ConecConnAddr::Portnum(DFLT_PORT),
            keylog: false,
            extra_ca: None,
            client_ca: None,
            srcaddr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            cert_and_key: None,
            stateless_retry: false,
            listen: true,
        }
    }

    ///! Set the Coordinator's port number to `port`
    pub fn set_port(&mut self, port: u16) -> &mut Self {
        self.addr = ConecConnAddr::Portnum(port);
        self
    }

    ///! Set the Coordinator's address to `addr`, disabling name resolution
    ///
    /// Note that Client will still ensure that Coordinator's TLS certificate
    /// matches the name specified to [ClientConfig::new].
    pub fn set_addr(&mut self, addr: SocketAddr) -> &mut Self {
        self.addr = ConecConnAddr::Sockaddr(addr);
        self
    }

    ///! Enable logging key material to the file specified by the environment variable `SSLKEYLOGFILE`.
    pub fn enable_keylog(&mut self) -> &mut Self {
        self.keylog = true;
        self
    }

    ///! Add a trusted certificate authority
    ///
    /// This certificate authority is used to validate the Coordinator's certificate.
    pub fn set_ca(&mut self, ca: Certificate) -> &mut Self {
        self.extra_ca = Some(ca);
        self
    }

    ///! Add a trusted certificate authority from a file
    ///
    /// This is a convenience wrapper around [ClientConfig::set_ca].
    /// Both PEM and DER formats are supported.
    pub fn set_ca_from_file(&mut self, cert_path: &Path) -> Result<&mut Self, CertReadError> {
        Ok(self.set_ca(get_cert(cert_path)?))
    }

    ///! Add a trusted certificate authority for checking Client certs
    ///
    /// If no trusted CA is provided, self-signed Client certificates are required.
    pub fn set_client_ca(&mut self, ca: Certificate) -> &mut Self {
        self.client_ca = Some(ca);
        self
    }

    ///! Add a trusted certificate authority for checking Client certs from a file
    ///
    /// This is a convenience wrapper around [ClientConfig::set_client_ca].
    /// Both PEM and DER formats are supported.
    pub fn set_client_ca_from_file(
        &mut self,
        cert_path: &Path,
    ) -> Result<&mut Self, CertReadError> {
        Ok(self.set_client_ca(get_cert(cert_path)?))
    }

    ///! Set the Client's source address explicitly
    ///
    /// By default, the source address is set to `0.0.0.0:0`. To bind to a host-assigned
    /// IPv6 port instead, one might call
    ///
    /// ```
    /// # use std::net::{IpAddr, Ipv6Addr, SocketAddr};
    /// # let mut client_cfg = conec::ClientConfig::new("asdf".to_string(), "qwer".to_string());
    /// client_cfg.set_srcaddr(SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0));
    /// ```
    pub fn set_srcaddr(&mut self, src: SocketAddr) -> &mut Self {
        self.srcaddr = src;
        self
    }

    ///! Enable QUIC stateless retry.
    ///
    /// Per QUIC spec, stateless retry defends against client address spoofing.
    /// The downside is that this adds another round-trip to new connections.
    pub fn enable_stateless_retry(&mut self) -> &mut Self {
        self.stateless_retry = true;
        self
    }

    ///! Disable Client listening for incoming direct connections
    ///
    /// This means that all streams must be proxed through Coordinator
    pub fn disable_listen(&mut self) -> &mut Self {
        self.listen = false;
        self
    }

    ///! Set a certificate and key for Client
    ///
    /// This certificate is used to authenticate to the Coordinator and when accepting
    /// direct connections from other clients.
    ///
    /// To be usable, a certificate must meet two criteria:
    ///
    /// - It must be valid for the Client `id` provided to [ClientConfig::new],
    /// otherwise the coordinator will reject the connection.
    ///
    /// - If the Coordinator is configured to accept self-signed certificates
    /// (which is the default), this certificate must be self-signed. Otherwise, if the
    /// coordinator is configured to accept certificates signed by a particular CA
    /// (via [CoordConfig::set_client_ca](crate::CoordConfig::set_client_ca)), this certificate must
    /// be signed by that CA.
    pub fn set_cert(
        &mut self,
        cert: CertificateChain,
        key: PrivateKey,
        key_der: Vec<u8>,
    ) -> &mut Self {
        self.cert_and_key = Some((cert, key, key_der));
        self
    }

    ///! Set a certificate and key for Client from file
    ///
    /// This is a convenience wrapper around [ClientConfig::set_cert].
    /// Both PEM and DER formats are supported.
    pub fn set_cert_from_file(
        &mut self,
        cert_path: &Path,
        key_path: &Path,
    ) -> Result<&mut Self, CertReadError> {
        let (cert, key, key_der) = get_cert_and_key(cert_path, key_path)?;
        Ok(self.set_cert(cert, key, key_der))
    }

    pub(super) fn gen_certs(&mut self) -> Result<(), CertGenError> {
        if self.cert_and_key.is_some() {
            return Ok(());
        }

        let cert = generate_simple_self_signed(&[self.id.clone()][..])?;
        let key = cert.serialize_private_key_der();
        let cert = CertificateChain::from_certs(Certificate::from_der(&cert.serialize_der()?));
        self.cert_and_key = Some((cert, PrivateKey::from_der(&key)?, key));
        Ok(())
    }
}
