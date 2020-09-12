// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use crate::consts::DFLT_PORT;
use crate::util::{get_cert, get_cert_and_key, CertReadError};

use quinn::{Certificate, CertificateChain, PrivateKey};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;

/// Coordinator configuration struct
///
/// See [library documentation](index.html) for usage example.
#[derive(Clone, Debug)]
pub struct CoordConfig {
    pub(super) laddr: SocketAddr,
    pub(super) keylog: bool,
    pub(super) stateless_retry: bool,
    pub(super) cert_and_key: (CertificateChain, PrivateKey),
    pub(super) client_ca: Option<Certificate>,
}

impl CoordConfig {
    /// Construct a new coordinator configuration from files.
    ///
    /// This is a conveniece wrapper around [CoordConfig::new].
    /// Both PEM and DER formats are supported.
    pub fn new_from_file(cert_path: &Path, key_path: &Path) -> Result<Self, CertReadError> {
        let (cert, key, _) = get_cert_and_key(cert_path, key_path)?;
        Ok(Self::new(cert, key))
    }

    /// Construct a new coordinator configuration using the given CertificateChainand PrivateKey.
    pub fn new(cert: CertificateChain, key: PrivateKey) -> Self {
        Self {
            laddr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), DFLT_PORT),
            keylog: false,
            stateless_retry: false,
            cert_and_key: (cert, key),
            client_ca: None,
        }
    }

    /// Set listen port number to `port`
    pub fn set_port(&mut self, port: u16) -> &mut Self {
        self.laddr.set_port(port);
        self
    }

    /// Set listen address to `ip`
    ///
    /// By default, the listen address is set to `0.0.0.0:0`. To bind to a host-assigned
    /// IPv6 port instead, one might call
    ///
    /// ```ignore
    /// coord_cfg.set_ip(IpAddr::V6(Ipv6Addr::UNSPECIFIED));
    /// ```
    pub fn set_ip(&mut self, ip: IpAddr) -> &mut Self {
        self.laddr.set_ip(ip);
        self
    }

    /// Enable logging key material to the file specified by the environment variable `SSLKEYLOGFILE`.
    pub fn enable_keylog(&mut self) -> &mut Self {
        self.keylog = true;
        self
    }

    /// Enable QUIC stateless retry.
    ///
    /// Per QUIC spec, stateless retry defends against client address spoofing.
    /// The downside is that this adds another round-trip to new connections.
    pub fn enable_stateless_retry(&mut self) -> &mut Self {
        self.stateless_retry = true;
        self
    }

    /// Add a trusted certificate authority for checking Client certificates
    ///
    /// If no trusted CA is provided, self-signed Client certificates are required.
    pub fn set_client_ca(&mut self, ca: Certificate) -> &mut Self {
        self.client_ca = Some(ca);
        self
    }

    /// Add a trusted certificate authority for checking Client certs from a file
    ///
    /// This is a convenience wrapper around [CoordConfig::set_client_ca].
    /// Both PEM and DER formats are supported.
    pub fn set_client_ca_from_file(&mut self, cert_path: &Path) -> Result<&mut Self, CertReadError> {
        Ok(self.set_client_ca(get_cert(cert_path)?))
    }
}
