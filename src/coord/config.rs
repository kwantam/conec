// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use crate::consts::DFLT_PORT;

use err_derive::Error;
use quinn::{Certificate, CertificateChain, ParseError, PrivateKey};
use std::fs::read as fs_read;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

///! Errors when constructing a new coordinator configuration
#[derive(Debug, Error)]
pub enum CoordConfigError {
    ///! Failed to read certificate or key file
    #[error(display = "Reading certificate or key file: {:?}", _0)]
    ReadingCertOrKey(#[source] io::Error),
    ///! Failed to parse certificate or key
    #[error(display = "Parsing certificate or key: {:?}", _0)]
    ParsingCertOrKey(#[source] ParseError),
}

///! Coordinator configuration struct
///
/// See [library documentation](index.html) for usage example.
#[derive(Clone, Debug)]
pub struct CoordConfig {
    pub(super) laddr: SocketAddr,
    pub(super) keylog: bool,
    pub(super) stateless_retry: bool,
    pub(super) cert: CertificateChain,
    pub(super) key: PrivateKey,
}

impl CoordConfig {
    ///! Construct a new coordinator configuration
    ///
    /// - `cert_path` is the path to a certificate in PEM or DER format
    /// - `key_path` is the path to the corresponding key in PEM or DER format
    pub fn new(cert_path: PathBuf, key_path: PathBuf) -> Result<Self, CoordConfigError> {
        let key = {
            let tmp = fs_read(&key_path)?;
            if key_path.extension().map_or(false, |x| x == "der") {
                PrivateKey::from_der(&tmp)
            } else {
                PrivateKey::from_pem(&tmp)
            }
        }?;
        let cert = {
            let tmp = fs_read(&cert_path)?;
            if cert_path.extension().map_or(false, |x| x == "der") {
                CertificateChain::from_certs(Certificate::from_der(&tmp))
            } else {
                CertificateChain::from_pem(&tmp)?
            }
        };
        Ok(Self {
            laddr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), DFLT_PORT),
            keylog: false,
            stateless_retry: false,
            cert,
            key,
        })
    }

    ///! Set listen port number to `port`
    pub fn set_port(&mut self, port: u16) -> &mut Self {
        self.laddr.set_port(port);
        self
    }

    ///! Set listen address to `ip`
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

    ///! Enable logging key material to the file specified by the environment variable `SSLKEYLOGFILE`.
    pub fn enable_keylog(&mut self) -> &mut Self {
        self.keylog = true;
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
}
