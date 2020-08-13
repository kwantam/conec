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

use quinn::Certificate;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

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
    pub(super) srcaddr: SocketAddr,
}

impl ClientConfig {
    ///! Construct new ClientConfig
    ///
    /// - `id` is the Client's name, which will be used to identify it to other clients.
    /// - `coord` is the hostname of the coordinator. The coordinator's TLS certificate must match this name.
    ///
    /// By default, Client will attempt to resolve the hostname `coord` and connect
    /// on the default port. Use [set_port] to change the port number, or use
    /// [set_addr] to specify a [SocketAddr](std::net::SocketAddr) rather than
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
            srcaddr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
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
    pub fn set_ca(&mut self, ca: Certificate) -> &mut Self {
        self.extra_ca = Some(ca);
        self
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
}
