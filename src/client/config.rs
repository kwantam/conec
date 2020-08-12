// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use crate::consts::DFLT_PORT;

use quinn::Certificate;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub(super) id: String,
    pub(super) coord: String,
    pub(super) port: u16,
    pub(super) keylog: bool,
    pub(super) extra_ca: Option<Certificate>,
    pub(super) srcaddr: SocketAddr,
}

impl ClientConfig {
    pub fn new(id: String, coord: String) -> Self {
        Self {
            id,
            coord,
            port: DFLT_PORT,
            keylog: false,
            extra_ca: None,
            srcaddr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0),
        }
    }

    pub fn set_port(&mut self, port: u16) -> &mut Self {
        self.port = port;
        self
    }

    pub fn enable_keylog(&mut self) -> &mut Self {
        self.keylog = true;
        self
    }

    pub fn set_ca(&mut self, ca: Certificate) -> &mut Self {
        self.extra_ca = Some(ca);
        self
    }

    pub fn set_srcaddr(&mut self, src: SocketAddr) -> &mut Self {
        self.srcaddr = src;
        self
    }
}
