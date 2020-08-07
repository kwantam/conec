use crate::consts::DFLT_PORT;

use quinn::{Certificate, CertificateChain, PrivateKey};
use std::fs::read as fs_read;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct CoordConfig {
    pub(super) laddr: SocketAddr,
    pub(super) keylog: bool,
    pub(super) stateless_retry: bool,
    pub(super) cert: CertificateChain,
    pub(super) key: PrivateKey,
}

impl CoordConfig {
    pub fn new(cert_path: PathBuf, key_path: PathBuf) -> io::Result<Self> {
        let key = {
            let tmp = fs_read(&key_path).map_err(|e| {
                io::Error::new(e.kind(), format!("failed to read private key: {}", e))
            })?;
            if key_path.extension().map_or(false, |x| x == "der") {
                PrivateKey::from_der(&tmp).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            } else {
                PrivateKey::from_pem(&tmp).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
            }
        };
        let cert = {
            let tmp = fs_read(&cert_path).map_err(|e| {
                io::Error::new(e.kind(), format!("failed to read certificate chain: {}", e))
            })?;
            if cert_path.extension().map_or(false, |x| x == "der") {
                CertificateChain::from_certs(Certificate::from_der(&tmp))
            } else {
                CertificateChain::from_pem(&tmp)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
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

    pub fn set_port(&mut self, port: u16) -> &mut Self {
        self.laddr.set_port(port);
        self
    }

    pub fn set_ip(&mut self, ip: IpAddr) -> &mut Self {
        self.laddr.set_ip(ip);
        self
    }

    // log master secret to ENV{SSLKEYLOGFILE}
    pub fn enable_keylog(&mut self) -> &mut Self {
        self.keylog = true;
        self
    }

    // Per QUIC spec, stateless retry defends against client address spoofing.
    pub fn enable_stateless_retry(&mut self) -> &mut Self {
        self.stateless_retry = true;
        self
    }
}
