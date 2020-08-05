use super::consts::{ALPN_CONEC, DFLT_PORT};
use super::types::{ConecChannel, ConecConnection};

use anyhow::{anyhow, Context, Result};
use futures::prelude::*;
use quinn::{Certificate, CertificateChain, Endpoint, Incoming, PrivateKey, ServerConfigBuilder};
use std::fs::read as fs_read;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct CoordConfig {
    laddr: SocketAddr,
    keylog: bool,
    cert: CertificateChain,
    key: PrivateKey,
}

impl CoordConfig {
    pub fn new(cert_path: PathBuf, key_path: PathBuf) -> Result<Self> {
        let key = {
            let tmp = fs_read(&key_path).context("failed to read private key")?;
            if key_path.extension().map_or(false, |x| x == "der") {
                PrivateKey::from_der(&tmp)?
            } else {
                PrivateKey::from_pem(&tmp)?
            }
        };
        let cert = {
            let tmp = fs_read(&cert_path).context("failed to read certificate chain")?;
            if cert_path.extension().map_or(false, |x| x == "der") {
                CertificateChain::from_certs(Certificate::from_der(&tmp))
            } else {
                CertificateChain::from_pem(&tmp)?
            }
        };
        Ok(Self {
            laddr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), DFLT_PORT),
            keylog: false,
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

    pub fn enable_keylog(&mut self) -> &mut Self {
        self.keylog = true;
        self
    }
}

pub(crate) struct CoordInner {
    endpoint: Endpoint,
    incoming: Incoming,
    clients: Vec<ConecChannel>,
}

// a shared reference to a Coordinator
pub(crate) struct CoordRef(Arc<Mutex<CoordInner>>);

impl CoordRef {
    pub(crate) fn new(endpoint: Endpoint, incoming: Incoming) -> Self {
        Self(Arc::new(Mutex::new(CoordInner {
            endpoint,
            incoming,
            clients: vec![],
        })))
    }
}

impl std::ops::Deref for CoordRef {
    type Target = Mutex<CoordInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct Coord {
    pub(crate) inner: CoordRef,
}

impl Coord {
    /// construct a new coord
    pub async fn new(config: CoordConfig) -> Result<Self> {
        // build configuration
        let mut qsc = ServerConfigBuilder::default();
        qsc.protocols(ALPN_CONEC);
        // qsc.use_stateless_retry(true); // XXX ???
        if config.keylog {
            qsc.enable_keylog();
        }
        qsc.certificate(config.cert, config.key)?;

        // build QUIC endpoint
        let mut endpoint = Endpoint::builder();
        endpoint.listen(qsc.build());
        let (endpoint, incoming) = endpoint.bind(&config.laddr)?;

        Ok(Self {
            inner: CoordRef::new(endpoint, incoming)
        })
    }

    /// accept a new connection from a client
    pub async fn accept(&mut self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let mut conn = ConecConnection::new(
            inner.incoming
                .next()
                .await
                .ok_or(anyhow!("accept failed: unexpected end of Incoming stream"))?
                .await
                .map_err(|e| anyhow!("accept failed: {}", e))?,
        );
        let ctrl = conn
            .accept_ctrl(None)
            .await
            .map_err(|e| anyhow!("failed to accept control stream: {}", e))?;
        inner.clients.push(ConecChannel { conn, ctrl });
        Ok(())
    }
}
