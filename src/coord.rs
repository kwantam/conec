use super::consts::{ALPN_CONEC, DFLT_PORT};
use super::types::{ConecChannel, ConecConnection};

use anyhow::{anyhow, Context, Result};
use futures::prelude::*;
use quinn::{Certificate, CertificateChain, Endpoint, Incoming, PrivateKey, ServerConfigBuilder};
use std::fs::read as fs_read;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

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

pub struct Coord {
    endpoint: Endpoint,
    incoming: Incoming,
    clients: Vec<ConecChannel>,
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
            endpoint,
            incoming,
            clients: vec![],
        })
    }

    /// accept a new connection from a client
    pub async fn accept(&mut self) -> Result<()> {
        let mut conn = ConecConnection::new(
            self.incoming
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
        self.clients.push(ConecChannel { conn, ctrl });
        Ok(())
    }
}
