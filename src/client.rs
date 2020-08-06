use super::consts::{ALPN_CONEC, DFLT_PORT};
use super::types::{ConecChannel, ConecConnection, CtrlStream};

use quinn::{Certificate, ClientConfigBuilder, Endpoint, Incoming};
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[derive(Clone, Debug)]
pub struct ClientConfig {
    id: String,
    coord: String,
    port: u16,
    keylog: bool,
    extra_ca: Option<Certificate>,
    srcaddr: SocketAddr,
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

pub struct Client {
    endpoint: Endpoint,
    incoming: Incoming,
    coord: ConecChannel,
    peers: Vec<CtrlStream>, // TODO: ConecChannel eventually
}

impl Client {
    /// Construct a new Client
    pub async fn new(config: ClientConfig) -> io::Result<Self> {
        // build the client configuration
        let mut qcc = ClientConfigBuilder::default();
        qcc.protocols(ALPN_CONEC);
        if config.keylog {
            qcc.enable_keylog();
        }
        if let Some(ca) = config.extra_ca {
            qcc.add_certificate_authority(ca)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        }

        // build the QUIC endpoint
        let mut endpoint = Endpoint::builder();
        endpoint.default_client_config(qcc.build());
        let (mut endpoint, incoming) = endpoint
            .bind(&config.srcaddr)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // set up the network endpoint and connect to the coordinator
        let mut conn = ConecConnection::connect(&mut endpoint, &config.coord[..], config.port)
            .await
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("failed to set up coord connection: {}", e),
                )
            })?;

        // set up the control stream with the coordinator
        let ctrl = conn.connect_ctrl(config.id).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("failed to open coord control stream: {}", e),
            )
        })?;

        Ok(Self {
            endpoint,
            incoming,
            coord: ConecChannel { conn, ctrl },
            peers: vec![],
        })
    }
}
