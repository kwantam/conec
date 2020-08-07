pub(crate) mod config;

use super::consts::{ALPN_CONEC};
use super::types::{ConecConnection, CtrlStream};
use config::ClientConfig;

use quinn::{ClientConfigBuilder, Endpoint, Incoming};
use std::io;

struct ClientChan {
    conn: ConecConnection,
    ctrl: CtrlStream,
    peer: Option<String>,
}

pub struct Client {
    endpoint: Endpoint,
    incoming: Incoming,
    coord: ClientChan,
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
        let (ctrl, peer) = conn.connect_ctrl(config.id).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("failed to open coord control stream: {}", e),
            )
        })?;

        Ok(Self {
            endpoint,
            incoming,
            coord: ClientChan { conn, ctrl, peer },
        })
    }
}
