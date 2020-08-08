pub(crate) mod config;

use super::consts::ALPN_CONEC;
use super::types::{ConecConn, CtrlStream};
use config::ClientConfig;

// use futures::channel::mpsc;
use quinn::{ClientConfigBuilder, Endpoint};
use std::io;

/*
enum ClientEvent {
    Error(io::Error),
}
*/

pub(super) struct ClientChan {
    pub(super) conn: ConecConn,
    pub(super) ctrl: CtrlStream,
}

pub struct Client {
    endpoint: Endpoint,
    // incoming: Incoming,
    coord: ClientChan,
    /*
    sender: mpsc::UnboundedSender<ClientEvent>,
    events: mpsc::UnboundedReceiver<ClientEvent>,
    */
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
        let (mut endpoint, _incoming) = endpoint
            .bind(&config.srcaddr)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // set up the network endpoint and connect to the coordinator
        let mut conn = ConecConn::connect(&mut endpoint, &config.coord[..], config.port)
            .await
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("failed to set up coord connection: {}", e),
                )
            })?;

        // set up the control stream with the coordinator
        let ctrl = conn.accept_ctrl(config.id).await.map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("failed to open coord control stream: {}", e),
            )
        })?;

        // let (sender, events) = mpsc::unbounded();
        Ok(Self {
            endpoint,
            // incoming,
            coord: ClientChan { conn, ctrl },
            // sender,
            // events,
        })
    }
}
