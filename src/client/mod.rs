pub(crate) mod config;

use super::consts::ALPN_CONEC;
use super::types::{ConecConn, ConecConnError, CtrlStream};
use config::ClientConfig;

use err_derive::Error;
use quinn::{ClientConfigBuilder, Endpoint, EndpointError};

/*
enum ClientEvent {
    Error(io::Error),
}
*/

#[derive(Debug, Error)]
pub enum ClientError {
    #[error(display = "Adding certificate authority: {:?}", _0)]
    CertificateAuthority(#[source] webpki::Error),
    #[error(display = "Binding port: {:?}", _0)]
    Bind(#[source] EndpointError),
    #[error(display = "Connecting to coordinator: {:?}", _0)]
    Connect(#[source] ConecConnError),
    #[error(display = "Accepting control stream from coordinator: {:?}", _0)]
    AcceptCtrl(#[error(source, no_from)] ConecConnError),
}

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
    pub async fn new(config: ClientConfig) -> Result<Self, ClientError> {
        // build the client configuration
        let mut qcc = ClientConfigBuilder::default();
        qcc.protocols(ALPN_CONEC);
        if config.keylog {
            qcc.enable_keylog();
        }
        if let Some(ca) = config.extra_ca {
            qcc.add_certificate_authority(ca)?;
        }

        // build the QUIC endpoint
        let mut endpoint = Endpoint::builder();
        endpoint.default_client_config(qcc.build());
        let (mut endpoint, _incoming) = endpoint.bind(&config.srcaddr)?;

        // set up the network endpoint and connect to the coordinator
        let mut conn = ConecConn::connect(&mut endpoint, &config.coord[..], config.port).await?;

        // set up the control stream with the coordinator
        let ctrl = conn
            .accept_ctrl(config.id)
            .await
            .map_err(ClientError::AcceptCtrl)?;

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
