use super::consts::{ALPN_CONEC, DFLT_PORT};
use super::types::{ConecConnection, CtrlStream};

use anyhow::{anyhow, Result};
use quinn::{Certificate, ClientConfigBuilder, Endpoint, Incoming};

#[derive(Clone, Debug)]
pub struct ClientConfig {
    id: String,
    coord: String,
    port: u16,
    keylog: bool,
    extra_ca: Option<Certificate>,
}

impl ClientConfig {
    pub fn new(id: String, coord: String) -> Self {
        ClientConfig {
            id,
            coord,
            port: DFLT_PORT,
            keylog: false,
            extra_ca: None,
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
}

pub struct Client {
    endpoint: Endpoint,
    incoming: Incoming,
    connection: ConecConnection,
    coord: CtrlStream,
    peers: Vec<CtrlStream>,
}

impl Client {
    /// Construct a new Client
    pub async fn new(config: ClientConfig) -> Result<Self> {
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
        let (mut endpoint, incoming) = endpoint.bind(&"0.0.0.0:0".parse().unwrap())?;

        // set up the network endpoint and connect to the coordinator
        let mut connection =
            ConecConnection::connect(&mut endpoint, &config.coord[..], config.port)
                .await
                .map_err(|e| anyhow!("failed to set up coord connection: {}", e))?;

        // set up the control stream with the coordinator
        let coord = connection
            .connect_ctrl(config.id)
            .await
            .map_err(|e| anyhow!("failed to open coord control stream: {}", e))?;

        Ok(Client {
            endpoint,
            incoming,
            connection,
            coord,
            peers: vec![],
        })
    }

    /*
    // probably don't need this for now
    pub async fn accept(&mut self) -> Result<ConecConnection> {
        let conn = self.incoming
            .next()
            .await
            .ok_or(anyhow!("accept failed: unexpected end of Incoming stream"))?
            .await
            .map_err(|e| anyhow!("accept failed: {}", e))?;
        Ok(ConecConnection::new(conn))
    }
    */
}
