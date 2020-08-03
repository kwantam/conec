use super::consts::{ALPN_CONEC, DFLT_PORT};
use super::types::{CtrlStream, ConecConnection};

use anyhow::{anyhow, Result};
use quinn::{Certificate, ClientConfigBuilder};

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
    network: ConecConnection,
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

        // set up the network endpoint and connect to the coordinator
        let network = ConecConnection::connect(qcc.build(), &config.coord[..], config.port)
            .await
            .map_err(|e| anyhow!("failed to set up ConecConnection: {}", e))?;

        // set up the control stream with the coordinator
        let (cc_send, cc_recv) = network
            .get_connection()
            .open_bi()
            .await
            .map_err(|e| anyhow!("failed to open coord control stream: {}", e))?;
        let mut ctrl_stream = CtrlStream::new(cc_send, cc_recv);
        ctrl_stream.send_hello(config.id)
            .await
            .map_err(|e| anyhow!("error sending hello: {}", e))?;

        Ok(Client {
            network,
            coord: ctrl_stream,
            peers: vec!(),
        })
    }
}
