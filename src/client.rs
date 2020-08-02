use super::consts::{ALPN_CONEC, DFLT_PORT};

use anyhow::{anyhow, Result};
use quinn::{
    Certificate, ClientConfigBuilder, Connection, Endpoint, Incoming, IncomingBiStreams,
    IncomingUniStreams, NewConnection, SendStream, RecvStream,
};
use std::net::ToSocketAddrs;

#[derive(Clone, Debug)]
pub struct ClientConfig {
    coord: String,
    port: u16,
    keylog: bool,
    extra_ca: Option<Certificate>,
}

impl ClientConfig {
    pub fn new(coord: String) -> Self {
        ClientConfig {
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
    coordinator: CoordinatorChannel,
    peers: Vec<ClientChannel>,
}

pub struct CoordinatorChannel {
    connection: Connection,
    iu_streams: IncomingUniStreams,
    ib_streams: IncomingBiStreams,
    control: InOutStream,
}

pub struct ClientChannel {
    control: InOutStream,
}

pub struct InOutStream {
    send: SendStream,
    recv: RecvStream,
}

pub struct InStream {
    recv: RecvStream,
}

pub struct OutStream {
    send: SendStream,
}

impl Client {
    /// Construct a new Client
    pub async fn new(config: ClientConfig) -> Result<Self> {
        // build the default client configuration
        let mut quinn_client_config = ClientConfigBuilder::default();
        quinn_client_config.protocols(ALPN_CONEC);
        if config.keylog {
            quinn_client_config.enable_keylog();
        }
        if let Some(ca) = config.extra_ca {
            quinn_client_config.add_certificate_authority(ca)?;
        }

        // build the QUIC endpoint
        let mut endpoint = Endpoint::builder();
        endpoint.default_client_config(quinn_client_config.build());
        let (endpoint, incoming) = endpoint.bind(&"0.0.0.0:0".parse().unwrap())?;

        // connect to the coordinator
        let coord_addr = (&config.coord[..], config.port)
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| anyhow!("could not resolve coordinator address"))?;
        let NewConnection {
            connection: conn,
            uni_streams: u_str,
            bi_streams: b_str,
            ..
        } = {
            endpoint
                .connect(&coord_addr, &config.coord[..])?
                .await
                .map_err(|e| anyhow!("failed to connect: {}", e))?
        };
        let (cc_send, cc_recv) = conn
            .open_bi()
            .await
            .map_err(|e| anyhow!("failed to open coord control stream: {}", e))?;

        Ok(Client {
            endpoint: endpoint,
            incoming: incoming,
            coordinator: CoordinatorChannel {
                connection: conn,
                iu_streams: u_str,
                ib_streams: b_str,
                control: InOutStream {
                    send: cc_send,
                    recv: cc_recv,
                }
            },
            peers: vec!(),
        })
    }
}
