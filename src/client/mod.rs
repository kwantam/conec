mod chan;
pub(crate) mod config;
mod istream;

use super::consts::ALPN_CONEC;
use super::types::{ConecConn, ConecConnError};
use chan::{ClientChan, ClientChanDriver, ClientChanRef};
pub use chan::{ClientChanError, ConnectingOutStream};
use config::ClientConfig;
pub use istream::{ConnectingInStream, IncomingStreams};
use istream::{IncomingStreamsDriver, IncomingStreamsRef};

use err_derive::Error;
use quinn::{ClientConfigBuilder, Endpoint, EndpointError};

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

pub struct Client {
    endpoint: Endpoint,
    coord: ClientChan,
}

impl Client {
    /// Construct a new Client
    pub async fn new(config: ClientConfig) -> Result<(Self, IncomingStreams), ClientError> {
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
        let (mut conn, iuni) =
            ConecConn::connect(&mut endpoint, &config.coord[..], config.port).await?;

        // set up the control stream with the coordinator
        let ctrl = conn
            .accept_ctrl(config.id.clone())
            .await
            .map_err(ClientError::AcceptCtrl)?;

        // set up the client-coordinator channel and spawn its driver
        let (inner, i_client, i_bye) = ClientChanRef::new(conn, ctrl, config.id);
        let driver = ClientChanDriver(inner.clone());
        tokio::spawn(async move { driver.await });
        let coord = ClientChan(inner);

        // set up the incoming streams listener
        let istrms = {
            let inner = IncomingStreamsRef::new(i_client, i_bye, iuni);
            let driver = IncomingStreamsDriver(inner.clone());
            tokio::spawn(async move { driver.await });
            IncomingStreams(inner)
        };

        Ok((Self { endpoint, coord }, istrms))
    }

    /// open a new proxy stream to another client
    pub fn new_stream(&self, to: String, cid: u32) -> Result<ConnectingOutStream, ClientChanError> {
        self.coord.new_stream(to, cid)
    }
}
