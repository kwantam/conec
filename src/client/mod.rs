pub(crate) mod config;
mod incomingstream;

use super::consts::ALPN_CONEC;
use super::types::{ConecConn, ConecConnError, CtrlStream};
use config::ClientConfig;
use futures::channel::oneshot;
pub use incomingstream::IncomingStreams;
use incomingstream::{IncomingStreamsDriver, IncomingStreamsRef};

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

struct ClientChan {
    conn: ConecConn,
    ctrl: CtrlStream,
    incs_bye_in: oneshot::Receiver<()>,
    incs_bye_out: oneshot::Sender<()>,
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

        // set up the incoming streams listener
        let (istrms, incs_bye_in, incs_bye_out) = {
            let (client, incs_bye_in) = oneshot::channel();
            let (incs_bye_out, bye) = oneshot::channel();
            let inner = IncomingStreamsRef::new(client, bye, iuni);
            let driver = IncomingStreamsDriver(inner.clone());
            tokio::spawn(async move { driver.await });
            (IncomingStreams(inner), incs_bye_in, incs_bye_out)
        };

        // set up the control stream with the coordinator
        let ctrl = conn
            .accept_ctrl(config.id)
            .await
            .map_err(ClientError::AcceptCtrl)?;

        // let (sender, events) = mpsc::unbounded();
        Ok((
            Self {
                endpoint,
                coord: ClientChan {
                    conn,
                    ctrl,
                    incs_bye_in,
                    incs_bye_out,
                },
            },
            istrms,
        ))
    }
}
