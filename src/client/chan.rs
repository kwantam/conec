use crate::types::{ConecConn, CtrlStream, OutStream};

use err_derive::Error;
use futures::channel::oneshot;
use std::sync::{Arc, Mutex};
use std::task::Waker;

pub type ConnectingOutStream = oneshot::Receiver<OutStream>;

#[derive(Debug, Error)]
pub enum ClientChanError {
    #[error(display = "other: {:?}", _0)]
    Other(#[source] std::io::Error),
}

pub(super) struct ClientChanInner {
    conn: ConecConn,
    ctrl: CtrlStream,
    incs_bye_in: oneshot::Receiver<()>,
    incs_bye_out: oneshot::Sender<()>,
    ref_count: usize,
    driver: Option<Waker>,
    driver_lost: bool,
}

pub(super) struct ClientChanRef(Arc<Mutex<ClientChanInner>>);

impl std::ops::Deref for ClientChanRef {
    type Target = Mutex<ClientChanInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Clone for ClientChanRef {
    fn clone(&self) -> Self {
        self.lock().unwrap().ref_count += 1;
        Self(self.0.clone())
    }
}

impl Drop for ClientChanRef {
    fn drop(&mut self) {
        let inner = &mut *self.0.lock().unwrap();
        if let Some(x) = inner.ref_count.checked_sub(1) {
            inner.ref_count = x;
            if x == 0 {
                if let Some(task) = inner.driver.take() {
                    task.wake();
                }
            }
        }
    }
}

impl ClientChanRef {
    pub(super) fn new(
        conn: ConecConn,
        ctrl: CtrlStream,
    ) -> (Self, oneshot::Sender<()>, oneshot::Receiver<()>) {
        let (i_client, incs_bye_in) = oneshot::channel();
        let (incs_bye_out, i_bye) = oneshot::channel();
        (
            Self(Arc::new(Mutex::new(ClientChanInner {
                conn,
                ctrl,
                incs_bye_in,
                incs_bye_out,
                ref_count: 0,
                driver: None,
                driver_lost: false,
            }))),
            i_client,
            i_bye,
        )
    }
}

pub(super) struct ClientChan(pub(super) ClientChanRef);

impl ClientChan {
    pub(super) fn new_stream(&self, to: String, cid: usize) -> ConnectingOutStream {
        oneshot::channel().1
    }
}
