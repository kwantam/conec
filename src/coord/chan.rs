use crate::coord::{CoordEvent};
use crate::types::{ConecConnection, CtrlStream};

use futures::channel::mpsc;
//use futures::prelude::*;
//use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task;

pub(super) struct CoordChanInner {
    conn: ConecConnection,
    ctrl: CtrlStream,
    peer: String,
    sender: mpsc::UnboundedSender<CoordEvent>,
    ref_count: usize,
    driver: Option<task::Waker>,
}

pub(super) struct CoordChanRef(Arc<Mutex<CoordChanInner>>);

impl std::ops::Deref for CoordChanRef {
    type Target = Mutex<CoordChanInner>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Clone for CoordChanRef {
    fn clone(&self) -> Self {
        self.lock().unwrap().ref_count += 1;
        Self(self.0.clone())
    }
}

impl Drop for CoordChanRef {
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

impl CoordChanRef {
    pub(super) fn new(
        conn: ConecConnection,
        ctrl: CtrlStream,
        peer: String,
        sender: mpsc::UnboundedSender<CoordEvent>,
    ) -> Self {
        Self(Arc::new(Mutex::new(CoordChanInner {
            conn,
            ctrl,
            peer,
            sender,
            ref_count: 0,
            driver: None,
        })))
    }
}

pub(super) struct CoordChan {
    pub(super) inner: CoordChanRef,
}
