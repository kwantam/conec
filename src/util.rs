// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use crate::consts::MAX_LOOPS;
use crate::types::{ControlMsg, CtrlStream};

use err_derive::Error;
use futures::prelude::*;
use quinn::{Certificate, CertificateChain, ParseError, PrivateKey};
use std::collections::VecDeque;
use std::fs::read as fs_read;
use std::io;
use std::path::Path;
use std::task::{Context, Poll};

#[derive(Debug, Error)]
pub enum SinkError {
    #[error(display = "Sink ready polling: {:?}", _0)]
    Ready(#[error(source, no_from)] io::Error),
    #[error(display = "Sink flush polling: {:?}", _0)]
    Flush(#[error(source, no_from)] io::Error),
    #[error(display = "Sink start send: {:?}", _0)]
    Send(#[error(source, no_from)] io::Error),
}

pub(crate) fn drive_ctrl_send(
    cx: &mut Context,
    flushing: &mut bool,
    ctrl: &mut CtrlStream,
    to_send: &mut VecDeque<ControlMsg>,
) -> Result<bool, SinkError> {
    let mut sent = 0;
    let mut cont = false;
    loop {
        if to_send.is_empty() {
            break;
        }
        match ctrl.poll_ready_unpin(cx) {
            Poll::Pending => break,
            Poll::Ready(rdy) => rdy.map_err(SinkError::Ready),
        }?;
        ctrl.start_send_unpin(to_send.pop_front().unwrap())
            .map_err(SinkError::Send)?;
        sent += 1;
        if sent >= MAX_LOOPS {
            cont = !to_send.is_empty();
            break;
        }
    }
    *flushing = match ctrl.poll_flush_unpin(cx) {
        Poll::Pending => Ok(true),
        Poll::Ready(Ok(())) => Ok(false),
        Poll::Ready(Err(e)) => Err(SinkError::Flush(e)),
    }?;
    Ok(cont)
}

/// Errors when reading in a certificate
#[derive(Debug, Error)]
pub enum CertReadError {
    /// Failed to read certificate or key file
    #[error(display = "Reading certificate or key file: {:?}", _0)]
    ReadingCertOrKey(#[source] io::Error),
    /// Failed to parse certificate or key
    #[error(display = "Parsing certificate or key: {:?}", _0)]
    ParsingCertOrKey(#[source] ParseError),
    /// Certificate chain is incomplete
    #[error(display = "Certificate chain is incomplete")]
    CertificateChain,
}

pub(crate) fn get_cert(cert_path: &Path) -> Result<Certificate, CertReadError> {
    let tmp = fs_read(cert_path)?;
    if cert_path.extension().map_or(false, |x| x == "der") {
        Ok(Certificate::from_der(&tmp)?)
    } else {
        let chain = CertificateChain::from_pem(&tmp)?;
        let cert_bytes = chain.iter().next().ok_or(CertReadError::CertificateChain)?.0.clone();
        Ok(Certificate::from_der(cert_bytes.as_ref())?)
    }
}

pub(crate) fn get_cert_and_key(
    cert_path: &Path,
    key_path: &Path,
) -> Result<(CertificateChain, PrivateKey, Vec<u8>), CertReadError> {
    let (key, key_vec) = {
        let tmp = fs_read(key_path)?;
        if key_path.extension().map_or(false, |x| x == "der") {
            (PrivateKey::from_der(&tmp)?, tmp)
        } else {
            (PrivateKey::from_pem(&tmp)?, tmp)
        }
    };
    let cert = {
        let tmp = fs_read(cert_path)?;
        if cert_path.extension().map_or(false, |x| x == "der") {
            CertificateChain::from_certs(Certificate::from_der(&tmp))
        } else {
            CertificateChain::from_pem(&tmp)?
        }
    };
    Ok((cert, key, key_vec))
}

macro_rules! def_ref {
    ($i:ident, $r:tt) => ( def_ref!($i, $r, pub(super)); );
    ($i:ident, $r:tt, $v:vis) => {
        $v struct $r(Arc<Mutex<$i>>);

        impl std::ops::Deref for $r {
            type Target = Mutex<$i>;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl Clone for $r {
            fn clone(&self) -> Self {
                self.lock().unwrap().ref_count += 1;
                Self(self.0.clone())
            }
        }

        impl Drop for $r {
            fn drop(&mut self) {
                let mut inner = self.lock().unwrap();
                if let Some(x) = inner.ref_count.checked_sub(1) {
                    inner.ref_count = x;
                }
                if inner.ref_count == 0 {
                    if let Some(task) = inner.driver.take() {
                        task.wake();
                    }
                }
            }
        }
    };
}

macro_rules! def_driver {
    ($r:ident, $d:tt, $e:ty) => ( def_driver!(pub(super), $r; pub(super), $d; $e); );
    ($rv:vis, $r:ident; $dv:vis, $d:tt; $e:ty) => {
        #[must_use = "$r must be spawned!"]
        $dv struct $d($rv $r);
        impl Future for $d {
            type Output = Result<(), $e>;
            fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
                let mut inner = self.0.lock().unwrap();
                match &inner.driver {
                    Some(w) if w.will_wake(cx.waker()) => (),
                    _ => inner.driver = Some(cx.waker().clone()),
                };
                inner.run_driver(cx)?;
                if inner.ref_count == 0 {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            }
        }
    };
}

macro_rules! def_cs_future {
    ($f:tt, $h:tt, $ok:ty, $e:ty, $d:meta) => {
        def_cs_future!($f, pub(self), $h, pub(self), $ok, $e, $d);
    };
    ($f:tt, $vh:vis, $h:tt, $vf:vis, $ok:ty, $e:ty, $d:meta) => {
        $vh type $h = oneshot::Sender<Result<$ok, $e>>;
        #[$d]
        pub struct $f($vf oneshot::Receiver<Result<$ok, $e>>);
        def_flat_future!($f, $ok, $e);
    };
}

macro_rules! def_flat_future {
    ($f:ty, $ok:ty, $e:ty) => {
        def_flat_future!($f, $ok, $e, Canceled, 0);
    };
    ($f:ty, $ok:ty, $e:ty, $v:tt, $fld:tt) => {
        impl Future for $f {
            type Output = Result<$ok, $e>;
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
                match self.$fld.poll_unpin(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Err(e)) => Poll::Ready(Err(<$e>::$v(e))),
                    Poll::Ready(Ok(res)) => Poll::Ready(res),
                }
            }
        }
    };
}
