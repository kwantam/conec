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
use std::path::PathBuf;
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

///! Errors when constructing a new coordinator configuration
#[derive(Debug, Error)]
pub enum CertReadError {
    ///! Failed to read certificate or key file
    #[error(display = "Reading certificate or key file: {:?}", _0)]
    ReadingCertOrKey(#[source] io::Error),
    ///! Failed to parse certificate or key
    #[error(display = "Parsing certificate or key: {:?}", _0)]
    ParsingCertOrKey(#[source] ParseError),
}

pub(crate) fn get_cert_and_key(
    cert_path: PathBuf,
    key_path: PathBuf,
) -> Result<(CertificateChain, PrivateKey), CertReadError> {
    let key = {
        let tmp = fs_read(&key_path)?;
        if key_path.extension().map_or(false, |x| x == "der") {
            PrivateKey::from_der(&tmp)
        } else {
            PrivateKey::from_pem(&tmp)
        }
    }?;
    let cert = {
        let tmp = fs_read(&cert_path)?;
        if cert_path.extension().map_or(false, |x| x == "der") {
            CertificateChain::from_certs(Certificate::from_der(&tmp))
        } else {
            CertificateChain::from_pem(&tmp)?
        }
    };
    Ok((cert, key))
}
