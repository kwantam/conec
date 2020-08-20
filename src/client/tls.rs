// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use rustls::{Certificate, ClientConfig, PrivateKey, TLSError};
use std::sync::Arc;

pub(super) fn build_rustls_client_config(cert: Vec<u8>, key: Vec<u8>) -> Result<Arc<ClientConfig>, TLSError> {
    Ok(Arc::new({
        let mut cfg = ClientConfig::new();
        cfg.versions = vec![rustls::ProtocolVersion::TLSv1_3];
        cfg.enable_early_data = true;
        match rustls_native_certs::load_native_certs() {
            Ok(x) => {
                cfg.root_store = x;
            }
            Err((Some(x), e)) => {
                cfg.root_store = x;
                tracing::warn!("couldn't load some default trust roots: {}", e);
            }
            Err((None, e)) => {
                tracing::warn!("couldn't load any default trust roots: {}", e);
            }
        }
        {
            cfg.ct_logs = Some(&ct_logs::LOGS);
        }
        cfg.set_single_client_cert(vec![Certificate(cert)], PrivateKey(key))?;
        cfg
    }))
}
