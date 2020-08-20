// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use rustls::{Certificate, ClientCertVerified, ClientCertVerifier, DistinguishedNames, ServerConfig, TLSError};
use std::sync::Arc;
use webpki::{
    trust_anchor_util::cert_der_as_trust_anchor, DNSName, EndEntityCert, TLSClientTrustAnchors,
    TLSServerTrustAnchors, ECDSA_P256_SHA256,
};

struct ConecClientCertVerifier(Option<Vec<u8>>);

impl ConecClientCertVerifier {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(client_ca: Option<Vec<u8>>) -> Arc<dyn ClientCertVerifier> {
        Arc::new(Self(client_ca))
    }
}

impl ClientCertVerifier for ConecClientCertVerifier {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self, _sni: Option<&DNSName>) -> Option<bool> {
        Some(true)
    }

    fn client_auth_root_subjects(&self, _sni: Option<&DNSName>) -> Option<DistinguishedNames> {
        Some(DistinguishedNames::new())
    }

    fn verify_client_cert(
        &self,
        presented_certs: &[Certificate],
        _sni: Option<&DNSName>,
    ) -> Result<ClientCertVerified, TLSError> {
        if presented_certs.len() != 1 {
            // not quite the right error...
            return Err(TLSError::NoCertificatesPresented);
        }
        let cert_der = &presented_certs[0].0;
        let trust_der = self.0.as_ref().unwrap_or(&presented_certs[0].0);
        let cert = EndEntityCert::from(cert_der).map_err(TLSError::WebPKIError)?;
        let time =
            webpki::Time::try_from(std::time::SystemTime::now()).map_err(|_| TLSError::FailedToGetCurrentTime)?;
        cert.verify_is_valid_tls_client_cert(
            &[&ECDSA_P256_SHA256],
            &TLSClientTrustAnchors(&[cert_der_as_trust_anchor(trust_der).map_err(TLSError::WebPKIError)?]),
            &[],
            time,
        )
        .map_err(TLSError::WebPKIError)?;
        cert.verify_is_valid_tls_server_cert(
            &[&ECDSA_P256_SHA256],
            &TLSServerTrustAnchors(&[cert_der_as_trust_anchor(trust_der).map_err(TLSError::WebPKIError)?]),
            &[],
            time,
        )
        .map_err(TLSError::WebPKIError)?;
        Ok(ClientCertVerified::assertion())
    }
}

pub(super) fn build_rustls_server_config(client_ca: Option<Vec<u8>>) -> Arc<ServerConfig> {
    Arc::new({
        let mut tls_cfg = ServerConfig::new(ConecClientCertVerifier::new(client_ca));
        tls_cfg.versions = vec![rustls::ProtocolVersion::TLSv1_3];
        tls_cfg.max_early_data_size = u32::max_value();
        tls_cfg
    })
}
