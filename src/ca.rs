// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

/*!
This module defines convenience functions for generating certificates.

See `examples/gencerts.rs` and `src/tests.rs` for more information.
*/

use crate::consts::VERSION;

use err_derive::Error;
use rand::{thread_rng, Rng};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DistinguishedName, DnType, IsCa, RcgenError, SanType,
};

/// Errors during key generation
#[derive(Debug, Error)]
pub enum CertAuthError {
    /// Error during key generation
    #[error(display = "Error generating key")]
    KeyGen(#[source] RcgenError),
}
def_into_error!(CertAuthError);

/// Generate a new CA certificate for use with CONEC
pub fn generate_ca() -> Result<Certificate, CertAuthError> {
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    params.serial_number = Some(thread_rng().gen());
    params.distinguished_name = {
        let mut tmp = DistinguishedName::new();
        tmp.push(DnType::OrganizationName, VERSION);
        tmp.push(DnType::CommonName, "ca");
        tmp
    };
    params.subject_alt_names = vec![SanType::DnsName("ca".to_string())];
    Ok(Certificate::from_params(params)?)
}

/// Generate a client certificate signed by a given CA
pub fn generate_cert(name: String, ca: &Certificate) -> Result<(Vec<u8>, Vec<u8>), CertAuthError> {
    let mut params = CertificateParams::default();
    params.serial_number = Some(thread_rng().gen());
    params.distinguished_name = {
        let mut tmp = DistinguishedName::new();
        tmp.push(DnType::OrganizationName, VERSION);
        tmp.push(DnType::CommonName, &name);
        tmp
    };
    params.subject_alt_names = vec![SanType::DnsName(name)];

    let cert = Certificate::from_params(params)?;
    Ok((cert.serialize_der_with_signer(ca)?, cert.serialize_private_key_der()))
}
