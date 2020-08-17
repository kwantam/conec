// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use conec::ca::{generate_ca, generate_cert};

use rcgen::{Certificate, CertificateParams, KeyPair};
use std::{
    convert::TryFrom,
    fs,
    io::{self, BufRead},
    path,
};

fn main() {
    let cacert = get_ca();
    let stdin = io::stdin();
    for cert_name in stdin.lock().lines() {
        new_cert(cert_name.unwrap(), &cacert);
    }
}

fn get_ca() -> Certificate {
    let path = path::PathBuf::from("certs/");
    let cert_path = path.join("ca_cert.der");
    let key_path = path.join("ca_key.der");
    match (fs::read(&cert_path), fs::read(&key_path)) {
        (Ok(cert), Ok(key)) => {
            let key = KeyPair::try_from(key.as_ref()).unwrap();
            let params = CertificateParams::from_ca_cert_der(cert.as_ref(), key).unwrap();
            Certificate::from_params(params).unwrap()
        }
        _ => {
            println!("generating new CA cert and key");
            let cert = generate_ca().unwrap();
            fs::create_dir_all(&path).unwrap();
            fs::write(&cert_path, cert.serialize_der().unwrap()).unwrap();
            fs::write(&key_path, cert.serialize_private_key_der()).unwrap();
            cert
        }
    }
}

fn new_cert(name: String, ca: &Certificate) {
    let path = path::PathBuf::from("certs/");
    let cert_path = path.join(&format!("{}_cert.der", name));
    let key_path = path.join(&format!("{}_key.der", name));

    let (cert_bytes, key_bytes) = generate_cert(name, ca).unwrap();
    fs::create_dir_all(&path).unwrap();
    fs::write(&cert_path, &cert_bytes).unwrap();
    fs::write(&key_path, &key_bytes).unwrap();
}
