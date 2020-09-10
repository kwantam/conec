// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use anyhow::Context;
use conec::{Coord, CoordConfig};
use std::{env::args, fs, io, path::PathBuf};

fn get_cert_paths(server_name: Option<String>) -> (PathBuf, PathBuf) {
    let dir = directories_next::ProjectDirs::from("am.kwant", "conec", "conec-tests").unwrap();
    let path = dir.data_local_dir();
    let cert_path = path.join("cert.der");
    let key_path = path.join("key.der");
    match fs::read(&cert_path) {
        Err(ref e) if e.kind() == io::ErrorKind::NotFound => {
            println!("generating self-signed cert");
            let hostnames = {
                let mut tmp: Vec<String> = server_name.into_iter().collect();
                tmp.push("localhost".into());
                tmp
            };
            let cert = rcgen::generate_simple_self_signed(hostnames).unwrap();
            let key = cert.serialize_private_key_der();
            let cert = cert.serialize_der().unwrap();
            fs::create_dir_all(&path).context("failed to create cert dir").unwrap();
            fs::write(&cert_path, &cert).context("failed to write cert").unwrap();
            fs::write(&key_path, &key).context("failed to write key").unwrap();
        }
        Ok(_) => (),
        _ => panic!("could not stat file {:?}", cert_path),
    }
    (cert_path, key_path)
}

fn main() {
    let server_name = args().nth(1);
    let (cpath, kpath) = get_cert_paths(server_name);
    let mut cfg = CoordConfig::new_from_file(&cpath, &kpath).unwrap();
    cfg.enable_stateless_retry();
    run_coord(cfg)
}

#[tokio::main]
async fn run_coord(cfg: CoordConfig) {
    Coord::new(cfg).await.unwrap().await.unwrap()
}
