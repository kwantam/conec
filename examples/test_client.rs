// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use anyhow::Context;
use conec::{Client, ClientConfig};
use futures::{future, prelude::*};
use std::{env::args, fs, io, path::PathBuf};
use tokio::io::AsyncBufReadExt;
use tokio_serde::{formats::SymmetricalBincode, SymmetricallyFramed};

fn get_cert_paths() -> (PathBuf, PathBuf) {
    let dir = directories_next::ProjectDirs::from("am.kwant", "conec", "conec-tests").unwrap();
    let path = dir.data_local_dir();
    let cert_path = path.join("cert.der");
    let key_path = path.join("key.der");
    match fs::read(&cert_path) {
        Err(ref e) if e.kind() == io::ErrorKind::NotFound => {
            println!("generating self-signed cert");
            let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
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
    let mut args: Vec<String> = args().skip(1).collect();
    if args.len() < 3 {
        println!("Usage: test_client <server> <id> <peer> [initiate]");
        std::process::exit(1);
    }
    let initiate = if args.len() > 3 {
        args.truncate(3);
        true
    } else {
        false
    };
    let peer = args.pop().unwrap();
    let id = args.pop().unwrap();
    let server = args.pop().unwrap();

    let (cpath, _) = get_cert_paths();
    let mut cfg = ClientConfig::new(id, server);
    cfg.set_ca_from_file(&cpath).unwrap();

    run_client(cfg, peer, initiate)
}

#[tokio::main]
async fn run_client(cfg: ClientConfig, peer: String, initiate: bool) {
    let (mut client, mut incoming) = Client::new(cfg).await.unwrap();

    let (send, recv) = if initiate {
        client.new_channel(peer.clone()).await.unwrap();
        client.new_direct_stream(peer).await.unwrap()
    } else {
        let (_, _, send, recv) = incoming.next().await.unwrap();
        (send, recv)
    };

    let rfut = SymmetricallyFramed::new(recv, SymmetricalBincode::<String>::default()).for_each(|s| {
        println!("---> {}", s.unwrap());
        future::ready(())
    });

    let stdin = tokio::io::BufReader::new(tokio::io::stdin());
    let sfut = stdin
        .lines()
        .forward(SymmetricallyFramed::new(send, SymmetricalBincode::<String>::default()));

    let (sf, _) = futures::future::join(sfut, rfut).await;
    sf.ok();
}
