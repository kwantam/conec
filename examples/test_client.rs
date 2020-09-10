// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use conec::{Client, ClientConfig};
use futures::{future, prelude::*};
use std::{env::args, path::PathBuf};
use tokio::io::AsyncBufReadExt;
use tokio_serde::{formats::SymmetricalBincode, SymmetricallyFramed};

fn get_cert_path() -> PathBuf {
    let dir = directories_next::ProjectDirs::from("am.kwant", "conec", "conec-tests").unwrap();
    let path = dir.data_local_dir();
    path.join("cert.der")
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

    let cpath = get_cert_path();
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
