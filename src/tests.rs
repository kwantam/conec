// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use crate::{
    ca::{generate_ca, generate_cert},
    Client, ClientConfig, Coord, CoordConfig,
};

use anyhow::Context;
use bytes::Bytes;
use futures::prelude::*;
use quinn::{Certificate, CertificateChain, PrivateKey};
use std::path::PathBuf;
use std::time::Duration;
use std::{fs, io};
use tokio::{runtime, time};

#[test]
fn test_simple() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        // start server
        let (coord, _cinc) = {
            let mut coord_cfg = CoordConfig::new_from_file(&cpath, &kpath).unwrap();
            coord_cfg.enable_stateless_retry();
            coord_cfg.set_port(0); // auto assign
            Coord::new(coord_cfg).await.unwrap()
        };
        let port = coord.local_addr().port();

        // start client
        let client = {
            let mut client_cfg = ClientConfig::new("client".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        time::delay_for(Duration::from_millis(20)).await;
        assert_eq!(coord.num_clients(), 1);
        drop(client);
        time::delay_for(Duration::from_millis(20)).await;
        assert_eq!(coord.num_clients(), 0);
    });
}

#[test]
fn test_repeat_name() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        // start server
        let (coord, _cinc) = {
            let mut coord_cfg = CoordConfig::new_from_file(&cpath, &kpath).unwrap();
            coord_cfg.enable_stateless_retry();
            coord_cfg.set_port(0);
            Coord::new(coord_cfg).await.unwrap()
        };
        let port = coord.local_addr().port();

        // start client
        let client_cfg = {
            let mut tmp = ClientConfig::new("client".to_string(), "localhost".to_string());
            tmp.set_ca_from_file(&cpath).unwrap();
            tmp.set_port(port);
            tmp
        };
        let client = Client::new(client_cfg.clone()).await.unwrap();

        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_clients(), 1);

        // start another client with the same name --- should fail
        assert!(Client::new(client_cfg.clone()).await.is_err());
        assert_eq!(coord.num_clients(), 1);
        drop(client);
        time::delay_for(Duration::from_millis(20)).await;
        assert_eq!(coord.num_clients(), 0);

        // start another client with the old name --- should generate no error
        let client = Client::new(client_cfg).await.unwrap();
        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_clients(), 1);
        drop(client);
        time::delay_for(Duration::from_millis(20)).await;
        assert_eq!(coord.num_clients(), 0);
    });
}

#[test]
fn test_stream_uni() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        // start server
        let (coord, _cinc) = {
            let mut coord_cfg = CoordConfig::new_from_file(&cpath, &kpath).unwrap();
            coord_cfg.enable_stateless_retry();
            coord_cfg.set_port(0); // auto assign
            Coord::new(coord_cfg).await.unwrap()
        };
        let port = coord.local_addr().port();

        // start client 1
        let (mut client1, _inc1) = {
            let mut client_cfg = ClientConfig::new("client1".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        // start client 2
        let (_client2, mut inc2) = {
            let mut client_cfg = ClientConfig::new("client2".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_clients(), 2);

        // open stream to client2
        let (mut s12, _r21) = client1.new_stream("client2".to_string()).await.unwrap();
        // receive stream at client2
        let (_sender, _strmid, _s21, mut r12) = inc2.next().await.unwrap();

        let to_send = Bytes::from("test stream");
        s12.send(to_send.clone()).await.unwrap();
        let rec = r12.try_next().await?.unwrap();
        assert_eq!(to_send, rec);
        /*
        println!(
            "{}:{} sent '{:?}' (expected: '{:?}')",
            sender, strmid, rec, to_send
        );
        */
        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn test_stream_bi() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        // start server
        let (coord, _cinc) = {
            let mut coord_cfg = CoordConfig::new_from_file(&cpath, &kpath).unwrap();
            coord_cfg.enable_stateless_retry();
            coord_cfg.set_port(0); // auto assign
            Coord::new(coord_cfg).await.unwrap()
        };
        let port = coord.local_addr().port();

        // start client 1
        let (mut client1, _inc1) = {
            let mut client_cfg = ClientConfig::new("client1".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        // start client 2
        let (_client2, mut inc2) = {
            let mut client_cfg = ClientConfig::new("client2".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_clients(), 2);

        // open stream to client2
        let (mut s12, mut r21) = client1.new_stream("client2".to_string()).await.unwrap();
        // receive stream at client2
        let (_sender, _strmid, mut s21, mut r12) = inc2.next().await.unwrap();

        let to_send = Bytes::from("ping pong");
        s12.send(to_send.clone()).await.unwrap();
        let rec = r12.try_next().await?.unwrap().freeze();
        s21.send(rec).await.unwrap();
        let rec = r21.try_next().await?.unwrap();
        assert_eq!(to_send, rec);
        /*
        println!(
            "{}:{} sent '{:?}' (expected: '{:?}')",
            sender, strmid, rec, to_send
        );
        */
        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn test_stream_bi_multi() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        // start server
        let (coord, _cinc) = {
            let mut coord_cfg = CoordConfig::new_from_file(&cpath, &kpath).unwrap();
            coord_cfg.enable_stateless_retry();
            coord_cfg.set_port(0); // auto assign
            Coord::new(coord_cfg).await.unwrap()
        };
        let port = coord.local_addr().port();

        // start client 1
        let (mut client1, _inc1) = {
            let mut client_cfg = ClientConfig::new("client1".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        // start client 2
        let (_client2, mut inc2) = {
            let mut client_cfg = ClientConfig::new("client2".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_clients(), 2);

        let mut streams = Vec::new();
        for _ in 0..4usize {
            let (s12, r21) = client1.new_stream("client2".to_string()).await.unwrap();
            let (sender, _strmid, s21, r12) = inc2.next().await.unwrap();
            assert_eq!(sender, Some("client1".to_string()));
            streams.push((s12, r21, s21, r12));
        }
        let to_send = Bytes::from("ping pong");
        for (mut s12, mut r21, mut s21, mut r12) in streams.into_iter() {
            s12.send(to_send.clone()).await.unwrap();
            let rec = r12.try_next().await?.unwrap().freeze();
            s21.send(rec).await.unwrap();
            let rec = r21.try_next().await?.unwrap();
            assert_eq!(to_send, rec);
        }
        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn test_stream_loopback() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        // start server
        let (coord, _cinc) = {
            let mut coord_cfg = CoordConfig::new_from_file(&cpath, &kpath).unwrap();
            coord_cfg.enable_stateless_retry();
            coord_cfg.set_port(0); // auto assign
            Coord::new(coord_cfg).await.unwrap()
        };
        let port = coord.local_addr().port();

        // start client 1
        let (mut client, mut inc) = {
            let mut client_cfg = ClientConfig::new("client1".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_clients(), 1);

        // open stream to client
        let (mut s11, mut r11x) = client.new_stream("client1".to_string()).await.unwrap();
        // receive stream at client
        let (_sender, _strmid, mut s11x, mut r11) = inc.next().await.unwrap();

        let to_send = Bytes::from("loopback stream");
        s11.send(to_send.clone()).await.unwrap();
        let rec = r11.try_next().await?.unwrap().freeze();
        s11x.send(rec).await.unwrap();
        let rec = r11x.try_next().await?.unwrap();
        assert_eq!(to_send, rec);
        /*
        println!(
            "{}:{} sent '{:?}' (expected: '{:?}')",
            sender, strmid, rec, to_send
        );
        */
        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn test_stream_client_to_coord() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        // start server
        let (coord, mut cinc) = {
            let mut coord_cfg = CoordConfig::new_from_file(&cpath, &kpath).unwrap();
            coord_cfg.enable_stateless_retry();
            coord_cfg.set_port(0); // auto assign
            Coord::new(coord_cfg).await.unwrap()
        };
        let port = coord.local_addr().port();

        // start client 1
        let (mut client, _inc) = {
            let mut client_cfg = ClientConfig::new("client1".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_clients(), 1);

        // open stream to client
        let (mut s11, mut r11x) = client.new_stream(None).await.unwrap();
        // receive stream at coordinator
        let (sender, _strmid, mut s11x, mut r11) = cinc.next().await.unwrap();

        let to_send = Bytes::from("loopback stream");
        s11.send(to_send.clone()).await.unwrap();
        let rec = r11.try_next().await?.unwrap().freeze();
        s11x.send(rec).await.unwrap();
        let rec = r11x.try_next().await?.unwrap();
        assert_eq!(to_send, rec);
        assert_eq!("client1", &sender);

        // should error if we try to reuse a sid, even with a different target
        assert!(client
            .new_stream_with_sid("client1".to_string(), 1u32 << 31)
            .await
            .is_err());

        /*
        println!(
            "{}:{} sent '{:?}' (expected: '{:?}')",
            sender, strmid, rec, to_send
        );
        */
        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn test_stream_coord_to_client() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        // start server
        let (mut coord, _cinc) = {
            let mut coord_cfg = CoordConfig::new_from_file(&cpath, &kpath).unwrap();
            coord_cfg.enable_stateless_retry();
            coord_cfg.set_port(0); // auto assign
            Coord::new(coord_cfg).await.unwrap()
        };
        let port = coord.local_addr().port();

        // start client 1
        let (_client, mut inc) = {
            let mut client_cfg = ClientConfig::new("client1".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_clients(), 1);

        // open stream to client
        let (mut s11, mut r11x) = coord.new_stream("client1".to_string()).await.unwrap();
        // receive stream at coordinator
        let (sender, _strmid, mut s11x, mut r11) = inc.next().await.unwrap();

        let to_send = Bytes::from("loopback stream");
        s11.send(to_send.clone()).await.unwrap();
        let rec = r11.try_next().await?.unwrap().freeze();
        s11x.send(rec).await.unwrap();
        let rec = r11x.try_next().await?.unwrap();
        assert_eq!(to_send, rec);
        assert!(sender.is_none());

        // should error if we try to reuse a sid, even with a different target
        assert!(coord
            .new_stream_with_sid("client2".to_string(), 1u32 << 31)
            .await
            .is_err());

        /*
        println!(
            "{}:{} sent '{:?}' (expected: '{:?}')",
            sender, strmid, rec, to_send
        );
        */
        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn test_channel_loopback() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        // start server
        let (coord, _cinc) = {
            let mut coord_cfg = CoordConfig::new_from_file(&cpath, &kpath).unwrap();
            coord_cfg.enable_stateless_retry();
            coord_cfg.set_port(0); // auto assign
            Coord::new(coord_cfg).await.unwrap()
        };
        let port = coord.local_addr().port();

        // start client 1
        let (mut client, _inc) = {
            let mut client_cfg = ClientConfig::new("client1".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_clients(), 1);

        let (addr, cert) = client.new_channel("client1".to_string()).await.unwrap();
        println!("{:?} {:?}", addr, cert);

        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn test_channel_simple() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        // start server
        let (coord, _cinc) = {
            let mut coord_cfg = CoordConfig::new_from_file(&cpath, &kpath).unwrap();
            coord_cfg.enable_stateless_retry();
            coord_cfg.set_port(0); // auto assign
            Coord::new(coord_cfg).await.unwrap()
        };
        let port = coord.local_addr().port();

        // start client 1
        let (mut client, _inc) = {
            let mut client_cfg = ClientConfig::new("client1".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        // start client 2
        let (_client2, _inc2) = {
            let mut client_cfg = ClientConfig::new("client2".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_clients(), 2);

        let (addr, cert) = client.new_channel("client2".to_string()).await.unwrap();
        println!("{:?} {:?}", addr, cert);

        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn test_client_cert() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap();

    // generate CA and client certificates
    let cacert = generate_ca().unwrap();
    let cacert_bytes = cacert.serialize_der().unwrap();
    let (cert1_bytes, key1_bytes) = generate_cert("client1".to_string(), &cacert).unwrap();
    let (cert2_bytes, key2_bytes) = generate_cert("client2".to_string(), &cacert).unwrap();
    let cacert = Certificate::from_der(cacert_bytes.as_ref()).unwrap();
    let c1cert = CertificateChain::from_certs(Certificate::from_der(cert1_bytes.as_ref()));
    let c1key = PrivateKey::from_der(key1_bytes.as_ref()).unwrap();
    let c2cert = CertificateChain::from_certs(Certificate::from_der(cert2_bytes.as_ref()));
    let c2key = PrivateKey::from_der(key2_bytes.as_ref()).unwrap();
    let c2key2 = PrivateKey::from_der(key2_bytes.as_ref()).unwrap();

    rt.block_on(async move {
        // start server
        let (coord, _cinc) = {
            let mut coord_cfg = CoordConfig::new_from_file(&cpath, &kpath).unwrap();
            coord_cfg.enable_stateless_retry();
            coord_cfg.set_port(0); // auto assign
            coord_cfg.set_client_ca(cacert.clone());
            Coord::new(coord_cfg).await.unwrap()
        };
        let port = coord.local_addr().port();

        // start client 1
        let (_client, _inc) = {
            let mut client_cfg = ClientConfig::new("client1".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            client_cfg.set_cert(c1cert, c1key, key1_bytes);
            Client::new(client_cfg).await.unwrap()
        };

        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_clients(), 1);

        // start client with wrong name in cert
        {
            let mut client_cfg = ClientConfig::new("client3".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            client_cfg.set_cert(c2cert.clone(), c2key, key2_bytes.clone());
            Client::new(client_cfg)
                .await
                .map(|_| ())
                .expect_err("should have failed to connect with mismatched name");
        };

        // start client with wrong name in cert
        let (_client2, _inc2) = {
            let mut client_cfg = ClientConfig::new("client2".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            client_cfg.set_cert(c2cert, c2key2, key2_bytes);
            Client::new(client_cfg).await.unwrap()
        };

        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_clients(), 2);

        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn test_reject_client_cert() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap();

    // generate CA and client certificates
    let cacert = generate_ca().unwrap();
    let cacert = Certificate::from_der(cacert.serialize_der().unwrap().as_ref()).unwrap();
    rt.block_on(async move {
        // start server
        let (coord, _cinc) = {
            let mut coord_cfg = CoordConfig::new_from_file(&cpath, &kpath).unwrap();
            coord_cfg.enable_stateless_retry();
            coord_cfg.set_port(0); // auto assign
            coord_cfg.set_client_ca(cacert.clone());
            Coord::new(coord_cfg).await.unwrap()
        };
        let port = coord.local_addr().port();

        // try to start client --- should fail because self-signed cert should be rejected
        let mut client_cfg = ClientConfig::new("client1".to_string(), "localhost".to_string());
        client_cfg.set_ca_from_file(&cpath).unwrap();
        client_cfg.set_port(port);
        Client::new(client_cfg.clone())
            .await
            .map(|_| ())
            .expect_err("client should have failed to connect");

        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_clients(), 0);

        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
#[ignore]
fn test_keepalive() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        // start server
        let (coord, _cinc) = {
            let mut coord_cfg = CoordConfig::new_from_file(&cpath, &kpath).unwrap();
            coord_cfg.enable_stateless_retry();
            coord_cfg.set_port(0); // auto assign
            Coord::new(coord_cfg).await.unwrap()
        };
        let port = coord.local_addr().port();

        // start client 1
        let (_client, _inc) = {
            let mut client_cfg = ClientConfig::new("client1".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        time::delay_for(Duration::from_millis(11_000)).await;
        assert_eq!(coord.num_clients(), 1);

        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn check_version() {
    assert_eq!(
        crate::consts::VERSION,
        &format!("CONEC_V{}", env!("CARGO_PKG_VERSION"))
    );
}

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
            fs::create_dir_all(&path)
                .context("failed to create cert dir")
                .unwrap();
            fs::write(&cert_path, &cert)
                .context("failed to write cert")
                .unwrap();
            fs::write(&key_path, &key)
                .context("failed to write key")
                .unwrap();
        }
        Ok(_) => (),
        _ => panic!("could not stat file {:?}", cert_path),
    }
    (cert_path, key_path)
}
