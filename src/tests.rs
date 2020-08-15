// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

use crate::{Client, ClientConfig, Coord, CoordConfig};

use anyhow::Context;
use bytes::Bytes;
use futures::prelude::*;
use quinn::Certificate;
use ring::rand::SystemRandom;
use ring::signature::{UnparsedPublicKey, ECDSA_P256_SHA256_ASN1};
use std::path::PathBuf;
use std::time::Duration;
use std::{fs, io};
use tokio::{runtime, time};
use webpki::{
    trust_anchor_util::cert_der_as_trust_anchor, DNSNameRef, EndEntityCert, TLSServerTrustAnchors,
    ECDSA_P256_SHA256,
};
use x509_signature::{parse_certificate, SignatureScheme};

#[test]
fn test_simple() {
    let (cert, cpath, kpath) = get_cert_and_paths();
    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
            let mut coord_cfg = CoordConfig::new(cpath, kpath).unwrap();
            coord_cfg.enable_stateless_retry();
            coord_cfg.set_port(0); // auto assign
            Coord::new(coord_cfg).await.unwrap()
        };
        let port = coord.local_addr().port();

        // start client
        let client = {
            let mut client_cfg = ClientConfig::new("client".to_string(), "localhost".to_string());
            client_cfg.set_ca(cert);
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
    let (cert, cpath, kpath) = get_cert_and_paths();
    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
            let mut coord_cfg = CoordConfig::new(cpath, kpath).unwrap();
            coord_cfg.enable_stateless_retry();
            coord_cfg.set_port(0);
            Coord::new(coord_cfg).await.unwrap()
        };
        let port = coord.local_addr().port();

        // start client
        let client_cfg = {
            let mut tmp = ClientConfig::new("client".to_string(), "localhost".to_string());
            tmp.set_ca(cert).set_port(port);
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
    let (cert, cpath, kpath) = get_cert_and_paths();
    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
            let mut coord_cfg = CoordConfig::new(cpath, kpath).unwrap();
            coord_cfg.enable_stateless_retry();
            coord_cfg.set_port(0); // auto assign
            Coord::new(coord_cfg).await.unwrap()
        };
        let port = coord.local_addr().port();

        // start client 1
        let (mut client1, _inc1) = {
            let mut client_cfg = ClientConfig::new("client1".to_string(), "localhost".to_string());
            client_cfg.set_ca(cert.clone());
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        // start client 2
        let (_client2, mut inc2) = {
            let mut client_cfg = ClientConfig::new("client2".to_string(), "localhost".to_string());
            client_cfg.set_ca(cert);
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_clients(), 2);

        // open stream to client2
        let (mut s12, _r21) = client1
            .new_stream("client2".to_string())
            .unwrap()
            .await
            .unwrap();
        // receive stream at client2
        let (_sender, _strmid, _s21, mut r12) = inc2.next().await.unwrap().await.unwrap();

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
    let (cert, cpath, kpath) = get_cert_and_paths();
    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
            let mut coord_cfg = CoordConfig::new(cpath, kpath).unwrap();
            coord_cfg.enable_stateless_retry();
            coord_cfg.set_port(0); // auto assign
            Coord::new(coord_cfg).await.unwrap()
        };
        let port = coord.local_addr().port();

        // start client 1
        let (mut client1, _inc1) = {
            let mut client_cfg = ClientConfig::new("client1".to_string(), "localhost".to_string());
            client_cfg.set_ca(cert.clone());
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        // start client 2
        let (_client2, mut inc2) = {
            let mut client_cfg = ClientConfig::new("client2".to_string(), "localhost".to_string());
            client_cfg.set_ca(cert);
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_clients(), 2);

        // open stream to client2
        let (mut s12, mut r21) = client1
            .new_stream("client2".to_string())
            .unwrap()
            .await
            .unwrap();
        // receive stream at client2
        let (_sender, _strmid, mut s21, mut r12) = inc2.next().await.unwrap().await.unwrap();

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
    let (cert, cpath, kpath) = get_cert_and_paths();
    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
            let mut coord_cfg = CoordConfig::new(cpath, kpath).unwrap();
            coord_cfg.enable_stateless_retry();
            coord_cfg.set_port(0); // auto assign
            Coord::new(coord_cfg).await.unwrap()
        };
        let port = coord.local_addr().port();

        // start client 1
        let (mut client1, _inc1) = {
            let mut client_cfg = ClientConfig::new("client1".to_string(), "localhost".to_string());
            client_cfg.set_ca(cert.clone());
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        // start client 2
        let (_client2, mut inc2) = {
            let mut client_cfg = ClientConfig::new("client2".to_string(), "localhost".to_string());
            client_cfg.set_ca(cert);
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_clients(), 2);

        let mut streams = Vec::new();
        for _ in 0..4usize {
            let (s12, r21) = client1
                .new_stream("client2".to_string())
                .unwrap()
                .await
                .unwrap();
            let (sender, _strmid, s21, r12) = inc2.next().await.unwrap().await.unwrap();
            assert_eq!(&sender, "client1");
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
    let (cert, cpath, kpath) = get_cert_and_paths();
    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
            let mut coord_cfg = CoordConfig::new(cpath, kpath).unwrap();
            coord_cfg.enable_stateless_retry();
            coord_cfg.set_port(0); // auto assign
            Coord::new(coord_cfg).await.unwrap()
        };
        let port = coord.local_addr().port();

        // start client 1
        let (mut client, mut inc) = {
            let mut client_cfg = ClientConfig::new("client1".to_string(), "localhost".to_string());
            client_cfg.set_ca(cert.clone());
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_clients(), 1);

        // open stream to client
        let (mut s11, mut r11x) = client
            .new_stream("client1".to_string())
            .unwrap()
            .await
            .unwrap();
        // receive stream at client
        let (_sender, _strmid, mut s11x, mut r11) = inc.next().await.unwrap().await.unwrap();

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
fn test_client_signing() {
    let (cert, cpath, kpath) = get_cert_and_paths();
    let mut rt = runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
            let mut coord_cfg = CoordConfig::new(cpath, kpath).unwrap();
            coord_cfg.enable_stateless_retry();
            coord_cfg.set_port(0);
            Coord::new(coord_cfg).await.unwrap()
        };
        let port = coord.local_addr().port();

        // start client
        let (client, _inc) = {
            let mut client_cfg = ClientConfig::new("client1".to_string(), "localhost".to_string());
            client_cfg.set_ca(cert.clone());
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_clients(), 1);

        let cert = client._cert.clone();
        let cert_bytes = cert.as_der().to_vec();
        let one_cert = Certificate::from_der(&cert_bytes[..]).unwrap();
        assert_eq!(one_cert.as_der(), &cert_bytes[..]);
        let sig = {
            let rng = SystemRandom::new();
            client._key.sign(&rng, &cert_bytes).unwrap()
        };

        let pk = UnparsedPublicKey::new(&ECDSA_P256_SHA256_ASN1, &cert_bytes[..]);
        pk.verify(&cert_bytes, sig.as_ref())
            .expect_err("ring sig check unexpectedly succeeded");

        let x509 = parse_certificate(&cert_bytes[..]).unwrap();
        x509.check_tls13_signature(
            SignatureScheme::ECDSA_NISTP256_SHA256,
            &cert_bytes[..],
            sig.as_ref(),
        )
        .expect("x509 sig check unexpectedly failed");
        x509.check_self_issued()
            .expect("x509 self-sig check unexpectedly failed");

        let webp = EndEntityCert::from(&cert_bytes[..]).unwrap();
        let time = webpki::Time::try_from(std::time::SystemTime::now()).unwrap();
        let trust = cert_der_as_trust_anchor(&cert_bytes[..]).unwrap();
        webp.verify_is_valid_tls_server_cert(
            &[&ECDSA_P256_SHA256],
            &TLSServerTrustAnchors(&[trust]),
            &[],
            time,
        )
        .expect("invalid self-signed server cert");
        webp.verify_is_valid_for_dns_name(DNSNameRef::try_from_ascii_str("client1").unwrap())
            .expect("expected 'client1' hostname, got something else");
        webp.verify_signature(&ECDSA_P256_SHA256, &cert_bytes[..], sig.as_ref())
            .expect("webpki sig check unexpectedly failed");

        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

fn get_cert_and_paths() -> (Certificate, PathBuf, PathBuf) {
    let dir = directories_next::ProjectDirs::from("am.kwant", "conec", "conec-tests").unwrap();
    let path = dir.data_local_dir();
    let cert_path = path.join("cert.der");
    let key_path = path.join("key.der");
    let cert = match fs::read(&cert_path) {
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
            cert
        }
        Ok(cert) => cert,
        _ => panic!("could not stat file {:?}", cert_path),
    };
    (Certificate::from_der(&cert).unwrap(), cert_path, key_path)
}
