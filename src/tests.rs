use crate::{Client, ClientConfig, Coord, CoordConfig};

use anyhow::Context;
use bytes::Bytes;
use futures::prelude::*;
use quinn::Certificate;
use std::path::PathBuf;
use std::time::Duration;
use std::{fs, io};
use tokio::{runtime, time};

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
            coord_cfg.set_port(55110);
            Coord::new(coord_cfg).await.unwrap()
        };

        // start client
        let client = {
            let mut client_cfg = ClientConfig::new("client".to_string(), "localhost".to_string());
            client_cfg.set_ca(cert);
            client_cfg.set_port(55110);
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
            coord_cfg.set_port(55111);
            Coord::new(coord_cfg).await.unwrap()
        };

        // start client
        let client_cfg = {
            let mut tmp = ClientConfig::new("client".to_string(), "localhost".to_string());
            tmp.set_ca(cert);
            tmp.set_port(55111);
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
            coord_cfg.set_port(55112);
            Coord::new(coord_cfg).await.unwrap()
        };

        // start client 1
        let (client1, _inc1) = {
            let mut client_cfg = ClientConfig::new("client1".to_string(), "localhost".to_string());
            client_cfg.set_ca(cert.clone());
            client_cfg.set_port(55112);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        // start client 2
        let (_client2, mut inc2) = {
            let mut client_cfg = ClientConfig::new("client2".to_string(), "localhost".to_string());
            client_cfg.set_ca(cert);
            client_cfg.set_port(55112);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_clients(), 2);

        // open stream to client2
        let mut s12 = client1
            .new_stream("client2".to_string(), 0)
            .unwrap()
            .await
            .unwrap();
        // receive stream at client2
        let (sender, strmid, mut r12) = inc2.next().await.unwrap().await.unwrap();

        let to_send = Bytes::from("asdf");
        s12.send(to_send.clone()).await.unwrap();
        let rec = r12.try_next().await?.unwrap();
        assert_eq!(to_send, rec);
        println!(
            "{}:{} sent '{:?}' (expected: '{:?}')",
            sender, strmid, rec, to_send
        );
        Ok(()) as Result<(), std::io::Error>
    }).ok();
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
            coord_cfg.set_port(55113);
            Coord::new(coord_cfg).await.unwrap()
        };

        // start client 1
        let (client, mut inc) = {
            let mut client_cfg = ClientConfig::new("client1".to_string(), "localhost".to_string());
            client_cfg.set_ca(cert.clone());
            client_cfg.set_port(55113);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_clients(), 1);

        // open stream to client1
        let mut s11 = client
            .new_stream("client1".to_string(), 0)
            .unwrap()
            .await
            .unwrap();
        // receive stream at client1
        let (sender, strmid, mut r11) = inc.next().await.unwrap().await.unwrap();

        let to_send = Bytes::from("loopback stream");
        s11.send(to_send.clone()).await.unwrap();
        let rec = r11.try_next().await?.unwrap();
        assert_eq!(to_send, rec);
        println!(
            "{}:{} sent '{:?}' (expected: '{:?}')",
            sender, strmid, rec, to_send
        );
        Ok(()) as Result<(), std::io::Error>
    }).ok();
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
