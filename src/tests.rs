use super::client::{Client, ClientConfig};
use super::coord::{Coord, CoordConfig};

use anyhow::Context;
use quinn::Certificate;
use std::path::PathBuf;
use std::time::Duration;
use std::{fs, io};
use tokio::{runtime, time};
use unwrap::unwrap;

#[test]
fn test_simple() {
    let (cert, cpath, kpath) = get_cert_and_paths();
    let mut rt = unwrap!(runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build());
    println!("entering runtime");
    rt.block_on(async move {
        // start server
        println!("starting server");
        let coord = {
            let coord_cfg = CoordConfig::new(cpath, kpath).unwrap();
            Coord::new(coord_cfg).await.unwrap()
        };

        // start client
        println!("starting client");
        let mut client_cfg = ClientConfig::new("client".to_string(), "localhost".to_string());
        client_cfg.set_ca(cert);
        let client = Client::new(client_cfg.clone()).await.unwrap();

        time::delay_for(Duration::from_millis(20)).await;
        println!("clients connected: {}", coord.num_clients());
        time::delay_for(Duration::from_millis(20)).await;
        println!("clients connected: {}", coord.num_clients());
        drop(client);
        time::delay_for(Duration::from_millis(20)).await;
        println!("clients connected: {}", coord.num_clients());

        // should generate a failure (eventually)
        let client = Client::new(client_cfg).await.unwrap();

        time::delay_for(Duration::from_millis(20)).await;
        println!("clients connected: {}", coord.num_clients());
        time::delay_for(Duration::from_millis(20)).await;
        println!("clients connected: {}", coord.num_clients());
        drop(client);
        time::delay_for(Duration::from_millis(20)).await;
        println!("clients connected: {}", coord.num_clients());
    });
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
    (
        Certificate::from_der(&cert).unwrap(),
        cert_path.to_path_buf(),
        key_path.to_path_buf(),
    )
}
