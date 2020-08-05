use super::client::{Client, ClientConfig};
use super::coord::{Coord, CoordConfig};

use anyhow::Context;
use quinn::Certificate;
use std::{fs, io};
use std::path::PathBuf;
use std::time::Duration;
use tokio::{spawn, runtime, time};
use unwrap::unwrap;

#[test]
fn test_simple() {
    let (cert, cpath, kpath) = get_cert_and_paths();
    let mut rt = unwrap!(runtime::Builder::new().basic_scheduler().enable_all().build());
    println!("entering runtime");
    rt.block_on(async move {
        // start server
        println!("starting server");
        let mut coord = {
            let mut coord_cfg = CoordConfig::new(cpath, kpath).unwrap();
            //coord_cfg.enable_keylog();
            Coord::new(coord_cfg)
        }.await.unwrap();

        // start client
        println!("starting client");
        let jh = spawn(async move {
            let mut client = {
                let mut client_cfg = ClientConfig::new("client".to_string(), "localhost".to_string());
                client_cfg.set_ca(cert);
                Client::new(client_cfg)
            }.await.unwrap();
            time::delay_for(Duration::from_millis(20)).await;
        });

        // accept on coord
        println!("accepting control connection");
        coord.accept().await.unwrap();
        jh.await;
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
            let cert = rcgen::generate_simple_self_signed(vec!("localhost".into())).unwrap();
            let key = cert.serialize_private_key_der();
            let cert = cert.serialize_der().unwrap();
            fs::create_dir_all(&path).context("failed to create cert dir");
            fs::write(&cert_path, &cert).context("failed to write cert");
            fs::write(&key_path, &key).context("failed to write key");
            cert
        },
        Ok(cert) => cert,
        _ => panic!("could not stat file {:?}", cert_path),
    };
    (Certificate::from_der(&cert).unwrap(), cert_path.to_path_buf(), key_path.to_path_buf())
}
