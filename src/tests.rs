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
    client::StreamId,
    Client, ClientConfig, Coord, CoordConfig, NonblockingInStream, NonblockingInStreamError,
    TaggedBroadcastInStream, TaggedDeserializer, TaglessBroadcastInStream,
};

use anyhow::Context;
use bytes::Bytes;
use futures::{future, prelude::*, stream};
use quinn::{Certificate, CertificateChain, PrivateKey};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;
use std::{fs, io};
use tokio::{runtime, time};
use tokio_serde::{formats::SymmetricalBincode, SymmetricallyFramed};

#[test]
fn simple() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new().basic_scheduler().enable_all().build().unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
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

        assert_eq!(coord.num_clients(), 1);
        drop(client);
        time::delay_for(Duration::from_millis(20)).await;
        assert_eq!(coord.num_clients(), 0);
    });
}

#[test]
fn repeat_name() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new().basic_scheduler().enable_all().build().unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
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

        assert_eq!(coord.num_clients(), 1);

        // start another client with the same name --- should fail
        assert!(Client::new(client_cfg.clone()).await.is_err());
        assert_eq!(coord.num_clients(), 1);
        drop(client);
        time::delay_for(Duration::from_millis(20)).await;
        assert_eq!(coord.num_clients(), 0);

        // start another client with the old name --- should generate no error
        let client = Client::new(client_cfg).await.unwrap();
        assert_eq!(coord.num_clients(), 1);
        drop(client);
        time::delay_for(Duration::from_millis(20)).await;
        assert_eq!(coord.num_clients(), 0);
    });
}

#[test]
fn stream_uni() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new().basic_scheduler().enable_all().build().unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
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

        assert_eq!(coord.num_clients(), 2);

        // open stream to client2
        let (mut s12, _r21) = client1.new_proxied_stream("client2".to_string()).await.unwrap();
        // receive stream at client2
        let (_, _, _, mut r12) = inc2.next().await.unwrap();

        let to_send = Bytes::from("test stream");
        s12.send(to_send.clone()).await.unwrap();
        let rec = r12.try_next().await?.unwrap();
        assert_eq!(to_send, rec);

        // try to open new streams --- should fail because of sid reuse
        client1
            .new_stream_with_id("client1".to_string(), StreamId::Proxied(1u64 << 63))
            .await
            .map(|_| ())
            .expect_err("sid reuse should cause failure");

        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn stream_bi() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new().basic_scheduler().enable_all().build().unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
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

        assert_eq!(coord.num_clients(), 2);

        // open stream to client2
        let (mut s12, mut r21) = client1.new_proxied_stream("client2".to_string()).await.unwrap();
        // receive stream at client2
        let (_, _, mut s21, mut r12) = inc2.next().await.unwrap();

        let to_send = Bytes::from("ping pong");
        s12.send(to_send.clone()).await.unwrap();
        let rec = r12.try_next().await?.unwrap().freeze();
        s21.send(rec).await.unwrap();
        let rec = r21.try_next().await?.unwrap();
        assert_eq!(to_send, rec);

        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn new_stream() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new().basic_scheduler().enable_all().build().unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
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

        // start client 3
        let (_client3, mut inc3) = {
            let mut client_cfg = ClientConfig::new("client3".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            client_cfg.disable_listen();
            Client::new(client_cfg.clone()).await.unwrap()
        };

        assert_eq!(coord.num_clients(), 3);

        // open stream to client2
        let (mut s12, mut r21) = client1.new_stream("client2".to_string()).await.unwrap();
        let (_s12, _r21) = client1.new_stream("client2".to_string()).await.unwrap();
        let (clt2, sid2, mut s21, mut r12) = inc2.next().await.unwrap();
        let (_, sid2x, _s21, _r12) = inc2.next().await.unwrap();
        assert_eq!(&clt2, "client1");
        assert!(sid2.is_direct());
        assert!(sid2x.is_direct());
        let to_send = Bytes::from("ping pong");
        s12.send(to_send.clone()).await.unwrap();
        let rec = r12.try_next().await?.unwrap().freeze();
        s21.send(rec).await.unwrap();
        let rec = r21.try_next().await?.unwrap();
        assert_eq!(to_send, rec);

        // open stream to client3
        let (mut s13, mut r31) = client1.new_stream("client3".to_string()).await.unwrap();
        let (clt3, sid3, mut s31, mut r13) = inc3.next().await.unwrap();
        assert_eq!(&clt3, "client1");
        assert!(sid3.is_proxied());
        let to_send = Bytes::from("ping pong");
        s13.send(to_send.clone()).await.unwrap();
        let rec = r13.try_next().await?.unwrap().freeze();
        s31.send(rec).await.unwrap();
        let rec = r31.try_next().await?.unwrap();
        assert_eq!(to_send, rec);

        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn stream_block_nonblock() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new().basic_scheduler().enable_all().build().unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
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

        assert_eq!(coord.num_clients(), 2);

        const BCAST_QUEUE: usize = 16;

        // open stream to client2, make input nonblocking
        let (mut s12, r21) = client1.new_proxied_stream("client2".to_string()).await.unwrap();
        let mut r21 = NonblockingInStream::new(r21, BCAST_QUEUE);
        // receive stream at client2
        let (_, _, mut s21, mut r12) = inc2.next().await.unwrap();

        // send from client1 to client2 (blocking direction)
        let to_send = Bytes::copy_from_slice(&vec![0; 16384][..]);
        let mut count = 0;
        loop {
            let timeout = time::delay_for(Duration::from_millis(100));
            let send = s12.send(to_send.clone());
            match future::select(timeout, send).await {
                future::Either::Left(_) => break,
                future::Either::Right((r, _)) => r.unwrap(),
            };
            count += 1;
            assert!(count < 1048576);
        }
        let count = count;
        for _ in 0..count {
            let rec = r12.try_next().await?.unwrap();
            assert_eq!(rec, to_send);
        }
        {
            let timeout = time::delay_for(Duration::from_millis(100));
            let recv = r12.try_next();
            match future::select(timeout, recv).await {
                future::Either::Left(_) => (),
                future::Either::Right(_) => panic!("received more than we sent"),
            };
        }

        // send from client2 to client1 (nonblocking direction)
        for _ in 0..count + BCAST_QUEUE {
            s21.send(to_send.clone()).await.unwrap();
        }
        // close send side
        drop(s21);

        // this is a race, but since everything is local it is probably fine
        time::delay_for(Duration::from_millis(100)).await;
        match r21.try_next().await {
            Err(NonblockingInStreamError::Lagged(num)) => assert_eq!(num, count),
            _ => panic!("expected lagged error"),
        };
        for _ in 0..BCAST_QUEUE {
            let rec = r21.try_next().await?.unwrap();
            assert_eq!(rec, to_send);
        }
        assert!(r21.try_next().await?.is_none());

        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn stream_bi_multi() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new().basic_scheduler().enable_all().build().unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
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

        assert_eq!(coord.num_clients(), 2);

        let mut streams = Vec::with_capacity(4);
        #[allow(clippy::same_item_push)] // suppress false positive
        for _ in 0..4usize {
            let (s12, r21) = client1.new_proxied_stream("client2".to_string()).await.unwrap();
            let (sender, _, s21, r12) = inc2.next().await.unwrap();
            assert_eq!(sender, "client1".to_string());
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
fn stream_loopback() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new().basic_scheduler().enable_all().build().unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
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

        assert_eq!(coord.num_clients(), 1);

        // open stream to client
        let (mut s11, mut r11x) = client.new_proxied_stream("client1".to_string()).await.unwrap();
        // receive stream at client
        let (_, _, mut s11x, mut r11) = inc.next().await.unwrap();

        let to_send = Bytes::from("loopback stream");
        s11.send(to_send.clone()).await.unwrap();
        let rec = r11.try_next().await?.unwrap().freeze();
        s11x.send(rec).await.unwrap();
        let rec = r11x.try_next().await?.unwrap();
        assert_eq!(to_send, rec);

        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn broadcast_loopback() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new().basic_scheduler().enable_all().build().unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
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

        assert_eq!(coord.num_clients(), 1);

        // open broadcast stream
        let (mut s11, r11) = client.new_broadcast("broadcast_chan".to_string()).await.unwrap();
        let mut r11 = TaggedBroadcastInStream::new(r11);

        let to_send = Bytes::from("loopback broadcast");
        s11.send(to_send.clone()).await.unwrap();
        let rec = r11.try_next().await?.unwrap();
        assert_eq!("client1", &rec.0);
        assert_eq!(to_send, rec.1);
        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn broadcast_bidi() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new().basic_scheduler().enable_all().build().unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
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
        let (mut client2, _inc2) = {
            let mut client_cfg = ClientConfig::new("client2".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        assert_eq!(coord.num_clients(), 2);

        // open broadcast streams
        let (mut s1, r1) = client1.new_broadcast("broadcast_chan".to_string()).await.unwrap();
        let mut r1 = TaggedBroadcastInStream::new(r1);
        let (mut s2, r2) = client2.new_broadcast("broadcast_chan".to_string()).await.unwrap();
        let mut r2 = TaglessBroadcastInStream::new(r2);

        let to_send = Bytes::from("loopback broadcast");
        s1.send(to_send.clone()).await.unwrap();
        let rec1 = r1.try_next().await?.unwrap();
        let rec2 = r2.try_next().await?.unwrap().freeze();
        assert_eq!("client1", &rec1.0);
        assert_eq!(to_send, rec1.1);
        assert_eq!(to_send, rec2);
        s2.send(rec2).await.unwrap();
        let rec1 = r1.try_next().await?.unwrap();
        let rec2 = r2.try_next().await?.unwrap();
        assert_eq!("client2", &rec1.0);
        assert_eq!(to_send, rec1.1);
        assert_eq!(to_send, rec2);

        assert_eq!(coord.num_broadcasts(), 1);

        drop(r2);
        drop(s2);
        let mut r1 = TaglessBroadcastInStream::new(r1.into_inner());
        assert_eq!(coord.num_broadcasts(), 1);
        s1.send(to_send.clone()).await.unwrap();
        let rec1 = r1.try_next().await?.unwrap().freeze();
        assert_eq!(to_send, rec1);

        drop(r1);
        drop(s1);
        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_broadcasts(), 0);

        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn broadcast_codec() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new().basic_scheduler().enable_all().build().unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
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
        let (mut client2, _inc2) = {
            let mut client_cfg = ClientConfig::new("client2".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        assert_eq!(coord.num_clients(), 2);

        #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
        enum TestValues {
            ValueOne,
            ValueTwo,
        }

        // open broadcast streams and wrap in codec
        let (s1, r1) = client1.new_broadcast("broadcast_chan".to_string()).await.unwrap();
        let mut s1 = SymmetricallyFramed::new(s1, SymmetricalBincode::<TestValues>::default());
        let r1 = NonblockingInStream::new(r1, 16);
        let r1 = TaglessBroadcastInStream::new(r1);
        let mut r1 = SymmetricallyFramed::new(r1, SymmetricalBincode::<TestValues>::default());

        let (s2, r2) = client2.new_broadcast("broadcast_chan".to_string()).await.unwrap();
        let mut s2 = SymmetricallyFramed::new(s2, SymmetricalBincode::<TestValues>::default());
        let r2 = TaggedBroadcastInStream::new(r2);
        let mut r2 = TaggedDeserializer::new(r2, SymmetricalBincode::<TestValues>::default());

        s1.send(TestValues::ValueOne).await.unwrap();
        let rec1 = r1.try_next().await?.unwrap();
        let rec2 = r2.try_next().await?.unwrap();
        assert_eq!(rec1, TestValues::ValueOne);
        assert_eq!(&rec2.0, "client1");
        assert_eq!(rec2.1, TestValues::ValueOne);

        s2.send(TestValues::ValueTwo).await.unwrap();
        let rec1 = r1.try_next().await?.unwrap();
        let rec2 = r2.try_next().await?.unwrap();
        assert_eq!(rec1, TestValues::ValueTwo);
        assert_eq!(&rec2.0, "client2");
        assert_eq!(rec2.1, TestValues::ValueTwo);

        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn broadcast_sender_close() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new().basic_scheduler().enable_all().build().unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
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
        let (mut client2, _inc2) = {
            let mut client_cfg = ClientConfig::new("client2".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        assert_eq!(coord.num_clients(), 2);

        // open broadcast streams
        let (mut s1, r1) = client1.new_broadcast("broadcast_chan".to_string()).await.unwrap();
        let mut r1 = TaglessBroadcastInStream::new(r1);
        let (mut s2, r2) = client2.new_broadcast("broadcast_chan".to_string()).await.unwrap();
        let mut r2 = TaglessBroadcastInStream::new(r2);

        let to_send = Bytes::from("loopback broadcast");
        s1.send(to_send.clone()).await.unwrap();
        let rec1 = r1.try_next().await?.unwrap().freeze();
        let rec2 = r2.try_next().await?.unwrap().freeze();
        assert_eq!(to_send, rec1);
        assert_eq!(to_send, rec2);
        s2.send(rec1).await.unwrap();
        let rec1 = r1.try_next().await?.unwrap().freeze();
        let rec2 = r2.try_next().await?.unwrap().freeze();
        assert_eq!(to_send, rec1);
        assert_eq!(to_send, rec2);

        assert_eq!(coord.num_broadcasts(), 1);

        drop(s1);
        drop(s2);
        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_broadcasts(), 0);
        assert!(r1.try_next().await?.is_none());
        assert!(r2.try_next().await?.is_none());

        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn broadcast_receiver_close() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new().basic_scheduler().enable_all().build().unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
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
        let (mut client2, _inc2) = {
            let mut client_cfg = ClientConfig::new("client2".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        assert_eq!(coord.num_clients(), 2);

        // open broadcast streams
        let (mut s1, r1) = client1.new_broadcast("broadcast_chan".to_string()).await.unwrap();
        let mut r1 = TaglessBroadcastInStream::new(r1);
        let (mut s2, r2) = client2.new_broadcast("broadcast_chan".to_string()).await.unwrap();
        let mut r2 = TaglessBroadcastInStream::new(r2);

        let to_send = Bytes::from("loopback broadcast");
        s1.send(to_send.clone()).await.unwrap();
        let rec1 = r1.try_next().await?.unwrap().freeze();
        let rec2 = r2.try_next().await?.unwrap().freeze();
        assert_eq!(to_send, rec1);
        assert_eq!(to_send, rec2);
        s2.send(rec1).await.unwrap();
        let rec1 = r1.try_next().await?.unwrap().freeze();
        let rec2 = r2.try_next().await?.unwrap().freeze();
        assert_eq!(to_send, rec1);
        assert_eq!(to_send, rec2);

        assert_eq!(coord.num_broadcasts(), 1);

        drop(r1);
        drop(r2);
        assert_eq!(coord.num_broadcasts(), 1);

        s1.send(to_send.clone()).await.unwrap();
        s2.send(to_send.clone()).await.unwrap();
        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_broadcasts(), 1);

        drop(s1);
        drop(s2);
        time::delay_for(Duration::from_millis(40)).await;
        assert_eq!(coord.num_broadcasts(), 0);

        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn broadcast_block_nonblock() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new().basic_scheduler().enable_all().build().unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
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
        let (mut client2, _inc2) = {
            let mut client_cfg = ClientConfig::new("client2".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        // start client 3
        let (mut client3, _inc3) = {
            let mut client_cfg = ClientConfig::new("client3".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        // start client 4
        let (mut client4, _inc4) = {
            let mut client_cfg = ClientConfig::new("client4".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };

        assert_eq!(coord.num_clients(), 4);

        // everyone connects to broadcast
        let (mut s1, r1) = client1.new_broadcast("broadcast_chan".to_string()).await.unwrap();
        let mut r1 = TaglessBroadcastInStream::new(r1);
        let (mut s2, r2) = client2.new_broadcast("broadcast_chan".to_string()).await.unwrap();
        let mut r2 = TaglessBroadcastInStream::new(r2);
        let (mut s3, r3) = client3.new_broadcast("broadcast_chan".to_string()).await.unwrap();
        let mut r3 = TaglessBroadcastInStream::new(r3);
        let (mut s4, r4) = client4.new_broadcast("broadcast_chan".to_string()).await.unwrap();
        let mut r4 = TaglessBroadcastInStream::new(r4);
        assert_eq!(coord.num_broadcasts(), 1);

        // send until broadcast blocks
        let to_send = Bytes::copy_from_slice(&vec![0; 16384][..]);
        let mut count = 0;
        loop {
            let timeout = time::delay_for(Duration::from_millis(10));
            let send = s1.send(to_send.clone());
            match future::select(timeout, send).await {
                future::Either::Left(_) => break,
                future::Either::Right((r, _)) => r.unwrap(),
            };
            count += 1;
            assert!(count < 1048576);
        }
        let count = count;
        for _ in 0..count {
            let rec1 = r1.try_next().await?.unwrap();
            let rec2 = r2.try_next().await?.unwrap();
            let rec3 = r3.try_next().await?.unwrap();
            let rec4 = r4.try_next().await?.unwrap();
            assert_eq!(rec1, to_send);
            assert_eq!(rec2, to_send);
            assert_eq!(rec3, to_send);
            assert_eq!(rec4, to_send);
        }

        // now send 4 * count copies all at once
        let handle = tokio::spawn(async move {
            let to_send = Bytes::copy_from_slice(&vec![0; 16384][..]);
            let mut to_send = stream::repeat(to_send).take(4 * count).map(Ok);
            s2.send_all(&mut to_send).await.ok();
            s2
        });
        // read from 3 of the 4 until timeout
        let mut count2 = 0;
        loop {
            let timeout = time::delay_for(Duration::from_millis(10));
            let recv = r1.try_next();
            match future::select(timeout, recv).await {
                future::Either::Left(_) => break,
                future::Either::Right((r, _)) => r.unwrap(),
            };
            let rec2 = r2.try_next().await?.unwrap();
            assert_eq!(rec2, to_send);
            let rec3 = r3.try_next().await?.unwrap();
            assert_eq!(rec3, to_send);
            count2 += 1;
            assert!(count < 1048576);
        }
        let count2 = count2;
        for _ in 0..count2 {
            let rec = r4.try_next().await?.unwrap();
            assert_eq!(rec, to_send);
        }
        // now read from all of them until we've received 4 * count total
        for _ in 0..(4 * count - count2) {
            for r in [&mut r4, &mut r3, &mut r2, &mut r1].iter_mut() {
                let rec = r.try_next().await?.unwrap();
                assert_eq!(rec, to_send);
            }
        }

        // same as above, except this time r4 drops before reading anything --- should unblock others
        let mut s2 = handle.await.unwrap();
        let handle = tokio::spawn(async move {
            let to_send = Bytes::copy_from_slice(&vec![0; 16384][..]);
            let mut to_send = stream::repeat(to_send).take(4 * count).map(Ok);
            s2.send_all(&mut to_send).await.ok();
            s2
        });
        // read from 3 of the 4 until timeout
        let mut count2 = 0;
        loop {
            let timeout = time::delay_for(Duration::from_millis(100));
            let recv = r1.try_next();
            match future::select(timeout, recv).await {
                future::Either::Left(_) => break,
                future::Either::Right((r, _)) => r.unwrap(),
            };
            let rec2 = r2.try_next().await?.unwrap();
            assert_eq!(rec2, to_send);
            let rec3 = r3.try_next().await?.unwrap();
            assert_eq!(rec3, to_send);
            count2 += 1;
            assert!(count < 1048576);
        }
        let count2 = count2;
        drop(r4);
        // now read from all of them until we've received 4 * count total
        for _ in 0..(4 * count - count2) {
            for r in [&mut r3, &mut r2, &mut r1].iter_mut() {
                let timeout = time::delay_for(Duration::from_millis(100));
                let recv = r.try_next(); //.await?.unwrap();
                let rec = match future::select(timeout, recv).await {
                    future::Either::Left(_) => panic!("unexpected timeout"),
                    future::Either::Right((r, _)) => r?.unwrap(),
                };
                assert_eq!(rec, to_send);
            }
        }

        // now use the nonblocking adapters for everything
        let mut r1 = TaglessBroadcastInStream::new(NonblockingInStream::new(r1.into_inner(), 4));
        let mut r2 = TaglessBroadcastInStream::new(NonblockingInStream::new(r2.into_inner(), 4));
        let mut r3 = TaglessBroadcastInStream::new(NonblockingInStream::new(r3.into_inner(), 4));
        for _ in 0..2 * count {
            s3.send(to_send.clone()).await.unwrap();
            s4.send(to_send.clone()).await.unwrap();
        }
        // end the stream --- nonblock adapters should also end
        drop(s1);
        handle.await.unwrap();
        drop(s3);
        drop(s4);
        loop {
            match r1.try_next().await {
                Err(NonblockingInStreamError::Lagged(_)) => (),
                Err(e) => panic!("{:?}", e),
                Ok(Some(rec)) => assert_eq!(rec, to_send),
                Ok(None) => break,
            }
        }
        loop {
            match r2.try_next().await {
                Err(NonblockingInStreamError::Lagged(_)) => (),
                Err(e) => panic!("{:?}", e),
                Ok(Some(rec)) => assert_eq!(rec, to_send),
                Ok(None) => break,
            }
        }
        loop {
            match r3.try_next().await {
                Err(NonblockingInStreamError::Lagged(_)) => (),
                Err(e) => panic!("{:?}", e),
                Ok(Some(rec)) => assert_eq!(rec, to_send),
                Ok(None) => break,
            }
        }

        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn channel_errors() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new().basic_scheduler().enable_all().build().unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
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

        assert_eq!(coord.num_clients(), 1);

        // open stream to Coord --- should work
        client
            .new_direct_stream("client1".to_string())
            .await
            .map(|_| ())
            .expect_err("should not be able to start direct stream until we have connected");
        client
            .new_channel("client1".to_string())
            .await
            .expect_err("should not be able to connect to self");
        client
            .close_channel("client1".to_string())
            .await
            .expect_err("should get err closing nonexistent channel");
        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn channel_simple() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new().basic_scheduler().enable_all().build().unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
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
        let (_client2, mut inc2) = {
            let mut client_cfg = ClientConfig::new("client2".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };
        assert_eq!(coord.num_clients(), 2);

        client
            .new_direct_stream("client2".to_string())
            .await
            .map(|_| ())
            .expect_err("should have needed to connect first");
        client.new_channel("client2".to_string()).await.unwrap();
        time::delay_for(Duration::from_millis(40)).await;
        client
            .new_channel("client2".to_string())
            .await
            .expect_err("duplicate channel should be rejected");
        time::delay_for(Duration::from_millis(40)).await;

        let (mut s12, mut r21) = client.new_direct_stream("client2".to_string()).await.unwrap();
        let (sender, strmid, mut s21, mut r12) = inc2.next().await.unwrap();
        assert_eq!(&sender, "client1");
        assert!(strmid.is_direct());

        let to_send = Bytes::from("ping pong");
        s12.send(to_send.clone()).await.unwrap();
        let rec = r12.try_next().await?.unwrap().freeze();
        s21.send(rec).await.unwrap();
        let rec = r21.try_next().await?.unwrap();
        assert_eq!(to_send, rec);

        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn channel_close() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new().basic_scheduler().enable_all().build().unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
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
        let (mut client2, mut inc2) = {
            let mut client_cfg = ClientConfig::new("client2".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };
        assert_eq!(coord.num_clients(), 2);

        client
            .new_direct_stream("client2".to_string())
            .await
            .map(|_| ())
            .expect_err("should have needed to connect first");
        client.new_channel("client2".to_string()).await.unwrap();
        client
            .new_channel("client2".to_string())
            .await
            .expect_err("duplicate channel should be rejected");

        let (mut s12, mut r21) = client.new_direct_stream("client2".to_string()).await.unwrap();
        let (sender, strmid, mut s21, mut r12) = inc2.next().await.unwrap();
        assert_eq!(&sender, "client1");
        assert!(strmid.is_direct());

        let to_send = Bytes::from("ping pong");
        s12.send(to_send.clone()).await.unwrap();
        let rec = r12.try_next().await?.unwrap().freeze();
        s21.send(rec).await.unwrap();
        let rec = r21.try_next().await?.unwrap();
        assert_eq!(to_send, rec);

        client.close_channel("client2".to_string()).await.unwrap();
        time::delay_for(Duration::from_millis(20)).await;
        s12.send(to_send.clone())
            .await
            .expect_err("closed channel should kill stream");
        r21.try_next().await.expect_err("close instream should also die");

        // this shouldn't be an error, but it is. I suspect a bug in quinn or rustls
        //client.new_channel("client2".to_string()).await.unwrap();

        client2.new_channel("client1".to_string()).await.unwrap();
        client2.close_channel("client1".to_string()).await.unwrap();

        // this also shouldn't be an error
        //client2.new_channel("client1".to_string()).await.unwrap();

        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn channel_oneway() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new().basic_scheduler().enable_all().build().unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
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
            client_cfg.disable_listen();
            Client::new(client_cfg.clone()).await.unwrap()
        };

        // start client 2
        let (mut client2, _inc2) = {
            let mut client_cfg = ClientConfig::new("client2".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            Client::new(client_cfg.clone()).await.unwrap()
        };
        assert_eq!(coord.num_clients(), 2);

        client2
            .new_channel("client1".to_string())
            .await
            .expect_err("client2 should not be able to connect, client1 is not listening");
        client.new_channel("client2".to_string()).await.unwrap();

        // once client1 connects, client2 can initiate a stream
        let (mut s12, mut r21) = client2.new_direct_stream("client1".to_string()).await.unwrap();
        let (sender, strmid, mut s21, mut r12) = inc.next().await.unwrap();
        assert_eq!(&sender, "client2");
        assert!(strmid.is_direct());

        let to_send = Bytes::from("ping pong");
        s12.send(to_send.clone()).await.unwrap();
        let rec = r12.try_next().await?.unwrap().freeze();
        s21.send(rec).await.unwrap();
        let rec = r21.try_next().await?.unwrap();
        assert_eq!(to_send, rec);

        // make sure that repeated sid use causes failure
        client2
            .new_stream_with_id("client2".to_string(), StreamId::Direct(1u64 << 63))
            .await
            .map(|_| ())
            .expect_err("reused sid should cause failure");

        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn client_cert() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new().basic_scheduler().enable_all().build().unwrap();

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
        let coord = {
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

        let (_client2, _inc2) = {
            let mut client_cfg = ClientConfig::new("client2".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            client_cfg.set_cert(c2cert, c2key2, key2_bytes);
            Client::new(client_cfg).await.unwrap()
        };

        assert_eq!(coord.num_clients(), 2);

        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn client_cert_channel() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new().basic_scheduler().enable_all().build().unwrap();

    // generate CA and client certificates
    let cacert = generate_ca().unwrap();
    let cacert_bytes = cacert.serialize_der().unwrap();
    let (cert1_bytes, key1_bytes) = generate_cert("client1".to_string(), &cacert).unwrap();
    let (cert2_bytes, key2_bytes) = generate_cert("client2".to_string(), &cacert).unwrap();
    let (cert3_bytes, key3_bytes) = generate_cert("client3".to_string(), &cacert).unwrap();
    let cacert = Certificate::from_der(cacert_bytes.as_ref()).unwrap();
    let c1cert = CertificateChain::from_certs(Certificate::from_der(cert1_bytes.as_ref()));
    let c1key = PrivateKey::from_der(key1_bytes.as_ref()).unwrap();
    let c2cert = CertificateChain::from_certs(Certificate::from_der(cert2_bytes.as_ref()));
    let c2key = PrivateKey::from_der(key2_bytes.as_ref()).unwrap();
    let c2key2 = PrivateKey::from_der(key2_bytes.as_ref()).unwrap();
    let c3cert = CertificateChain::from_certs(Certificate::from_der(cert3_bytes.as_ref()));
    let c3key = PrivateKey::from_der(key3_bytes.as_ref()).unwrap();

    rt.block_on(async move {
        // start server
        let coord = {
            let mut coord_cfg = CoordConfig::new_from_file(&cpath, &kpath).unwrap();
            coord_cfg.enable_stateless_retry();
            coord_cfg.set_port(0); // auto assign
            coord_cfg.set_client_ca(cacert.clone());
            Coord::new(coord_cfg).await.unwrap()
        };
        let port = coord.local_addr().port();

        // start client 1
        let (mut client, _inc) = {
            let mut client_cfg = ClientConfig::new("client1".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            client_cfg.set_cert(c1cert, c1key, key1_bytes);
            client_cfg.set_client_ca(cacert.clone());
            Client::new(client_cfg).await.unwrap()
        };

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

        let (_client2, mut inc2) = {
            let mut client_cfg = ClientConfig::new("client2".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            client_cfg.set_cert(c2cert, c2key2, key2_bytes);
            client_cfg.set_client_ca(cacert.clone());
            Client::new(client_cfg).await.unwrap()
        };
        assert_eq!(coord.num_clients(), 2);

        // client with wrong ca cert --- connections to and from this client should fail
        let (mut client3, _inc3) = {
            let mut client_cfg = ClientConfig::new("client3".to_string(), "localhost".to_string());
            client_cfg.set_ca_from_file(&cpath).unwrap();
            client_cfg.set_port(port);
            client_cfg.set_cert(c3cert, c3key, key3_bytes);
            Client::new(client_cfg).await.unwrap()
        };

        client
            .new_direct_stream("client2".to_string())
            .await
            .map(|_| ())
            .expect_err("should have needed to connect first");
        client.new_channel("client2".to_string()).await.unwrap();
        client
            .new_channel("client2".to_string())
            .await
            .expect_err("duplicate channel should be rejected");

        let (mut s12, mut r21) = client.new_direct_stream("client2".to_string()).await.unwrap();
        let (sender, strmid, mut s21, mut r12) = inc2.next().await.unwrap();
        assert_eq!(&sender, "client1");
        assert!(strmid.is_direct());

        let to_send = Bytes::from("ping pong");
        s12.send(to_send.clone()).await.unwrap();
        let rec = r12.try_next().await?.unwrap().freeze();
        s21.send(rec).await.unwrap();
        let rec = r21.try_next().await?.unwrap();
        assert_eq!(to_send, rec);

        client.close_channel("client2".to_string()).await.unwrap();
        time::delay_for(Duration::from_millis(20)).await;
        s12.send(to_send.clone())
            .await
            .expect_err("closed channel should kill stream");
        r21.try_next().await.expect_err("close instream should also die");

        // check that client3 connections fail in both directions
        client
            .new_channel("client3".to_string())
            .await
            .expect_err("bad ca cert, conneciton should have failed");
        client3
            .new_channel("client2".to_string())
            .await
            .expect_err("bad ca cert, connection should have failed");

        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn reject_client_cert() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new().basic_scheduler().enable_all().build().unwrap();

    // generate CA and client certificates
    let cacert = generate_ca().unwrap();
    let cacert = Certificate::from_der(cacert.serialize_der().unwrap().as_ref()).unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
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

        assert_eq!(coord.num_clients(), 0);

        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn keepalive() {
    let (cpath, kpath) = get_cert_paths();
    let mut rt = runtime::Builder::new().basic_scheduler().enable_all().build().unwrap();
    rt.block_on(async move {
        // start server
        let coord = {
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

        time::delay_for(Duration::new(12, 0)).await;
        assert_eq!(coord.num_clients(), 1);

        Ok(()) as Result<(), std::io::Error>
    })
    .ok();
}

#[test]
fn check_version() {
    let semver_req_version = &crate::consts::VERSION[7..];
    assert_eq!(crate::consts::VERSION, &format!("CONEC_V{}", semver_req_version));

    let req = semver::VersionReq::parse(&format!("~{}", semver_req_version)).unwrap();
    let ver = semver::Version::parse(env!("CARGO_PKG_VERSION")).unwrap();
    assert!(req.matches(&ver));
}

#[test]
fn bincode() {
    use bytes::{Buf, BufMut, BytesMut};
    use std::io::Cursor;
    use std::str::from_utf8;

    #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
    enum TestValues {
        ValueOne,
        ValueTwo,
    }

    // serialize a value, then pack a string onto the end of it with length as suffix
    let mut buf = BytesMut::with_capacity(1024);
    buf.put(bincode::serialize(&TestValues::ValueOne).unwrap().as_ref());
    let id = String::from("this is my identity");
    let id_buf = {
        let id_len = id.len();
        let mut tmp = id.into_bytes();
        tmp.put_u32(id_len as u32);
        tmp
    };
    buf.put(id_buf.as_ref());

    let buf = buf.freeze();
    let buf_len = buf.len();
    let id_len = (&buf[buf_len - 4..]).get_u32() as usize;
    assert!(buf_len >= id_len + 4);
    let id = String::from(from_utf8(&buf[buf_len - 4 - id_len..buf_len - 4]).unwrap());

    let mut cur = Cursor::new(buf.as_ref());
    println!("{:?}", bincode::deserialize_from::<_, TestValues>(&mut cur).unwrap());
    println!("{}", id);
    assert_eq!(cur.position() as usize + id_len + 4, buf_len);
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
            fs::create_dir_all(&path).context("failed to create cert dir").unwrap();
            fs::write(&cert_path, &cert).context("failed to write cert").unwrap();
            fs::write(&key_path, &key).context("failed to write key").unwrap();
        }
        Ok(_) => (),
        _ => panic!("could not stat file {:?}", cert_path),
    }
    (cert_path, key_path)
}
