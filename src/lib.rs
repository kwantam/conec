// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.
#![deny(missing_docs)]

/*!
COordinated NEtwork Channels: a network abstraction for communication
among many Clients, facilitated by one Coordinator.

Clients are assumed to know (e.g., by configuration or service discovery)
the hostname and port number of Coordinator. Coordinator is assumed to have
a TLS certificate for this hostname issued by a CA that Clients trust.

The basic abstraction is a channel, which connects two entities
(Client or Coordinator). A channel comprises one bi-directional
control stream and zero or more unidirectional data streams.
Every Client shares a channel with Coordinator: at startup, Client
connects to Coordinator.

A data stream accepts a sequence of messages from its writer. The data
stream's reader receives these messages in order. The stream handles
all message framing: a read yields a full message or nothing. There
is no support for out-of-order reads; use multiple data streams instead.

In this version of conec, the only supported data streams are
*proxied streams*: Client sends data to Coordinator, who forwards to
another Client. (In a future version, Coordinator will assist Clients
in constructing Client-to-Client channels, including NAT traversal.)

# Quickstart

A conec instance requires a Coordinator with a TLS certificate for its
hostname and an IP address that Clients can reach. It is possible to use
a self-signed certificate (generated, say, by [rcgen](https://docs.rs/rcgen/))
as long as the Clients trust it; see [ClientConfig::set_ca].

## Coordinator

To start a Coordinator, first build a [CoordConfig]. For example,

```ignore
let mut coord_cfg = CoordConfig::new(cert_path, key_path).unwrap();
coord_cfg.enable_stateless_retry();
coord_cfg.set_port(1337);
```

Next, pass this configuration to the [Coord] constructor, which returns
a future that, when forced, starts up the Coordinator.

```ignore
let coord = Coord::new(coord_cfg).await.unwrap();
```

The Coord constructor launches driver threads in the background.
These threads will run until the Coord struct is dropped and all
clients have disconnected.

## Client

To start a Client, first build a [ClientConfig]. For example,

```
# use conec::ClientConfig;
let mut client_cfg =
    ClientConfig::new("itme".to_string(), "coord.conec.example.com".to_string());
client_cfg.set_port(1337);
```

Next, pass this configuration to the [Client] constructor, which
is a future that returns the Client plus an [IncomingStreams](client::IncomingStreams) object
when forced:

```ignore
let (client, istreams) = Client::new(client_cfg).await.unwrap();
```

# Streams

Once your Client has connected to its Coordinator, it can set up
data streams with other Clients and send data on them:

```ignore
let to_you = client
    .new_stream("ityou".to_string(), 0)
    .unwrap()
    .await
    .unwrap();
to_you.send(Bytes::from("hi there")).await.unwrap();
```

The receiving client first accepts the stream and then reads data from it:

```ignore
let (peer, strmid, mut from_me) = istreams
    .next()
    .await
    .unwrap()
    .await
    .unwrap();
println!("Got new stream with id {} from peer {}", strmid, peer);
let rec = from_me
    .try_next()
    .await?
    .unwrap();
```
*/

pub mod client;
pub mod coord;
mod types;
mod util;

#[cfg(test)]
mod tests;

pub use client::{config::ClientConfig, Client};
pub use coord::{config::CoordConfig, Coord};
pub use types::{InStream, OutStream};

mod consts {
    pub(crate) const DFLT_PORT: u16 = 1719;
    pub(crate) const ALPN_CONEC: &[&[u8]] = &[b"conec"];
    pub(crate) const MAX_LOOPS: usize = 8;
    pub(crate) const VERSION: &str = "CONEC_V0.0.4::";
}
