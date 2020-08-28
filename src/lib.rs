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

The basic abstraction is a channel, which connects two entities (Client
or Coordinator). Every Client shares a channel with Coordinator: at
startup, Client connects to Coordinator. Clients can also ask Coordinator
to help them open a channel directly to another Client. There is currently
no explicit support for NAT traversal, but because of the way connections
are done Clients behind certain NATs (in particular, "full-cone NATs")
should be able to make direct connections. A future version will add
support for explicit NAT holepunching.

A channel comprises one bi-directional control stream, which is used
internally to manage the channel, and zero or more bidirectional data streams.
A data stream accepts a sequence of messages from its writer. The data
stream's reader receives these messages in order. The stream handles
all message framing: a read yields a full message or nothing. There
is no support for out-of-order reads; use multiple data streams instead.

# Quickstart

A conec instance requires a Coordinator with a TLS certificate for its
hostname and an IP address that Clients can reach. It is possible to use
a self-signed certificate (generated, say, by [rcgen](https://docs.rs/rcgen/))
as long as the Clients trust it; see [ClientConfig::set_ca].

See `tests.rs` for more complete usage examples than the ones below.

## Coordinator

To start a Coordinator, first build a [CoordConfig]. For example,

```ignore
let mut coord_cfg = CoordConfig::new(cert_path, key_path).unwrap();
coord_cfg.enable_stateless_retry();
coord_cfg.set_port(1337);
```

Next, pass this configuration to the [Coord] constructor, which returns
a future that returns the Coordiator plus a [coord::IncomingStreams] object
when forced.

```ignore
let (coord, coord_istreams) = Coord::new(coord_cfg).await.unwrap();
coord.await
```

The Coord constructor launches driver threads in the background.
These threads will run until the Coord struct is dropped and all
clients have disconnected.

Coord is a future that returns only if an error occurs. It is *not*
necessary to await this future for the coordinator to run, but you
shouldn't drop the Coord object if you want the Coordinator to run!

## Client

To start a Client, first build a [ClientConfig]. For example,

```
# use conec::ClientConfig;
let mut client_cfg =
    ClientConfig::new("client1".to_string(), "coord.conec.example.com".to_string());
client_cfg.set_port(1337);
```

Next, pass this configuration to the [Client] constructor, which
is a future that returns the Client plus a [client::IncomingStreams] object
when forced:

```ignore
let (client, istreams) = Client::new(client_cfg).await.unwrap();
```

# Streams

Once your Client has connected to its Coordinator, it can set up
data streams with the Coordinator or other Clients, and send data on
those streams:

```ignore
let (mut to_client2, _from_client2) = client
    .new_stream("client2".to_string())
    .await
    .unwrap();
to_client2.send(Bytes::from("hi there")).await.unwrap();
```

The receiving client first accepts the stream from its
[client::IncomingStreams] and then reads data from it:

```ignore
let (peer, strmid, _to_client1, mut from_client1) = istreams
    .next()
    .await
    .unwrap();
println!("Got new stream with id {:?} from peer {}", strmid, peer);
let rec = from_client1
    .try_next()
    .await?
    .unwrap();
```

The first element of the returned 4-tuple will be `Some(<name>)` when the
peer is a Client called `<name>`, or `None` when the peer is the Coordinator.
To open a stream to the Coordinator, pass `None` to `new_stream`:

```ignore
let (mut to_coord, _from_coord) = client
    .new_stream(None)   // NOTE: None means open stream to Coordinator
    .await
    .unwrap();
to_coord.send(Bytes::from("hi coordinator")).await.unwrap();
```

The Coordinator receives this stream by taking the next element from its
[coord::IncomingStreams]:

```ignore
let (peer, strmid, _to_client, mut from_client) = coord_istreams
    .next()
    .await
    .unwrap();
```

For the coordinator, the first element of the returned 4-tuple is a
[String] rather than an [Option], since all incoming streams
must be from clients.
*/

#[macro_use]
mod util;

pub mod ca;
pub mod client;
pub mod coord;
mod types;

#[cfg(test)]
mod tests;

pub use client::{config::ClientConfig, Client};
pub use coord::{config::CoordConfig, Coord};
pub use types::{InStream, OutStream};

mod consts {
    pub(crate) const DFLT_PORT: u16 = 1719;
    pub(crate) const ALPN_CONEC: &[&[u8]] = &[b"conec"];
    pub(crate) const MAX_LOOPS: usize = 8;
    pub(crate) const VERSION: &str = "CONEC_V0.0.10";
    pub(crate) const STRICT_CTRL: bool = true;
}

/// Re-exports from quinn
pub mod quinn {
    /// TLS Certificate (used in [ClientConfig](crate::ClientConfig))
    pub use quinn::Certificate;
}
