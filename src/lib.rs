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
For more information, see [Authentication](#authentication), below.

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
For more information on sending values over streams, see [Stream
Adapters](#stream-adapters), below.

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
shouldn't drop the Coord object unless you want the Coordinator to
stop executing!

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

# Channels and streams

## Proxied streams

Once your Client has connected to its Coordinator, it can set up
data streams with the Coordinator or other Clients, and send data on
those streams:

```ignore
let (mut to_client2, _from_client2) = client
    .new_proxied_stream("client2".to_string())
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
To open a stream to the Coordinator, pass `None` to `new_proxied_stream`:

```ignore
let (mut to_coord, _from_coord) = client
    .new_proxied_stream(None)   // NOTE: None means open stream to Coordinator
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

## Direct streams

Clients can initiate direct connections to one another with [Client::new_channel],
after which they can initiate direct streams to the peer.

```ignore
// first, connect a new channel to client2
client1.new_channel("client2".to_string()).await.unwrap();

// then open a direct stream
let (mut to_client2, _from_client2) = client1
    .new_direct_stream("client2".to_string())
    .await
    .unwrap();
to_client2.send(Bytes::from("hi there")).await.unwrap();
```

Once two clients share a channel, either client can initiate a direct stream
to the other. [Client::close_channel] closes the channel, which ends all
of the direct streams between the two clients.

## Broadcast streams

Clients can open broadcast streams, which allow many peers to send and receive
simultaneously. In this version of Conec, Coordinator cannot participate in
broadcast streams. This may change in a future version; until then, one can
start a local Client and broadcast from there.

Clients open a new broadcast stream with [Client::new_broadcast]. Every client
that supplies a given `chan` argument to [Client::new_broadcast] connects to
the same broadcast stream.

Unlike normal streams, broadcast streams carry both a message and the identity
of its sender. Conec provides several adapters that a Client can apply to a
broadcast InStream: [TaglessBroadcastInStream], [TaggedBroadcastInStream],
and [TaggedDeserializer]. See examples of usage in `tests.rs` and below.

Note that clients receive their own messages to broadcast, too!

```ignore
let (mut s1, r1) = client1
    .new_broadcast("test_broadcast_chan".to_string())
    .await
    .unwrap();
let mut r1 = TaglessBroadcastInStream::new(r1);

let (mut s2, r2) = client2
    .new_broadcast("test_broadcast_chan".to_string())
    .await
    .unwrap();
let r2 = NonblockingInStream::new(r2, 16);
let mut r2 = TaggedBroadcastInStream::new(r2);
// could now adapt with TaggedDeserializer to produce typed values

s1.send(Bytes::from("test test test")).await.unwrap();
let rec1 = r1.try_next().await?.unwrap();
let rec2 = r2.try_next().await?.unwrap();
assert_eq!(rec1, rec2.1);

s2.send(Bytes::from("sibilance")).await.unwrap();
let rec1 = r1.try_next().await?.unwrap();
let rec2 = r2.try_next().await?.unwrap();
assert_eq!(rec1, rec2.1);
```

# Authentication

Upon connecting, Clients require Coordinator to furnish a TLS certificate
that is valid for the hostname specified by the `coord` argument to
[ClientConfig::new]. By default, Clients use the system's CA store to validate
Coordinator's certificate. Coordinator can use a certificate that is self-signed
or signed by a local CA provided that Clients pass the signing certificate
to [ClientConfig::set_ca]. See `tests.rs` for examples of using self-signed
and locally signed Coordinator certificates.

Clients also use TLS certificates to authenticate with Coordinator. By default,
Coordinator will accept a self-signed certificate provided that it is valid for
a name that matches the `id` argument furnished to [ClientConfig::new]; Clients
automatically generate such certificates prior to connecting to Coordinator.

It is possible to instead require clients to present a certificate signed
by a specified CA, using the [CoordConfig::set_client_ca] method. In this
configuration, self-signed certificates are not accepted. This can be used
to implement access control: Clients may only connect to Coordinator if they
have a certificate signed by the correct CA and valid for their `id`.

Before a Client establishes a new channel directly to another Client, each
Client first learns the other's certificate from Coordinator. Upon connecting,
both Clients check that the new peer's certificate matches the one Coordinator
provided. This ensures that Clients connect to the correct entities, even when
using self-signed Client certificates.

**Important:** when Coordinator is configured to use a Client CA via
[CoordConfig::set_client_ca], all Clients that wish to accept direct
channels from other Clients **must** set the same Client CA via
[ClientConfig::set_client_ca].

# Stream adapters

In conec, bi-directional streams are represented by a reader/writer pair,
[InStream] and [OutStream]. InStream is a [TryStream](futures::stream::TryStream)
that outputs items of type [BytesMut](bytes::BytesMut); OutStream is a
[Sink](futures::sink::Sink) that accepts items of type [Bytes](bytes::Bytes).

## Sending typed values over a stream

Both InStream and OutStream can be adapted to accept values of another type,
using [tokio_serde]'s [SymmetricallyFramed](tokio_serde::SymmetricallyFramed)
adapter. This requires the type being written to or read from the channel to
implement the [Serialize](serde::Serialize) and [Deserialize](serde::Deserialize)
traits (these can often be `Derive`d; see the [serde] documentation for more info).
For example:

```ignore
#[derive(Serialize, Deserialize, PartialEq, Eq)]
enum MyType {
    Variant1,
    Variant2,
}

// open a loopback stream
let (send, recv) = client.new_stream("client".to_string()).await.unwrap();

// wrap send and recv in a codec
let send_mytype = SymmetricallyFramed::new(send, SymmetricalBincode::<MyType>::default());
let recv_mytype = SymmetricallyFramed::new(recv, SymmetricalBincode::<MyType>::default());

// send a type and receive the same type back
send_mytype.send(MyType::Variant1).await.unwrap();
assert_eq!(recv_mytype.try_next().await?.unwrap(), MyType::Variant1);
```

## Making streams non-blocking

By default, messages are queued until the receiver reads them from an InStream.
The underlying network transport implements buffering, which allows the sender
to transmit messages even when the receiver does not read them immediately.
This buffering is finite, however; when it is full, the transport layer
applies back-pressure, blocking the sender from transmitting new messages
until the receiver has consumed buffered ones.

In some cases---especially for [broadcast streams](Client::new_broadcast)---it
may be useful to make streams non-blocking, at the cost of forcing slow
receivers to drop messages. The [NonblockingInStream] adapter implements
this at the receiver. Like InStream, NonblockingInStream can be composed
with [tokio_serde]'s [SymmetricallyFramed](tokio_serde::SymmetricallyFramed)
adapter:

```ignore
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
enum MyType {
    Variant1,
    Variant2,
}

// open a stream to another client
let (send, recv) = client.new_stream("client2".to_string()).await.unwrap();

// wrap recv in a codec and make sure it does not block sender
let recv = NonblockingInStream::new(recv, 16);
let recv_mytype = SymmetricallyFramed::new(recv, SymmetricalBincode::<MyType>::default());

match recv_mytype.try_next().await {
    Err(NonblockingInStreamError::Lagged(nlost)) => println!("lost {} messages", nlost),
    Err(_) => panic!("unknown error"),
    Ok(Some(msg)) => println!("received {:?}", msg),
    Ok(None) => println!("stream is closed now"),
};
```

*/

#[macro_use]
mod macros;

pub mod ca;
pub mod client;
pub mod coord;
mod types;
mod util;

#[cfg(test)]
mod tests;

pub use client::{config::ClientConfig, Client};
pub use coord::{config::CoordConfig, Coord};
pub use types::{
    nbistream::{NonblockingInStream, NonblockingInStreamError},
    tagstream::{TaggedBroadcastInStream, TaggedDeserializer, TaglessBroadcastInStream},
    InStream, OutStream,
};

mod consts {
    pub(crate) const DFLT_PORT: u16 = 1719;
    pub(crate) const ALPN_CONEC: &[&[u8]] = &[b"conec"];
    pub(crate) const MAX_LOOPS: usize = 8;
    pub(crate) const VERSION: &str = "CONEC_V0.0.11";
    pub(crate) const STRICT_CTRL: bool = true;
}

/// Re-exports from quinn
pub mod quinn {
    /// TLS Certificate (used in [ClientConfig](crate::ClientConfig))
    pub use quinn::Certificate;
}
