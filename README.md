# conec: COordinated NEtwork Channels

[![Documentation](https://docs.rs/conec/badge.svg)](https://docs.rs/conec/)
[![Crates.io](https://img.shields.io/crates/v/conec.svg)](https://crates.io/crates/conec)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE-APACHE)

## high-level description

- A coordinated network channels instance comprises one Coordinator
  and one or more Clients.

  Clients know (by configuration) the identity of Coordinator (say,
  a hostname for which Coordinator has a TLS server cert).

  Clients authenticate to Coordinator with a TLS certificate.
  By default, clients generate ephemeral self-signed certificates;
  they can also be configured to use long-lived certs signed by a
  CA that Coordinator trusts.

- The basic abstraction is a channel, which connects two entities
  (Client or Coordinator). A channel comprises one bi-directional
  control stream and zero or more bidirectional data streams
  (streams defined below).

  Every Client shares a channel with Coordinator: at startup, Client
  connects to Coordinator.

  Clients can ask Coordinator to help them open a channel to another
  Client. In this version there is no explicit support for NAT traversal,
  but because of the low-level networking details clients behind
  [full-cone NAT](https://en.wikipedia.org/wiki/Network_address_translation#Methods_of_translation)
  should get traversal for free. In a future version this will be
  expanded to include address- and port-restricted-cone nats.

- A data stream accepts a sequence of (known length) messages from
  its writer. The data stream's reader receives these messages
  in order. The stream handles all message framing: a read yields
  a full message or nothing. No support for out-of-order reads;
  use multiple data streams instead.

- Control streams are used internally to manage the connection.
  There user code does not interact with them except via the API
  (e.g., in most cases opening a data stream entails sending and
  receiving messages on a control stream).

## TODOs / functionality / future features / maybes

- basic functionality
    - [x] per-channel driver @ Coord
    - [x] Client connection: switch handshake order, detect dup-id earlier
    - [x] proxied streams through coordinator
- features and improvements
    - [x] for Client futures: `map(|x| x.map_err(FooError))` to get rid of multiple unwraps
    - [x] Client authentication via pubkeys
        - right now, client connects with ephemeral self-signed cert. once we
          add client-to-client connections, coord will pass that cert to client.
    - [x] direct Client <-> Coord streams
    - [x] Client keepalive
    - [ ] direct Client <-> Client channels
        - [x] incoming channels listener
        - [x] client-to-client channels impl
        - [x] hold channels in ichan
        - [x] send ichan event to open new channel
        - [x] send ichan event to open new stream
        - [x] who owns Endpoint? Clone in ichan? ((( Option<> in Client? )))
        - [x] allow Client to connect even though it is not listening
        - [x] allow Client to close a channel to another client
            - seems like there is a bug somewhere---possibly in quinn or rustls---that close/reopen triggers
            - [ ] investigate this bug!
    - [x] broadcast channels
    - [ ] add intf to automagically pick between client-to-client and proxied streams
        - super magical version: automatically initiate a new client channel
        - less magical version: only use client channel if one is already open
    - [x] Allow Coord to require trusted CA for client certs
        - in this case, coord will forward trust root for client-to-client
    - [ ] NAT ~detection~ traversal
        - probably not so hard: clone UdpSocket, send a few packets on a timer
          when we try to connect directly to another client. This should work
          for most cases that are not symmetric NATs.
        - even without this, full-cone NAT traversal will already work
    - [ ] more `tracing`
    - [ ] carefully recheck drop notifications for critical pieces of Coord/Chan
- questions / maybes
    - [x] Client driver - what is the API for this? one driver for whole client?
    - [x] better Client naming (name by pubkey? but only if not ephemeral...)
        - no. eventually, can enforce naming policy by using client cert CA at coord
    - [x] Client <-> Coord streams? (decision: yes)
    - [x] switch ControlMsg -> CoCtrlMsg and ClCtrlMsg? (decision: no)
    - [x] switch from String to Bytes where possible to reduce copying?
        - decision: no: atomics are potentially worse than copies for <= 1kB
    - error handling
        - [x] eliminate `anyhow` in favor of unboxed Error types
        - [x] create ConecError type to give better error msgs
        - [x] switch to ConecError

## license

    Copyright 2020 Riad S. Wahby

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
