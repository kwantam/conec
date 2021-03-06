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
  Client.  By default, Clients will attempt to use holepunching for
  NAT traversal; this ought to work except when the Client receiving
  the connection is behind a
  [symmetric NAT](https://en.wikipedia.org/wiki/Network_address_translation#Methods_of_translation).
  Please post an issue on the GitHub repo if this doesn't work for you!

- A data stream accepts a sequence of (known length) messages from
  its writer. The data stream's reader receives these messages
  in order. The stream handles all message framing: a read yields
  a full message or nothing. No support for out-of-order reads;
  use multiple data streams instead.

- Control streams are used internally to manage the connection.
  The user code does not interact with them except via the API
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
    - [x] ~direct Client <-> Coord streams~
        - [x] v0.0.12 removes these --- just use a new Client with a well-known name!
            - this ends up being a considerable simplification
    - [x] Client keepalive
    - [x] direct Client <-> Client channels
        - [x] incoming channels listener
        - [x] client-to-client channels impl
        - [x] hold channels in ichan
        - [x] send ichan event to open new channel
        - [x] send ichan event to open new stream
        - [x] who owns Endpoint? Clone in ichan? ((( Option<> in Client? )))
        - [x] allow Client to connect even though it is not listening
        - [x] allow Client to close a channel to another client
            - seems like there is a bug somewhere---quinn? rustls?---that close/reopen triggers
            - [ ] investigate this bug!
    - [x] broadcast streams for Clients
    - [x] ~broadcast streams for Coordinator?~
        - no: just connect a Client
        - v0.0.12 removed Coordinator-side stream support
    - [x] Broadcast: identify the sending client?
    - [x] add intf to automagically pick between client-to-client and proxied streams
    - [x] Allow Coord to require trusted CA for client certs
        - in this case, coord will forward trust root for client-to-client
    - [x] NAT traversal
        - holepunching works in my one-off tests. Can we test automatically?
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
