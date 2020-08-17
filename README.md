# conec: COordinated NEtwork Channels

[![Documentation](https://docs.rs/conec/badge.svg)](https://docs.rs/conec/)
[![Crates.io](https://img.shields.io/crates/v/conec.svg)](https://crates.io/crates/conec)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE-APACHE)

## high-level description

This is a brand-new WIP repo in which I'll implement the following network
abstraction:

- A coordinated network channels instance comprises one Coordinator
  and one or more Clients.

  Clients know (by configuration) the identity of Coordinator (say,
  a hostname for which Coordinator has a TLS server cert).

  (LATER) Clients could be authenticated, too. TLS client certs?

- The basic abstraction is a channel, which connects two entities
  (Client or Coordinator). A channel comprises one bi-directional
  control stream and zero or more bidirectional data streams.

  Every Client shares a channel with Coordinator: at startup, Client
  connects to Coordinator. Clients can also share a channel with
  other Clients; see below for how this works.

- A data stream accepts a sequence of (known length) messages from
  its writer. The data stream's reader receives these messages
  in order. The stream handles all message framing: a read yields
  a full message or nothing. No support for out-of-order reads;
  use multiple data streams instead.

  (LATER) Not clear yet whether we should support both blocking and
  non-blocking writing and reading. Probably makes sense, but let's
  see how this shapes up before making a decision.

- The control stream handles implementation-specific messaging used
  to set up and tear down channels and streams. For example, when
  one of a channel's endpoints wants to open a new data stream,
  it sends a message over the control stream.

  Coordinator has a special piece of functionality: it can send a new
  channel to a Client (this is roughly analogous to sending a file
  descriptor over a UNIX domain socket via sendmsg() and recvmsg()).
  This is the only way that Client-Client channels can be established.

  (LATER) When Coordinator sends a new channel connecting two Clients,
  this should result in a new network connection between them. For now
  I am assuming that Coordinator will just proxy all messages. Things
  to consider, eventually: NAT traversal functionality?

  (LATER) Is there any need for clients to send channels to other
  clients? I am guessing / hoping no, but maybe I'm wrong.

  (LATER) When Coordinator sends a new channel connecting Client
  A to Client B, do A and B mutually authenticate? Do they encrypt
  their communication to one another? (The second question is only
  relevant if Coordinator is proxying messages.)

## TODOs / functionality / future features / maybes

- error handling
    - [x] eliminate `anyhow` in favor of unboxed Error types
    - [x] create ConecError type to give better error msgs
    - [x] switch to ConecError
- basic functionality
    - [x] per-channel driver @ Coord
    - [x] Client connection: switch handshake order, detect dup-id earlier
    - [x] proxied streams through coordinator
- future features and improvements
    - [x] for Client futures: `map(|x| x.map_err(FooError))` to get rid of multiple unwraps
    - [x] Client authentication via pubkeys
        - right now, client connects with ephemeral self-signed cert. once we
          add client-to-client connections, coord will pass that cert to client.
    - [x] direct Client <-> Coord streams
    - [ ] direct Client <-> Client channels
    - [ ] automagically pick client-to-client vs proxied streams
    - [x] Allow Coord to require trusted CA for client certs
        - in this case, coord will forward trust root for client-to-client
    - [ ] NAT ~detection~ traversal
        - probably not so hard: clone UdpSocket, send a few packets on a timer
          when we try to connect directly to another client. This should work
          for most cases that are not symmetric NATs.
- questions / maybes
    - [x] Client driver - what is the API for this? one driver for whole client?
    - [x] better Client naming (name by pubkey? but only if not ephemeral...)
        - no. eventually, can enforce naming policy by using client cert CA at coord
    - [x] Client <-> Coord streams? (decision: yes)
    - [x] switch ControlMsg -> CoCtrlMsg and ClCtrlMsg? (decision: no)
    - [x] switch from String to Bytes where possible to reduce copying?
        - decision: no: atomics are potentially worse than copies for <= 1kB

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
