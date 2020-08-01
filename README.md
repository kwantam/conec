# conec: COordinated NEtwork Channels

This is a brand-new WIP repo in which I'll implement the following network
abstraction:

- A coordinated network channels instance comprises one Coordinator
  and one or more Clients.

  Clients know (by configuration) the identity of Coordinator (say,
  a hostname for which Coordinator has a TLS server cert).

  (LATER) Clients could be authenticated, too. TLS client certs?

- The basic abstraction is a channel, which connects two entities
  (Client or Coordinator). A channel comprises one bi-directional
  control stream and zero or more unidirectional data streams.

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

- From the programmer's standpoint, a channel is an object that provides
  methods for reading from and writing to data channels, plus methods
  for setup / teardown, etc. Things like "Client A requests new channel
  to Client B" are handled at the application level; CNC just provides
  the abstractions and low-level functionality.

Based on the above requirements I'm thinking the right approach is
to use QUIC as the network transport, since it already provides the
abstraction of multiple streams over a single network connection. For
now, each Client will have a single QUIC connection to the Coordinator
that handles all of that Client's channels. Later, we can make each
channel a separate QUIC connection directly to the other endpoint.
