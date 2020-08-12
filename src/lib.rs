// Copyright 2020 Riad S. Wahby <rsw@cs.stanford.edu>
//
// This file is part of conec.
//
// Licensed under the Apache License, Version 2.0 (see
// LICENSE or https://www.apache.org/licenses/LICENSE-2.0).
// This file may not be copied, modified, or distributed
// except according to those terms.

/**
 * conec: COordinated NEtwork Channels
 */

pub mod client;
pub mod coord;
#[cfg(test)]
mod tests;
pub mod types;
mod util;

pub use client::{config::ClientConfig, Client};
pub use coord::{config::CoordConfig, Coord};

mod consts {
    pub(crate) const DFLT_PORT: u16 = 1719;
    pub(crate) const ALPN_CONEC: &[&[u8]] = &[b"conec"];
    pub(crate) const MAX_LOOPS: usize = 8;
    pub(crate) const VERSION: &str = "CONEC_V0.0.2::";
}
