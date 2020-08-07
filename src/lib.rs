/**
 * conec: COordinated NEtwork Channels
 */
pub mod client;
pub mod coord;
#[cfg(test)]
mod tests;
pub mod types;

pub use client::{Client, config::ClientConfig};
pub use coord::{Coord, config::CoordConfig};

mod consts {
    pub(crate) const DFLT_PORT: u16 = 1719;
    pub(crate) const ALPN_CONEC: &[&[u8]] = &[b"conec"];
    pub(crate) const MAX_LOOPS: usize = 8;
}
