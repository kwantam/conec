/**
 * conec: COordinated NEtwork Channels
 */
pub mod client;
pub mod coord;
#[cfg(test)]
mod tests;
pub mod types;

mod consts {
    pub(crate) const DFLT_PORT: u16 = 1719;
    pub(crate) const ALPN_CONEC: &[&[u8]] = &[b"conec"];
}
