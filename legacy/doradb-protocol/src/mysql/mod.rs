#[macro_use]
pub mod macros;

pub mod auth;
pub mod cmd;
pub mod col;
pub mod conn;
pub mod error;
pub mod flag;
pub mod handshake;
pub mod packet;
pub mod principal;
pub mod resultset;
pub mod serde;
pub mod time;
pub mod value;

pub struct ServerSpec {
    pub version: String,
    pub protocol_version: u8,
}

impl Default for ServerSpec {
    #[inline]
    fn default() -> Self {
        ServerSpec {
            version: String::from("mysql-8.0.30-doradb"),
            protocol_version: 10,
        }
    }
}
