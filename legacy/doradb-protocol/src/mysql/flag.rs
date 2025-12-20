use bitflags::bitflags;

bitflags! {
    /// Capability flags bitmask.
    /// See: https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__capabilities__flags.html
    pub struct CapabilityFlags: u32 {
        const LONG_PASSWORD     = 1;
        const FOUND_ROWS        = 2;
        const LONG_FLAG         = 4;
        const CONNECT_WITH_DB   = 8;
        const NO_SCHEMA         = 16; // Deprecated
        const COMPRESS          = 32;
        const ODBC              = 64;
        const LOCAL_FILES       = 128;
        const IGNORE_SPACE      = 256;
        const PROTOCOL_41       = 512;
        const INTERACTIVE       = 1024;
        const SSL               = 2048;
        const IGNORE_SIGPIPE    = 4096;  // client only
        const TRANSACTIONS      = 8192;
        const RESERVED          = 16384; // Deprecated
        const SECURE_CONNECTION = 32768; // Deprecated? impact handshake packet format.
        const MULTI_STATEMENTS  = 1 << 16;
        const MULTI_RESULTS     = 1 << 17;
        const PS_MULTI_RESULTS  = 1 << 18;
        const PLUGIN_AUTH       = 1 << 19;
        const CONNECT_ATTRS     = 1 << 20;
        const PLUGIN_AUTH_LENENC_CLIENT_DATA = 1 << 21;
        const CAN_HANDLE_EXPIRED_PASSWORDS = 1 << 22;
        const SESSION_TRACK     = 1 << 23;
        const DEPRECATE_EOF     = 1 << 24;
        const OPTIONAL_RESULTSET_METADATA = 1 << 25;
        const ZSTD_COMPRESSION_ALGORITHM = 1 << 26;
        const QUERY_ATTRIBUTES = 1 << 27; // impact COM_QUERY and COM_STMT_EXECUTE
        const MULTI_FACTOR_AUTHENTICATION = 1 << 28;
        const CAPABILITY_EXTENSION = 1 << 29;
        const SSL_VERITY_SERVER_CERT = 1 << 30;
        const REMEMBER_OPTIONS  = 1 << 31;
    }
}

impl Default for CapabilityFlags {
    fn default() -> Self {
        Self::empty()
        | CapabilityFlags::LONG_PASSWORD
        // | CapabilityFlags::FOUND_ROWS
        // | CapabilityFlags::LONG_FLAG
        // | CapabilityFlags::CONNECT_WITH_DB
        // | CapabilityFlags::NO_SCHEMA
        // | CapabilityFlags::COMPRESS
        // | CapabilityFlags::ODBC 
        // | CapabilityFlags::LOCAL_FILES
        // | CapabilityFlags::IGNORE_SPACE
        | CapabilityFlags::PROTOCOL_41
        // | CapabilityFlags::INTERACTIVE 
        // | CapabilityFlags::SSL 
        // | CapabilityFlags::IGNORE_SIGPIPE 
        | CapabilityFlags::TRANSACTIONS
        // | CapabilityFlags::RESERVED
        // | CapabilityFlags::SECURE_CONNECTION 
        // | CapabilityFlags::MULTI_STATEMENTS 
        | CapabilityFlags::MULTI_RESULTS
        // | CapabilityFlags::PS_MULTI_RESULTS
        | CapabilityFlags::PLUGIN_AUTH
        // | CapabilityFlags::CONNECT_ATTRS
        | CapabilityFlags::PLUGIN_AUTH_LENENC_CLIENT_DATA
        // | CapabilityFlags::CAN_HANDLE_EXPIRED_PASSWORDS 
        // | CapabilityFlags::SESSION_TRACK
        | CapabilityFlags::DEPRECATE_EOF
        // | CapabilityFlags::OPTIONAL_RESULTSET_METADATA
        // | CapabilityFlags::ZSTD_COMPRESSION_ALGORITHM
        // | CapabilityFlags::QUERY_ATTRIBUTES
        // | CapabilityFlags::MULTI_FACTOR_AUTHENTICATION
        // | CapabilityFlags::CAPABILITY_EXTENSION
        // | CapabilityFlags::SSL_VERITY_SERVER_CERT
        // | CapabilityFlags::REMEMBER_OPTIONS
    }
}

bitflags! {
    pub struct StatusFlags: u16 {
        const IN_TRANS              = 0x0001;
        const AUTOCOMMIT            = 0x0002;
        // not in doc but in real response
        const X0004                 = 0x0004;
        const MORE_RESULTS_EXISTS   = 0x0008;
        const NO_GOOD_INDEX_USED    = 0x0010;
        const NO_INDEX_USED         = 0x0020;
        const CURSOR_EXISTS         = 0x0040;
        const LAST_ROW_SENT         = 0x0080;
        const DB_DROPPED            = 0x0100;
        const NO_BACKSLASH_ESCAPES  = 0x0200;
        const METADATA_CHANGED      = 0x0400;
        const QUERY_WAS_SLOW        = 0x0800;
        const PS_OUT_PARAMS         = 0x1000;
        const IN_TRANS_READONLY     = 0x2000;
        const SESSION_STATE_CHANGED = 0x4000;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_status_flag() {
        let sf = StatusFlags::from_bits(0b0000100101100110_u16).unwrap();
        dbg!(sf);
    }
}
