use crate::mysql::packet::ErrPacket;
use std::borrow::Cow;
use std::fmt;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

macro_rules! define_errors {
    ($( $id:ident ( $($ty:ty),* ) : $n:literal => $code:literal, $state:literal, $msg:expr ; )*) => {
        #[derive(Debug, Clone, Error)]
        pub enum Error {
            #[error("error packet")]
            ErrPacket(Box<ErrPacket<'static>>),
            $(
                #[error( $msg )]
                $id ( $($ty,)* )
            ),*
        }

        impl Error {
            #[inline]
            pub fn code_and_state(&self) -> (u16, [u8; 5]) {
                match self {
                    Self::ErrPacket(pkt) => (pkt.code, pkt.state),
                    $(
                        Self::$id(..) => ($code, *$state)
                    ),*
                }
            }

            #[inline]
            pub fn num(&self) -> u16 {
                match self {
                    Self::ErrPacket(_) => 65535,
                    $(
                        Self::$id(..) => $n
                    ),*
                }
            }
        }
    }
}

define_errors! {
    AccessDenied(Box<AccessDenied>): 33 => 1045, b"28000", "{0}";
    BadDB(Box<String>): 37 => 1049, b"42000", "Unknown database '{0}'";
    SyntaxError(): 135 => 1149, b"42000", "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use";
    NetPacketOutOfOrder(): 137 => 1153, b"08S01", "Got packets out of order";
    WrongStringLength(): 420 => 1470, b"HY000", "String is too long";
    MalformedPacket(): 733 => 1834, b"HY000", "Malformed communication packet.";
    AuthRSACantParse(): 1806 => 10285, b"HY000", "Failure to parse RSA key";

    // error code larger than 60000 does not compatible with mysql errors.
    ExceedsCapacity(usize) : 60000 => 60000, b"00000", "Exceeds buffer capacity";
    InvalidBufferState() : 60001 => 60001, b"00000", "Invalid buffer state";
    IOError(std::io::ErrorKind) : 60002 => 60002, b"00000", "{0}";
    InvalidStatusFlags(u16) : 60003 => 60003, b"00000", "Invalid status flags {0}";
    InvalidPacketHeader(u8) : 60004 => 60004, b"00000", "Invalid packet header {0}";
    BufferFull(usize) : 60005 => 60005, b"00000", ""; // internal error
    PacketSplit() : 60006 => 60006, b"00000", ""; // internal error
    UnexpectedEOF() : 60007 => 60007, b"00000", "Unexpected EOF";
    Unimplemented(Box<&'static str>) : 60008 => 60008, b"00000", "'{0}' unimplemented";
    InvalidUtf8String(): 60009 => 60009, b"00000", "Invalid utf-8 string";
    InvalidCommandCode(u8): 60010 => 60010, b"00000", "Invalid command code {0}";
    InsufficientInput(): 60011 => 60011, b"00000", "Insufficient input";
    InvalidLengthEncoding(): 60012 => 60012, b"00000", "Invalid length encoding";
    AuthPluginNotSupported(Box<String>): 60013 => 60013, b"00000", "Auth plugin '{0}' not supported";
    AuthRSACantEncrypt(): 60014 => 60014, b"00000", "Failure to encrypt using RSA key";
    InvalidColumnType(): 60015 => 60015, b"00000", "Invalid column type";
    ServerNotStarted(): 60016 => 60016, b"00000", "Server not started";
    PlanError(Box<String>): 60017 => 60017, b"00000", "{0}";
    Cancelled(): 60018 => 60018, b"00000", "Execution cancelled";
    InvalidExecutorState(): 60019 => 60019, b"00000", "Invalid executor state";
    RuntimeError(Box<String>): 60020 => 60020, b"00000", "{0}";
}

#[derive(Debug, Clone)]
pub struct AccessDenied {
    pub user: String,
    pub host: String,
}

impl fmt::Display for AccessDenied {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Access denied for user '{}'@'{}'", self.user, self.host)
    }
}

impl From<crate::buf::Error> for Error {
    #[inline]
    fn from(src: crate::buf::Error) -> Self {
        match src {
            crate::buf::Error::ExceedsCapacity(n) => Error::ExceedsCapacity(n),
            crate::buf::Error::InvalidState => Error::InvalidBufferState(),
        }
    }
}

impl From<std::io::Error> for Error {
    #[inline]
    fn from(src: std::io::Error) -> Self {
        Error::IOError(src.kind())
    }
}

impl From<ErrPacket<'_>> for Error {
    #[inline]
    fn from(src: ErrPacket) -> Self {
        let msg = match src.msg {
            Cow::Borrowed(b) => Vec::from(b),
            Cow::Owned(vec) => vec,
        };
        Error::ErrPacket(Box::new(ErrPacket {
            code: src.code,
            state: src.state,
            msg: Cow::Owned(msg),
        }))
    }
}

impl From<Error> for ErrPacket<'static> {
    #[inline]
    fn from(src: Error) -> Self {
        match src {
            Error::ErrPacket(pkt) => (*pkt).clone(),
            _ => {
                let (code, state) = src.code_and_state();
                let msg = src.to_string();
                ErrPacket {
                    code,
                    state,
                    msg: Cow::Owned(msg.into_bytes()),
                }
            }
        }
    }
}

impl From<doradb_sql::error::Error> for Error {
    #[inline]
    fn from(src: doradb_sql::error::Error) -> Self {
        match src {
            doradb_sql::error::Error::SyntaxError(_) => Error::SyntaxError(),
        }
    }
}

impl From<doradb_plan::error::Error> for Error {
    #[inline]
    fn from(src: doradb_plan::error::Error) -> Self {
        Error::PlanError(Box::new(src.to_string()))
    }
}

#[inline]
pub(crate) fn ensure_empty(b: &[u8]) -> Result<()> {
    if !b.is_empty() {
        return Err(Error::MalformedPacket());
    }
    Ok(())
}
