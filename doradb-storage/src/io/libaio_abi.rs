use libc::{c_int, c_long, timespec};

#[allow(non_camel_case_types)]
pub enum io_iocb_cmd {
    IO_CMD_PREAD = 0,
    IO_CMD_PWRITE = 1,
    IO_CMD_FSYNC = 2,
    IO_CMD_FDSYNC = 3,
    // IO_CMD_PREADX = 4,
    IO_CMD_POLL = 5,
    IO_CMD_NOOP = 6,
    IO_CMD_PREADV = 7,
    IO_CMD_PWRITEV = 8,
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct iocb {
    pub data: u64,
    pub key: u32,
    _aio_rw_flags: u32, // not used.
    pub aio_lio_opcode: u16,
    pub aio_reqprio: u16,
    pub aio_fildes: u32,
    // PREAD/PWRITE -> void*
    // PREADV/PWRITEV -> iovec
    pub buf: *mut u8,
    pub count: u64,
    pub offset: u64,
    _padding: u64, // not used.
    pub flags: u32,
    pub resfd: u32,
}

impl Default for iocb {
    #[inline]
    fn default() -> Self {
        iocb {
            data: 0,
            key: 0,
            _aio_rw_flags: 0,
            aio_lio_opcode: io_iocb_cmd::IO_CMD_NOOP as u16,
            aio_reqprio: 0,
            aio_fildes: !0,
            buf: std::ptr::null_mut(),
            count: 0,
            offset: 0,
            _padding: 0,
            flags: 0,
            resfd: 0,
        }
    }
}

unsafe impl Send for iocb {}

pub const IOCB_FLAG_RESFD: u32 = 1;

#[derive(Clone)]
#[repr(C)]
#[allow(non_camel_case_types)]
pub struct io_event {
    pub data: u64,
    pub obj: *mut iocb,
    pub res: i64,
    pub res2: i64,
}

impl Default for io_event {
    #[inline]
    fn default() -> Self {
        io_event {
            data: 0,
            obj: std::ptr::null_mut(),
            res: 0,
            res2: 0,
        }
    }
}

#[allow(non_camel_case_types)]
pub enum io_context {}

#[allow(non_camel_case_types)]
pub type io_context_t = *mut io_context;

#[repr(C)]
#[allow(non_camel_case_types)]
pub struct iovec {
    pub iov_base: *mut u8,
    pub iov_len: usize,
}

#[link(name = "aio")]
extern "C" {
    pub fn io_queue_init(maxevents: c_int, ctxp: *mut io_context_t) -> c_int;

    pub fn io_queue_release(ctx: io_context_t) -> c_int;

    pub fn io_queue_run(ctx: io_context_t) -> c_int;

    pub fn io_setup(maxevents: c_int, ctxp: *mut io_context_t) -> c_int;

    pub fn io_destroy(ctx: io_context_t) -> c_int;

    pub fn io_submit(ctx: io_context_t, nr: c_long, ios: *mut *mut iocb) -> c_int;

    pub fn io_cancel(ctx: io_context_t, iocb: *mut iocb, evt: *mut io_event) -> c_int;

    pub fn io_getevents(
        ctx_id: io_context_t,
        min_nr: c_long,
        nr: c_long,
        events: *mut io_event,
        timeout: *mut timespec,
    ) -> c_int;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_libaoi_abi_size() {
        assert!(std::mem::size_of::<io_event>() == 32);
        assert!(std::mem::size_of::<iocb>() == 64);
    }
}
