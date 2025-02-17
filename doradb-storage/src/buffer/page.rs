pub const PAGE_SIZE: usize = 64 * 1024;
pub type Page = [u8; PAGE_SIZE];
pub type PageID = u64;
pub const INVALID_PAGE_ID: PageID = !0;

/// PageStatus indicates the status of page.
/// In our design, a page is mapped to a frame through its entire lifetime.
/// So page status is also frame status.
pub enum PageStatus {
    /// Page is in memory.
    Hot,
    /// Page is marked and will be evicted soon.
    Cool,
    /// Page is evicted(on-disk)
    Cold,
    /// Page is loading from disk to memory
    Loading,
}

pub const PAGE_STATUS_MASK: u64 = 0b11 << 62;
pub const PAGE_STATUS_HOT: u64 = 0b00 << 62;
pub const PAGE_STATUS_COOL: u64 = 0b01 << 62;
pub const PAGE_STATUS_COLD: u64 = 0b10 << 62;
pub const PAGE_STATUS_LOADING: u64 = 0b11 << 62;

pub const PAGE_NO_MASK: u64 = !0;
pub const PAGE_VERSION_MASK: u64 = !PAGE_STATUS_MASK;
