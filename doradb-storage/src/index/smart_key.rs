use std::alloc::{alloc, Layout};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::mem::{self, ManuallyDrop, MaybeUninit};
use std::ops::Deref;

pub const SMART_KEY_LEN: usize = 64;
pub const SMART_KEY_INLINE: usize = SMART_KEY_LEN - mem::size_of::<usize>();
pub const SMART_KEY_HEAP_PREFIX: usize =
    SMART_KEY_LEN - mem::size_of::<usize>() - mem::size_of::<Box<[u8]>>();

#[repr(transparent)]
pub struct SmartKey(Inner);

impl SmartKey {
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        if self.0.len <= SMART_KEY_INLINE {
            unsafe { &self.0.u.i[..self.0.len] }
        } else {
            unsafe { &self.0.u.h.data[..self.0.len] }
        }
    }
}

impl From<Vec<u8>> for SmartKey {
    #[inline]
    fn from(mut value: Vec<u8>) -> Self {
        if value.len() <= SMART_KEY_INLINE {
            unsafe {
                let mut k = MaybeUninit::<SmartKey>::uninit();
                let k_mut = k.assume_init_mut();
                k_mut.0.len = value.len();
                k_mut.0.u.i[..value.len()].copy_from_slice(&value);
                return k.assume_init();
            }
        }
        unsafe {
            // avoid allocation caused by shrinking.
            let len = value.len();
            let cap = value.capacity();
            value.set_len(cap);
            let data = value.into_boxed_slice();
            let mut k = MaybeUninit::<SmartKey>::uninit();
            let k_mut = k.assume_init_mut();
            k_mut.0.len = len;
            (*k_mut.0.u.h)
                .prefix
                .copy_from_slice(&data[..SMART_KEY_HEAP_PREFIX]);
            std::ptr::write(&mut (*k_mut.0.u.h).data, data);
            k.assume_init()
        }
    }
}

impl From<&[u8]> for SmartKey {
    #[inline]
    fn from(value: &[u8]) -> Self {
        if value.len() <= SMART_KEY_INLINE {
            unsafe {
                let mut k = MaybeUninit::<SmartKey>::uninit();
                let inner = &mut k.assume_init_mut().0;
                inner.len = value.len();
                inner.u.i[..value.len()].copy_from_slice(value);
                return k.assume_init();
            }
        }
        unsafe {
            let ptr = alloc(Layout::from_size_align_unchecked(value.len(), 1));
            let mut data = Vec::from_raw_parts(ptr, value.len(), value.len());
            data.copy_from_slice(value);
            let data = data.into_boxed_slice();
            let mut k = MaybeUninit::<SmartKey>::uninit();
            let inner = &mut k.assume_init_mut().0;
            inner.len = value.len();
            (*inner.u.h)
                .prefix
                .copy_from_slice(&data[..SMART_KEY_HEAP_PREFIX]);
            std::ptr::write(&mut (*inner.u.h).data, data);
            k.assume_init()
        }
    }
}

impl Deref for SmartKey {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_bytes()
    }
}

impl Drop for SmartKey {
    #[inline]
    fn drop(&mut self) {
        if self.0.len > SMART_KEY_INLINE {
            unsafe {
                ManuallyDrop::drop(&mut self.0.u.h);
            }
        }
    }
}

impl Hash for SmartKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

impl PartialEq for SmartKey {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes().eq(other.as_bytes())
    }
}

impl Eq for SmartKey {}

impl Ord for SmartKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

impl PartialOrd for SmartKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[repr(C)]
struct Inner {
    len: usize,
    u: InlineOrHeap,
}

union InlineOrHeap {
    i: [u8; SMART_KEY_INLINE],
    h: ManuallyDrop<Heap>,
}

#[repr(C)]
struct Heap {
    data: Box<[u8]>,
    prefix: [u8; SMART_KEY_HEAP_PREFIX],
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_smart_key_from_str() {
        let s = Vec::from(b"hello");
        let smart_key = SmartKey::from(&s[..]);
        assert_eq!(smart_key.as_bytes(), &s[..]);
    }
}
