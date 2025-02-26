/// Type to allow send raw pointer across multiple threads.
pub struct UnsafePtr<T>(pub(crate) *mut T);

impl<T> Clone for UnsafePtr<T> {
    #[inline]
    fn clone(&self) -> Self {
        UnsafePtr(self.0)
    }
}

unsafe impl<T: Send> Send for UnsafePtr<T> {}
