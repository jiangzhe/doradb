/// Utility trait to support static lifetime.
/// This is used for component which lifetime goes
/// through entire program, and other threads may access it
/// concurrently.
///
/// Use static lifetime can eliminate performance panalty
/// on reference counter maintainance.
///
/// # Safety
///
/// We must guarantee after the destruction, no thread
/// will access the leaked static reference.
pub unsafe trait StaticLifetime: Sized {
    /// Create a leaked static reference from given instance.
    fn new_static(this: Self) -> &'static Self {
        Box::leak(Box::new(this))
    }

    /// Drop the leaked reference as it's actually owned object.
    ///
    /// # Safety
    ///
    /// This method is marked as unsafe because caller must guarantee
    /// No thread will access this reference after it's dropped.
    /// If multiple objects of static lifetime has dependencies.
    /// The drop order is important.
    unsafe fn drop_static(this: &'static Self) {
        unsafe {
            drop(Box::from_raw(this as *const Self as *mut Self));
        }
    }
}
