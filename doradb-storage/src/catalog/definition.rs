use super::spec::{
    CreateIndexDefinition, CreateTableDefinition, DropIndexDefinition, StorageTableDefinition,
};
use super::storage::TableDescriptorObject;
use crate::error::{Error, OperationError, OperationResult};
use crate::id::TableID;
use error_stack::Report;
use std::error::Error as StdError;
use std::fmt;
use std::result::Result as StdResult;

/// Maximum persisted opaque descriptor payload accepted by managed table DDL.
pub const MAX_TABLE_DESCRIPTOR_BYTES: usize = 64_000;

// One descriptor row has three U64 columns, one 32-byte fingerprint, and two
// one-row VarByte offset tables. The exact current LWC estimate adds 151 bytes
// around the payload. Keep a separate conservative row-page proof so the
// public maximum cannot become constructible in memory but uncheckpointable.
const MAX_DESCRIPTOR_CHECKPOINT_ROW_BYTES: usize = MAX_TABLE_DESCRIPTOR_BYTES + 151;
const MAX_DESCRIPTOR_ROW_PAGE_BYTES: usize = MAX_TABLE_DESCRIPTOR_BYTES + 512;
const _: () = assert!(MAX_DESCRIPTOR_CHECKPOINT_ROW_BYTES <= crate::lwc::LWC_BLOCK_PAYLOAD_SIZE);
const _: () = assert!(MAX_DESCRIPTOR_ROW_PAGE_BYTES <= crate::row::ROW_PAGE_USABLE_SIZE);

/// One typed physical change paired with its complete replacement descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescriptorUpdate<C> {
    change: C,
    descriptor: Box<[u8]>,
}

impl<C> DescriptorUpdate<C> {
    /// Creates a descriptor replacement from a typed change and opaque bytes.
    #[inline]
    pub fn new(change: C, descriptor: impl Into<Box<[u8]>>) -> Self {
        Self {
            change,
            descriptor: descriptor.into(),
        }
    }

    /// Returns the typed physical change.
    #[inline]
    pub const fn change(&self) -> &C {
        &self.change
    }

    /// Returns the complete opaque replacement descriptor.
    #[inline]
    pub const fn descriptor(&self) -> &[u8] {
        &self.descriptor
    }

    /// Consumes the update into its typed change and opaque descriptor.
    #[inline]
    pub fn into_parts(self) -> (C, Box<[u8]>) {
        (self.change, self.descriptor)
    }
}

/// Synchronous higher-layer interpreter for opaque managed table requests.
pub trait TableDescriptorInterpreter {
    /// User-defined interpretation failure preserved by the managed DDL boundary.
    type Error;

    /// Interprets an opaque request for a new ID-free table definition.
    fn create_table(
        &mut self,
        source: &[u8],
    ) -> StdResult<DescriptorUpdate<CreateTableDefinition>, Self::Error>;

    /// Interprets an opaque request to add one index to a current managed table.
    fn create_index(
        &mut self,
        source: &[u8],
        previous_descriptor: &[u8],
        current_schema: &StorageTableDefinition,
        proposed_index_id: super::IndexID,
    ) -> StdResult<DescriptorUpdate<CreateIndexDefinition>, Self::Error>;

    /// Interprets an opaque request to drop one index from a current managed table.
    fn drop_index(
        &mut self,
        source: &[u8],
        previous_descriptor: &[u8],
        current_schema: &StorageTableDefinition,
    ) -> StdResult<DescriptorUpdate<DropIndexDefinition>, Self::Error>;
}

/// Public failure boundary for one managed DDL attempt.
#[derive(Debug)]
pub enum ManagedDdlError<E> {
    /// DoraDB validation, lifecycle, persistence, or execution failure.
    Engine(Error),
    /// User-defined interpreter failure.
    Interpreter(E),
}

impl<E> ManagedDdlError<E> {
    /// Returns the engine error, if this failure came from DoraDB.
    #[inline]
    pub const fn engine(&self) -> Option<&Error> {
        match self {
            Self::Engine(error) => Some(error),
            Self::Interpreter(_) => None,
        }
    }

    /// Returns the interpreter error, if interpretation failed.
    #[inline]
    pub const fn interpreter(&self) -> Option<&E> {
        match self {
            Self::Engine(_) => None,
            Self::Interpreter(error) => Some(error),
        }
    }

    /// Consumes this failure and returns its engine error arm.
    #[inline]
    pub fn into_engine(self) -> Option<Error> {
        match self {
            Self::Engine(error) => Some(error),
            Self::Interpreter(_) => None,
        }
    }

    /// Consumes this failure and returns its interpreter error arm.
    #[inline]
    pub fn into_interpreter(self) -> Option<E> {
        match self {
            Self::Engine(_) => None,
            Self::Interpreter(error) => Some(error),
        }
    }
}

impl<E: fmt::Display> fmt::Display for ManagedDdlError<E> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Engine(error) => write!(f, "managed DDL engine error: {error}"),
            Self::Interpreter(error) => write!(f, "managed DDL interpreter error: {error}"),
        }
    }
}

impl<E: StdError + 'static> StdError for ManagedDdlError<E> {
    #[inline]
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Engine(error) => Some(error),
            Self::Interpreter(error) => Some(error),
        }
    }
}

/// Result of one complete engine-orchestrated managed DDL attempt.
pub type ManagedDdlResult<T, E> = StdResult<T, ManagedDdlError<E>>;

/// Private optimistic definition copied while target metadata-S is held.
pub(crate) struct CurrentTableDefinition {
    schema: StorageTableDefinition,
    descriptor: TableDescriptorObject,
    storage_epoch: u64,
    effective_next_index_id: u64,
}

impl CurrentTableDefinition {
    /// Builds one private coherent preflight snapshot.
    #[inline]
    pub(crate) fn new(
        schema: StorageTableDefinition,
        descriptor: TableDescriptorObject,
        storage_epoch: u64,
        effective_next_index_id: u64,
    ) -> Self {
        Self {
            schema,
            descriptor,
            storage_epoch,
            effective_next_index_id,
        }
    }

    /// Returns the slot-free public projection supplied to the interpreter.
    #[inline]
    pub(crate) const fn schema(&self) -> &StorageTableDefinition {
        &self.schema
    }

    /// Returns the exact descriptor envelope copied during preflight.
    #[inline]
    pub(crate) const fn descriptor(&self) -> &TableDescriptorObject {
        &self.descriptor
    }

    /// Returns the private expected storage epoch.
    #[inline]
    pub(crate) const fn storage_epoch(&self) -> u64 {
        self.storage_epoch
    }

    /// Returns the private effective stable-index allocator watermark.
    #[inline]
    pub(crate) const fn effective_next_index_id(&self) -> u64 {
        self.effective_next_index_id
    }
}

/// Extensible storage-owned catalog effects accepted with a DDL plan.
#[derive(Clone)]
pub(crate) struct CatalogDefinitionEffects {
    descriptor: TableDescriptorEffect,
}

impl CatalogDefinitionEffects {
    /// Builds an empty definition-effect bundle for unmanaged DDL.
    #[inline]
    pub(crate) const fn none() -> Self {
        Self {
            descriptor: TableDescriptorEffect::None,
        }
    }

    /// Builds a managed CREATE TABLE descriptor insertion bundle.
    #[inline]
    pub(crate) const fn insert(descriptor: TableDescriptorObject) -> Self {
        Self {
            descriptor: TableDescriptorEffect::Insert(descriptor),
        }
    }

    /// Builds a managed index DDL descriptor replacement bundle.
    #[inline]
    pub(crate) const fn replace(descriptor: TableDescriptorObject) -> Self {
        Self {
            descriptor: TableDescriptorEffect::Replace(descriptor),
        }
    }

    /// Builds a DROP TABLE optional descriptor deletion bundle.
    #[inline]
    pub(crate) const fn delete_if_present(table_id: TableID) -> Self {
        Self {
            descriptor: TableDescriptorEffect::DeleteIfPresent(table_id),
        }
    }

    /// Returns the descriptor portion of the extensible bundle.
    #[inline]
    pub(crate) const fn descriptor(&self) -> &TableDescriptorEffect {
        &self.descriptor
    }
}

/// Descriptor-row mutation committed with one numeric catalog DDL operation.
#[derive(Clone)]
pub(crate) enum TableDescriptorEffect {
    /// No descriptor change for unmanaged DDL.
    None,
    /// Insert a new managed descriptor.
    Insert(TableDescriptorObject),
    /// Replace a required current managed descriptor.
    Replace(TableDescriptorObject),
    /// Delete a descriptor if the dropped table was managed.
    DeleteIfPresent(TableID),
}

/// Validates the live managed DDL descriptor payload envelope.
#[inline]
pub(crate) fn validate_descriptor_payload(payload: &[u8]) -> OperationResult<()> {
    if payload.len() > MAX_TABLE_DESCRIPTOR_BYTES || payload.len() > usize::from(u16::MAX) {
        return Err(Report::new(OperationError::InvalidMetadata).attach(format!(
            "managed table descriptor exceeds maximum: actual={}, maximum={MAX_TABLE_DESCRIPTOR_BYTES}",
            payload.len()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::DiscloseError;
    use std::io;

    #[test]
    fn descriptor_update_exposes_and_consumes_both_parts() {
        let update = DescriptorUpdate::new(42, &b"\xff\0"[..]);
        assert_eq!(update.change(), &42);
        assert_eq!(update.descriptor(), [0xff, 0]);
        let (change, descriptor) = update.into_parts();
        assert_eq!(change, 42);
        assert_eq!(&*descriptor, [0xff, 0]);
    }

    #[test]
    fn managed_error_preserves_both_domains_and_standard_error_traits() {
        let engine_error = Report::new(OperationError::InvalidMetadata).disclose();
        let error = ManagedDdlError::<io::Error>::Engine(engine_error);
        assert!(error.engine().is_some());
        assert!(error.interpreter().is_none());
        assert!(format!("{error}").contains("managed DDL engine error"));
        assert!(StdError::source(&error).is_some());
        assert!(error.into_engine().is_some());

        let error = ManagedDdlError::Interpreter(io::Error::other("interpretation failed"));
        assert!(error.engine().is_none());
        assert_eq!(
            error.interpreter().map(ToString::to_string).as_deref(),
            Some("interpretation failed")
        );
        assert!(format!("{error}").contains("managed DDL interpreter error"));
        assert!(StdError::source(&error).is_some());
        assert!(error.into_interpreter().is_some());

        let engine_error = Report::new(OperationError::InvalidMetadata).disclose();
        assert!(
            ManagedDdlError::<io::Error>::Engine(engine_error)
                .into_interpreter()
                .is_none()
        );
        assert!(
            ManagedDdlError::Interpreter(io::Error::other("failure"))
                .into_engine()
                .is_none()
        );
    }
}
