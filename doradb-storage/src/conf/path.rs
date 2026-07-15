use crate::conf::consts::SWAP_FILE_SUFFIX;
use crate::error::{ConfigError, ConfigResult};
use error_stack::{Report, ResultExt, ensure};
use std::path::Path;

/// Return whether a catalog file name is a plain `.mtb` file name.
pub(crate) fn validate_catalog_file_name(file_name: &str) -> bool {
    if file_name.is_empty() || !file_name.ends_with(".mtb") {
        return false;
    }
    Path::new(file_name).file_name().and_then(|n| n.to_str()) == Some(file_name)
}

/// Return whether a redo log stem is a plain file name without glob characters.
pub(crate) fn validate_log_file_stem(file_stem: &str) -> bool {
    if file_stem.is_empty() {
        return false;
    }
    if Path::new(file_stem).file_name().and_then(|n| n.to_str()) != Some(file_stem) {
        return false;
    }
    !file_stem
        .chars()
        .any(|c| matches!(c, '*' | '?' | '[' | ']' | '{' | '}'))
}

/// Validate a configured swap-file path before storage-root resolution.
pub(crate) fn validate_swap_file_path_candidate(path: impl AsRef<Path>) -> ConfigResult<()> {
    let path = path.as_ref();
    (|| {
        ensure!(
            !path.as_os_str().is_empty(),
            ConfigError::PathMustNotBeEmpty
        );
        Ok(())
    })()
    .attach_with(|| format!("swap-file path must not be empty: {}", path.display()))?;
    let path_str =
        path_to_utf8(path).attach_with(|| format!("invalid swap-file path: {}", path.display()))?;
    (|| {
        ensure!(
            path_str.ends_with(SWAP_FILE_SUFFIX),
            ConfigError::PathMustUseRequiredSuffix
        );
        Ok(())
    })()
    .attach_with(|| {
        format!(
            "swap-file path must end with `{SWAP_FILE_SUFFIX}`: {}",
            path.display()
        )
    })?;
    (|| {
        ensure!(
            path.file_name().is_some(),
            ConfigError::PathMustResolveToFile
        );
        Ok(())
    })()
    .attach_with(|| format!("swap-file path must resolve to a file: {}", path.display()))?;
    Ok(())
}

/// Convert a path to UTF-8 for configuration serialization and diagnostics.
pub(crate) fn path_to_utf8(path: &Path) -> ConfigResult<&str> {
    path.to_str().ok_or_else(|| {
        Report::new(ConfigError::PathMustBeUtf8)
            .attach(format!("path must be valid UTF-8: {}", path.display()))
    })
}
