use crate::conf::consts::SWAP_FILE_SUFFIX;
use crate::error::{Error, Result};
use std::path::Path;

pub(crate) fn validate_catalog_file_name(file_name: &str) -> bool {
    if file_name.is_empty() || !file_name.ends_with(".mtb") {
        return false;
    }
    Path::new(file_name).file_name().and_then(|n| n.to_str()) == Some(file_name)
}

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

pub(crate) fn validate_swap_file_path_candidate(field: &str, path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    if path.as_os_str().is_empty() {
        return Err(Error::InvalidStoragePath(format!(
            "{field} must end with `{SWAP_FILE_SUFFIX}`: {}",
            path.display()
        )));
    }
    if !path_to_utf8(path, field)?.ends_with(SWAP_FILE_SUFFIX) {
        return Err(Error::InvalidStoragePath(format!(
            "{field} must end with `{SWAP_FILE_SUFFIX}`: {}",
            path.display()
        )));
    }
    if path.file_name().is_none() {
        return Err(Error::InvalidStoragePath(format!(
            "{field} must resolve to a file path: {}",
            path.display()
        )));
    }
    Ok(())
}

pub(crate) fn path_to_utf8<'a>(path: &'a Path, field: &str) -> Result<&'a str> {
    path.to_str()
        .ok_or_else(|| Error::InvalidStoragePath(format!("{field} must be valid UTF-8")))
}
