use crate::conf::consts::SWAP_FILE_SUFFIX;
use crate::error::{ConfigError, ConfigResult};
use error_stack::{Report, ResultExt, ensure};
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

pub(crate) fn validate_swap_file_path_candidate(
    field: &str,
    path: impl AsRef<Path>,
) -> ConfigResult<()> {
    let path = path.as_ref();
    (|| {
        ensure!(
            !path.as_os_str().is_empty(),
            ConfigError::PathMustNotBeEmpty
        );
        Ok(())
    })()
    .attach_with(|| format!("{field} must not be empty"))?;
    let path_str =
        path_to_utf8(path, field).attach_with(|| format!("invalid {field}: {}", path.display()))?;
    (|| {
        ensure!(
            path_str.ends_with(SWAP_FILE_SUFFIX),
            ConfigError::PathMustUseRequiredSuffix
        );
        Ok(())
    })()
    .attach_with(|| {
        format!(
            "{field} must end with `{SWAP_FILE_SUFFIX}`: {}",
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
    .attach_with(|| format!("{field} must resolve to a file path: {}", path.display()))?;
    Ok(())
}

pub(crate) fn path_to_utf8<'a>(path: &'a Path, field: &str) -> ConfigResult<&'a str> {
    path.to_str().ok_or_else(|| {
        Report::new(ConfigError::PathMustBeUtf8).attach(format!("{field} must be valid UTF-8"))
    })
}
