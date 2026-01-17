# Unit Testing

This project uses `cargo test` for all unit tests. It is crucial to ensure that all tests pass before submitting any code changes.

## `libaio` Feature Gate

The storage engine uses `libaio` for asynchronous I/O operations to achieve high performance. This feature is enabled by default. However, `libaio` may not be available in all development or testing environments (e.g., certain cloud-based sandboxes).

To accommodate this, there is a feature gate for `libaio`. When the `libaio` feature is disabled, the system falls back to a thread-pool-based implementation for I/O. This ensures that all unit tests can still be executed and are expected to succeed.

### Running Tests

*   **With `libaio` (default):**

    ```bash
    cargo test
    ```

*   **Without `libaio`:**

    Use the `--no-default-features` flag to disable the `libaio` feature.

    ```bash
    cargo test --no-default-features
    ```

## Test Policy

-   When making code changes, you must ensure that all existing tests continue to pass.
-   All new features or bug fixes should be accompanied by well-designed unit tests to cover the new logic.
-   Run tests with and without the `libaio` feature to ensure both I/O backends are working correctly, if your changes touch I/O related code.
