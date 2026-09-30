use super::measurement_error;
use crate::error::{DiscloseError, RuntimeError, RuntimeResult as Result};
use crate::io::read_profiling_procfs;
use error_stack::ResultExt;
use rustix::param::page_size;
use rustix::time::{ClockId, clock_gettime};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Explicitly sampled whole-process resident-set measurements.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SampledProcessRss {
    /// Synchronous RSS sample immediately before starting the sampler.
    pub baseline_bytes: usize,
    /// Greatest one-millisecond or terminal synchronous RSS sample.
    pub peak_bytes: usize,
    /// Saturating sampled peak above the pre-operation baseline.
    pub peak_above_baseline_bytes: usize,
}

/// Running one-millisecond Linux process-RSS sampler.
///
/// Dropping the sampler stops and joins its worker, waiting for any current read
/// or sampling sleep to finish. Use [`Self::stop`] to obtain measurements and
/// report worker errors; implicit cleanup discards them.
pub struct ProcessRssSampler {
    baseline_bytes: usize,
    peak_bytes: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<Result<()>>>,
}

impl ProcessRssSampler {
    /// Capture the baseline, start sampling, and wait for sampler readiness.
    pub fn start() -> crate::Result<Self> {
        (|| -> Result<_> {
            let baseline_bytes = current_process_rss()?;
            let peak_bytes = Arc::new(AtomicUsize::new(baseline_bytes));
            let stop = Arc::new(AtomicBool::new(false));
            let (ready_tx, ready_rx) = mpsc::sync_channel(1);
            let thread_peak = Arc::clone(&peak_bytes);
            let thread_stop = Arc::clone(&stop);
            let thread = thread::Builder::new()
                .name("doradb-profiling-rss".to_owned())
                .spawn(move || {
                    let first = current_process_rss();
                    match first {
                        Ok(bytes) => {
                            thread_peak.fetch_max(bytes, Ordering::Relaxed);
                            let _ = ready_tx.send(Ok(()));
                        }
                        Err(error) => {
                            let _ = ready_tx.send(Err(error));
                            return Ok(());
                        }
                    }
                    while !thread_stop.load(Ordering::Acquire) {
                        thread::sleep(Duration::from_millis(1));
                        let bytes = current_process_rss()?;
                        thread_peak.fetch_max(bytes, Ordering::Relaxed);
                    }
                    Ok(())
                })
                .map_err(|error| {
                    measurement_error(format!("failed to start process RSS sampler: {error}"))
                })?;
            match ready_rx.recv() {
                Ok(Ok(())) => Ok(Self {
                    baseline_bytes,
                    peak_bytes,
                    stop,
                    thread: Some(thread),
                }),
                Ok(Err(error)) => {
                    let _ = thread.join();
                    Err(error.attach("process RSS sampler could not read Linux procfs"))
                }
                Err(error) => {
                    let _ = thread.join();
                    Err(measurement_error(format!(
                        "process RSS sampler readiness channel closed: {error}"
                    )))
                }
            }
        })()
        .map_err(DiscloseError::disclose)
    }

    /// Take the terminal sample, stop and join the sampler, and return its peak.
    pub fn stop(mut self) -> crate::Result<SampledProcessRss> {
        (|| -> Result<_> {
            let final_sample = current_process_rss();
            if let Ok(bytes) = &final_sample {
                self.peak_bytes.fetch_max(*bytes, Ordering::Relaxed);
            }
            self.stop_and_join()?;
            final_sample?;
            let peak_bytes = self.peak_bytes.load(Ordering::Relaxed);
            Ok(SampledProcessRss {
                baseline_bytes: self.baseline_bytes,
                peak_bytes,
                peak_above_baseline_bytes: peak_bytes.saturating_sub(self.baseline_bytes),
            })
        })()
        .map_err(DiscloseError::disclose)
    }

    fn stop_and_join(&mut self) -> Result<()> {
        self.stop.store(true, Ordering::Release);
        let Some(thread) = self.thread.take() else {
            return Ok(());
        };
        // The caller owns this worker independently of engine poison or shutdown.
        // It finishes its current read/sleep before observing the stop flag;
        // joining is authoritative completion, including on caller cancellation.
        // Taking the handle gives stop() or Drop sole cleanup ownership.
        thread
            .join()
            .map_err(|_| measurement_error("process RSS sampler thread panicked before joining"))?
    }
}

impl Drop for ProcessRssSampler {
    fn drop(&mut self) {
        let _ = self.stop_and_join();
    }
}

/// Read all process threads' accumulated CPU time using the safe Linux clock API.
pub fn process_cpu_nanos() -> u64 {
    let value = clock_gettime(ClockId::ProcessCPUTime);
    value.tv_sec as u64 * 1_000_000_000 + value.tv_nsec as u64
}

fn current_process_rss() -> Result<usize> {
    read_process_rss(Path::new("/proc/self/statm"), page_size())
}

fn read_process_rss(path: &Path, page_size: usize) -> Result<usize> {
    let contents = read_profiling_procfs(path)
        .change_context(RuntimeError::ProfilingMeasurement)
        .attach_with(|| format!("failed to read process RSS from {}", path.display()))?;
    parse_statm_rss(&contents, page_size)
}

fn parse_statm_rss(contents: &str, page_size: usize) -> Result<usize> {
    let resident_pages = contents
        .split_ascii_whitespace()
        .nth(1)
        .ok_or_else(|| measurement_error("/proc/self/statm has no resident-page field"))?
        .parse::<usize>()
        .map_err(|error| {
            measurement_error(format!(
                "/proc/self/statm resident-page field is malformed: {error}"
            ))
        })?;
    Ok(resident_pages * page_size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use tempfile::TempDir;

    fn assert_sampler_cleanup(
        sampler: ProcessRssSampler,
        case: &str,
        cleanup: impl FnOnce(ProcessRssSampler),
    ) {
        let stop = Arc::clone(&sampler.stop);
        let peak = Arc::downgrade(&sampler.peak_bytes);
        cleanup(sampler);
        assert!(stop.load(Ordering::Acquire), "stop not signaled: {case}");
        assert_eq!(Arc::strong_count(&stop), 1, "worker retained stop: {case}");
        assert!(peak.upgrade().is_none(), "worker retained peak: {case}");
    }

    fn abandon_sampler(sampler: ProcessRssSampler, exit: &str) {
        match exit {
            "drop" => drop(sampler),
            "early error" => {
                let result = (|| {
                    let _sampler = sampler;
                    Err::<(), _>("caller error")?;
                    Ok::<(), &str>(())
                })();
                assert_eq!(result, Err("caller error"));
            }
            "unwind" => {
                let panic = catch_unwind(AssertUnwindSafe(|| {
                    let _sampler = sampler;
                    panic!("caller panic");
                }))
                .unwrap_err();
                assert_eq!(panic.downcast_ref::<&str>(), Some(&"caller panic"));
            }
            _ => unreachable!("unknown sampler exit: {exit}"),
        }
    }

    /// Purpose: Read accumulated CPU time across all process threads.
    /// Expected: Successive samples remain nondecreasing.
    #[test]
    fn process_cpu_clock_is_nondecreasing() {
        let first = process_cpu_nanos();
        let second = process_cpu_nanos();
        assert!(second >= first);
    }

    /// Purpose: Validate resident-memory accounting from process statistics.
    /// Expected: Resident pages convert accurately to bytes while malformed input is rejected.
    #[test]
    fn process_rss_parser_checks_shape() {
        assert_eq!(parse_statm_rss("100 7 2 1\n", 4_096).unwrap(), 28_672);
        assert!(parse_statm_rss("100\n", 4_096).is_err());
        assert!(parse_statm_rss("100 nope\n", 4_096).is_err());
    }

    /// Purpose: Keep sampled memory peaks consistent with the baseline.
    /// Expected: Explicit stop joins the worker and reports a peak above or equal to its baseline.
    #[test]
    fn process_rss_sampler_synchronizes_and_returns_a_nondecreasing_peak() {
        assert_sampler_cleanup(ProcessRssSampler::start().unwrap(), "stop", |sampler| {
            let sample = sampler.stop().unwrap();
            assert!(sample.peak_bytes >= sample.baseline_bytes);
            assert_eq!(
                sample.peak_above_baseline_bytes,
                sample.peak_bytes.saturating_sub(sample.baseline_bytes)
            );
        });
    }

    /// Purpose: Reclaim abandoned sampler workers on ordinary scope exit, errors, and unwinding.
    /// Expected: Cleanup signals stop and releases all worker references before returning.
    #[test]
    fn process_rss_sampler_drop_joins_abandoned_workers() {
        for exit in ["drop", "early error", "unwind"] {
            assert_sampler_cleanup(ProcessRssSampler::start().unwrap(), exit, |sampler| {
                abandon_sampler(sampler, exit);
            });
        }
    }

    /// Purpose: Preserve explicit worker failure reporting while making implicit cleanup infallible.
    /// Expected: Stop reports worker errors and panics; Drop joins without replacing caller failures.
    #[test]
    fn process_rss_sampler_worker_failures_respect_cleanup_mode() {
        for worker_panics in [false, true] {
            for exit in ["stop", "drop", "unwind"] {
                let peak_bytes = Arc::new(AtomicUsize::new(0));
                let stop = Arc::new(AtomicBool::new(false));
                let worker_peak = Arc::clone(&peak_bytes);
                let worker_stop = Arc::clone(&stop);
                let thread = thread::spawn(move || {
                    let _worker_state = (worker_peak, worker_stop);
                    assert!(!worker_panics, "worker panic");
                    Err(measurement_error("worker error"))
                });
                let sampler = ProcessRssSampler {
                    baseline_bytes: 0,
                    peak_bytes,
                    stop,
                    thread: Some(thread),
                };
                let case = format!("worker_panics={worker_panics}, exit={exit}");
                assert_sampler_cleanup(sampler, &case, |sampler| {
                    if exit == "stop" {
                        let error = sampler.stop().unwrap_err();
                        assert_eq!(
                            error.report().downcast_ref::<RuntimeError>(),
                            Some(&RuntimeError::ProfilingMeasurement),
                            "{case}"
                        );
                        let expected = if worker_panics {
                            "process RSS sampler thread panicked before joining"
                        } else {
                            "worker error"
                        };
                        assert!(format!("{error:?}").contains(expected), "{case}: {error:?}");
                    } else {
                        abandon_sampler(sampler, exit);
                    }
                });
            }
        }
    }

    /// Purpose: Expose unavailable process-memory statistics as a measurement failure.
    /// Expected: The diagnostic identifies the failed RSS read.
    #[test]
    fn process_rss_reader_rejects_unavailable_input() {
        let temp = TempDir::new().unwrap();
        let error = read_process_rss(&temp.path().join("missing-statm"), 4_096).unwrap_err();
        assert_eq!(*error.current_context(), RuntimeError::ProfilingMeasurement);
        assert_eq!(
            error
                .downcast_ref::<crate::error::IoError>()
                .unwrap()
                .kind(),
            std::io::ErrorKind::NotFound
        );
        assert!(format!("{error:?}").contains("failed to read process RSS"));
    }
}
