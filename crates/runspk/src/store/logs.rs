//! Byte cursors into the existing, capped log prefixes; fetching never modifies logs.
use super::Store;
use crate::model::{LOG_LIMIT, Logs};
use anyhow::Result;
use serde::{Deserialize, Serialize};

/// Offsets and tail are UTF-8 byte counts, independently for each stream.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct LogQuery {
    pub stdout_offset: usize,
    pub stderr_offset: usize,
    pub attempt_id: Option<String>,
    /// Per-stream byte budget: 64 KiB by default, capped at 256 KiB.
    /// A budget smaller than the next UTF-8 character returns no bytes.
    pub limit: Option<usize>,
    /// Start within the last N retained bytes, overriding both offsets.
    pub tail: Option<usize>,
}

/// Offsets, next cursors, and totals count retained UTF-8 bytes, not raw output.
/// A capped prefix supplies only a lower-bound view of output. The worker captures
/// at most LOG_LIMIT raw bytes per stream; lossy decoding may expand invalid UTF-8,
/// so retained byte totals do not measure the original raw output size.
/// Truncation flags mean *possible* capture truncation when retained bytes reach
/// LOG_LIMIT; neither the exact original size nor actual truncation is measured.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LogChunk {
    pub stdout: String,
    pub stderr: String,
    pub attempt_id: Option<String>,
    pub stdout_offset: usize,
    pub stderr_offset: usize,
    pub stdout_next: usize,
    pub stderr_next: usize,
    pub stdout_total: usize,
    pub stderr_total: usize,
    pub reset: bool,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

impl Store {
    pub fn log_chunk(&self, id: &str, query: &LogQuery) -> Result<LogChunk> {
        Ok(chunk(self.logs(id)?, query))
    }
}

fn chunk(logs: Logs, query: &LogQuery) -> LogChunk {
    let reset = (query.attempt_id.is_some() && query.attempt_id != logs.attempt_id)
        || query.stdout_offset > logs.stdout.len()
        || query.stderr_offset > logs.stderr.len();
    let limit = query.limit.unwrap_or(64 * 1024).min(256 * 1024);
    let (stdout_offset, stdout_next) =
        bounds(&logs.stdout, query.stdout_offset, query, reset, limit);
    let (stderr_offset, stderr_next) =
        bounds(&logs.stderr, query.stderr_offset, query, reset, limit);
    LogChunk {
        stdout: logs.stdout[stdout_offset..stdout_next].into(),
        stderr: logs.stderr[stderr_offset..stderr_next].into(),
        attempt_id: logs.attempt_id,
        stdout_offset,
        stderr_offset,
        stdout_next,
        stderr_next,
        stdout_total: logs.stdout.len(),
        stderr_total: logs.stderr.len(),
        reset,
        stdout_truncated: logs.stdout.len() >= LOG_LIMIT,
        stderr_truncated: logs.stderr.len() >= LOG_LIMIT,
    }
}

fn bounds(
    text: &str,
    offset: usize,
    query: &LogQuery,
    reset: bool,
    limit: usize,
) -> (usize, usize) {
    let mut start = if let Some(tail) = query.tail {
        let mut start = text.len().saturating_sub(tail);
        // A tail must stay within the requested byte window.
        while !text.is_char_boundary(start) {
            start += 1;
        }
        start
    } else if reset {
        0
    } else {
        offset
    };
    // Round arbitrary byte cursors down so a character is never skipped.
    while !text.is_char_boundary(start) {
        start -= 1;
    }
    let mut end = start.saturating_add(limit).min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    (start, end)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn logs(stdout: &str, stderr: &str) -> Logs {
        Logs {
            stdout: stdout.into(),
            stderr: stderr.into(),
            attempt_id: Some("attempt".into()),
        }
    }

    #[test]
    fn byte_cursors_are_independent_and_resume_without_repeating() {
        let first = chunk(
            logs("abcdef", "12345"),
            &LogQuery {
                stdout_offset: 1,
                stderr_offset: 2,
                limit: Some(2),
                ..Default::default()
            },
        );
        assert_eq!((&*first.stdout, &*first.stderr), ("bc", "34"));
        assert_eq!((first.stdout_offset, first.stderr_offset), (1, 2));
        assert_eq!((first.stdout_next, first.stderr_next), (3, 4));
        assert_eq!((first.stdout_total, first.stderr_total), (6, 5));
        assert!(!first.reset);
        let next = chunk(
            logs("abcdef", "12345"),
            &LogQuery {
                stdout_offset: first.stdout_next,
                stderr_offset: first.stderr_next,
                attempt_id: first.attempt_id,
                ..Default::default()
            },
        );
        assert_eq!((&*next.stdout, &*next.stderr), ("def", "5"));
        let end = chunk(
            logs("abcdef", "12345"),
            &LogQuery {
                stdout_offset: 6,
                stderr_offset: 5,
                ..Default::default()
            },
        );
        assert!(end.stdout.is_empty() && end.stderr.is_empty() && !end.reset);
    }

    #[test]
    fn multibyte_boundaries_are_safe_and_offsets_describe_actual_slice() {
        let result = chunk(
            logs("aé🦀z", "é"),
            &LogQuery {
                stdout_offset: 2,
                stderr_offset: 1,
                limit: Some(5),
                ..Default::default()
            },
        );
        assert_eq!((&*result.stdout, &*result.stderr), ("é", "é"));
        assert_eq!((result.stdout_offset, result.stdout_next), (1, 3));
        assert_eq!((result.stderr_offset, result.stderr_next), (0, 2));
        let tiny = chunk(
            logs("🦀", ""),
            &LogQuery {
                limit: Some(1),
                ..Default::default()
            },
        );
        assert!(tiny.stdout.is_empty());
        assert_eq!(tiny.stdout_next, 0);
    }

    #[test]
    fn every_cursor_and_small_budget_produce_bounded_valid_utf8() {
        let text = "aé🦀z";
        for offset in 0..=text.len() + 1 {
            for limit in 0..=text.len() + 1 {
                for tail in [None, Some(0), Some(1), Some(4), Some(usize::MAX)] {
                    let result = chunk(
                        logs(text, text),
                        &LogQuery {
                            stdout_offset: offset,
                            stderr_offset: offset,
                            limit: Some(limit),
                            tail,
                            ..Default::default()
                        },
                    );
                    assert!(result.stdout.len() <= limit);
                    assert_eq!(
                        result.stdout,
                        text[result.stdout_offset..result.stdout_next]
                    );
                    assert_eq!(result.stdout, result.stderr);
                    if let Some(tail) = tail {
                        assert!(text.len() - result.stdout_offset <= tail);
                    }
                }
            }
        }
    }

    #[test]
    fn attempt_changes_and_stale_offsets_reset_both_streams() {
        for query in [
            LogQuery {
                stdout_offset: 1,
                stderr_offset: 1,
                attempt_id: Some("old".into()),
                ..Default::default()
            },
            LogQuery {
                stdout_offset: usize::MAX,
                stderr_offset: 1,
                ..Default::default()
            },
            LogQuery {
                stdout_offset: 1,
                stderr_offset: 4,
                ..Default::default()
            },
        ] {
            let result = chunk(logs("abc", "xyz"), &query);
            assert!(result.reset);
            assert_eq!((result.stdout_offset, result.stderr_offset), (0, 0));
            assert_eq!((&*result.stdout, &*result.stderr), ("abc", "xyz"));
        }
        let empty = chunk(
            Logs::default(),
            &LogQuery {
                attempt_id: Some("old".into()),
                ..Default::default()
            },
        );
        assert!(empty.reset);
        assert_eq!(empty.attempt_id, None);
    }

    #[test]
    fn tail_is_bytes_and_respects_utf8_and_chunk_limit() {
        let result = chunk(
            logs("first\né🦀z", "123456"),
            &LogQuery {
                tail: Some(5),
                limit: Some(4),
                stdout_offset: 1,
                ..Default::default()
            },
        );
        assert_eq!((&*result.stdout, &*result.stderr), ("🦀", "2345"));
        assert_eq!((result.stdout_offset, result.stdout_next), (8, 12));
        let split = chunk(
            logs("aé🦀z", ""),
            &LogQuery {
                tail: Some(4),
                ..Default::default()
            },
        );
        assert_eq!(split.stdout, "z");
        assert_eq!(split.stdout_offset, 7);
        let empty = chunk(
            logs("abc", "xyz"),
            &LogQuery {
                tail: Some(0),
                ..Default::default()
            },
        );
        assert!(empty.stdout.is_empty() && empty.stderr.is_empty());
        let all = chunk(
            logs("abc", "xyz"),
            &LogQuery {
                tail: Some(usize::MAX),
                ..Default::default()
            },
        );
        assert_eq!((&*all.stdout, &*all.stderr), ("abc", "xyz"));
    }

    #[test]
    fn limits_and_potential_capture_truncation_are_bounded() {
        let output = "x".repeat(LOG_LIMIT);
        for (limit, expected) in [
            (None, 64 * 1024),
            (Some(usize::MAX), 256 * 1024),
            (Some(0), 0),
        ] {
            let result = chunk(
                logs(&output, "short"),
                &LogQuery {
                    limit,
                    ..Default::default()
                },
            );
            assert_eq!(result.stdout.len(), expected);
            assert_eq!(result.stdout_total, LOG_LIMIT);
            assert!(result.stdout_truncated);
            assert!(!result.stderr_truncated);
        }
        let result = chunk(logs(&"x".repeat(LOG_LIMIT - 1), ""), &LogQuery::default());
        assert!(!result.stdout_truncated);
        let query: LogQuery = serde_json::from_str("{}").unwrap();
        assert_eq!(query.stdout_offset, 0);
        assert_eq!(query.stderr_offset, 0);
        assert!(query.attempt_id.is_none() && query.limit.is_none() && query.tail.is_none());
    }

    #[test]
    fn store_checks_job_existence_and_supports_jobs_without_attempts() {
        let mut store = Store::open(":memory:").unwrap();
        assert!(store.log_chunk("missing", &LogQuery::default()).is_err());
        let spec = serde_json::from_value(serde_json::json!({
            "command": ["true"], "resources": {"cpu": 1, "mem_mib": 64},
            "timeout_secs": 60, "trusted": true
        }))
        .unwrap();
        let job = store.submit(spec).unwrap();
        let result = store.log_chunk(&job.id, &LogQuery::default()).unwrap();
        assert!(result.stdout.is_empty() && result.stderr.is_empty());
        assert_eq!(result.attempt_id, None);
        assert_eq!((result.stdout_total, result.stderr_total), (0, 0));
        assert!(!result.reset);
    }
}
