use crate::clock::{Clock, SystemClock};
use crate::errors::{Error, Result};
use crate::id::EntryId;
use crate::stream::{Entry, RangeEnd, RangeStart, Stream};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

pub struct Database<C: Clock> {
    streams: RwLock<HashMap<String, Arc<RwLock<Stream>>>>,
    clock: C,
}

impl Database<SystemClock> {
    pub fn new() -> Self {
        Self {
            streams: RwLock::new(HashMap::new()),
            clock: SystemClock,
        }
    }
}

impl<C: Clock> Database<C> {
    #[cfg(test)]
    pub fn with_clock(clock: C) -> Self {
        Self {
            streams: RwLock::new(HashMap::new()),
            clock: clock,
        }
    }

    pub async fn xadd(
        &self,
        key: &str,
        entry_id: Option<&str>,
        fields: Vec<(String, Vec<u8>)>,
    ) -> Result<EntryId> {
        if fields.is_empty() {
            return Err(Error::InvalidArgs);
        }
        let mut streams_guard = self.streams.write().await;
        let stream_handle = streams_guard
            .entry(key.to_string())
            .or_insert(Arc::new(RwLock::new(Stream::new())));

        let mut stream_guard = stream_handle.write().await;
        stream_guard.xadd(entry_id, fields, &self.clock)
    }

    pub async fn xrange(
        &self,
        key: &str,
        start: RangeStart,
        end: RangeEnd,
        count: Option<usize>,
    ) -> Result<Vec<Entry>> {
        let streams_guard = self.streams.read().await;
        let stream_handle = streams_guard.get(key).ok_or(Error::StreamNotFound)?;
        let stream_guard = stream_handle.read().await;
        Ok(stream_guard.xrange(start, end, count))
    }

    pub async fn xread(
        &self,
        key: &str,
        from: &str,
        count: Option<usize>,
        block: Option<Duration>,
    ) -> Result<Vec<Entry>> {
        let stream_handle = match block {
            Some(_) => {
                let mut streams_guard = self.streams.write().await;
                streams_guard
                    .entry(key.to_string())
                    .or_insert_with(|| Arc::new(RwLock::new(Stream::new())))
                    .clone()
            }
            None => {
                let streams_guard = self.streams.read().await;
                streams_guard
                    .get(key)
                    .cloned()
                    .ok_or(Error::StreamNotFound)?
            }
        };

        let start_id = match from {
            "$" => {
                let stream_guard = stream_handle.read().await;
                stream_guard.get_latest_id().unwrap_or(EntryId::zero())
            }
            "0" | "0-0" => EntryId::zero(),
            _ => match EntryId::from_str(from) {
                Ok(id) => id,
                Err(_) => return Err(Error::BadIdFormat),
            },
        };

        if let Some(timeout) = block {
            // Blocking case
            let deadline = tokio::time::Instant::now() + timeout;

            let notify_handle = {
                let stream_guard = stream_handle.read().await;
                stream_guard.get_notify_handle()
            };
            let notified = notify_handle.notified();
            tokio::pin!(notified);

            loop {
                let result = {
                    let stream_guard = stream_handle.read().await;
                    stream_guard.xread(start_id, count)
                };

                if !result.is_empty() {
                    return Ok(result);
                }

                let remaining = deadline.checked_duration_since(tokio::time::Instant::now());
                if remaining.is_none() {
                    return Ok(Vec::new()); // Return empty on timeout as in Redis
                }

                // Wait for notification or timeout
                match tokio::time::timeout(remaining.unwrap(), notified.as_mut()).await {
                    Ok(_) => {
                        notified.set(notify_handle.notified());
                    }
                    Err(_) => return Ok(Vec::new()),
                }
            }
        } else {
            // Non-blocking case
            let stream_guard = stream_handle.read().await;
            Ok(stream_guard.xread(start_id, count))
        }
    }

    pub async fn xlen(&self, key: &str) -> Result<usize> {
        let streams_guard = self.streams.read().await;
        let stream_handle = streams_guard.get(key).ok_or(Error::StreamNotFound)?;
        let stream_guard = stream_handle.read().await;
        Ok(stream_guard.xlen())
    }

    pub async fn list_streams(&self) -> Vec<String> {
        let streams = self.streams.read().await;
        streams.keys().cloned().collect()
    }

    #[cfg(test)]
    pub async fn stream_exists(&self, key: &str) -> bool {
        let streams = self.streams.read().await;
        streams.contains_key(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MockClock;

    fn create_test_fields() -> Vec<(String, Vec<u8>)> {
        vec![
            ("user".to_string(), b"bob".to_vec()),
            ("action".to_string(), b"login".to_vec()),
        ]
    }

    #[tokio::test]
    async fn test_xadd_creates_stream() {
        let db = Database::new();
        assert!(!db.stream_exists("mystream").await);
        let id = db
            .xadd("mystream", None, create_test_fields())
            .await
            .unwrap();
        assert!(id.ms > 0);
        assert_eq!(id.seq, 0);
        assert!(db.stream_exists("mystream").await);
    }

    #[tokio::test]
    async fn test_xlen_nonexistent_stream() {
        let db = Database::new();
        assert!(db.xlen("nonexistent").await.is_err());
    }

    #[tokio::test]
    async fn test_xrange_nonexistent_stream() {
        let db = Database::new();
        let result = db
            .xrange("nonexistent", RangeStart::Start, RangeEnd::End, None)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_multiple_streams() {
        let clock = MockClock::new(1000);
        let db = Database::with_clock(clock);
        db.xadd("stream1", None, create_test_fields())
            .await
            .unwrap();
        db.xadd("stream2", None, create_test_fields())
            .await
            .unwrap();
        assert_eq!(db.xlen("stream1").await.unwrap(), 1);
        assert_eq!(db.xlen("stream2").await.unwrap(), 1);
        let streams = db.list_streams().await;
        assert_eq!(streams.len(), 2);
        assert!(streams.contains(&"stream1".to_string()));
        assert!(streams.contains(&"stream2".to_string()));
    }

    #[tokio::test]
    async fn test_xread_nonexistent_stream() {
        let db = Database::new();
        let result = db.xread("nonexistent", "$", None, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_concurrent_xadd_xread() {
        let db = Arc::new(Database::new());

        // Create stream first with an initial entry
        db.xadd("mystream", None, create_test_fields())
            .await
            .unwrap();

        let db_xread = Arc::clone(&db);
        let xread_handle = tokio::spawn(async move {
            let result = db_xread
                .xread("mystream", "$", None, Some(Duration::from_millis(5000)))
                .await;
            (result, std::time::Instant::now())
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let db_xadd = Arc::clone(&db);
        let xadd_handle = tokio::spawn(async move {
            let fields = vec![("field1".to_string(), b"value1".to_vec())];
            let result = db_xadd.xadd("mystream", None, fields).await;
            (result, std::time::Instant::now())
        });

        let (xread_result, xread_time) = xread_handle.await.unwrap();
        let (xadd_result, xadd_time) = xadd_handle.await.unwrap();

        assert!(
            xread_result.is_ok(),
            "XREAD should succeed, got error: {:?}",
            xread_result.as_ref().err()
        );
        let entries = xread_result.unwrap();
        assert_eq!(entries.len(), 1, "XREAD should return one entry");
        assert_eq!(entries[0].id, xadd_result.unwrap());
        assert_eq!(
            entries[0].fields,
            vec![(Box::from("field1"), b"value1".to_vec())]
        );
        assert!(xread_time >= xadd_time, "XREAD completes after XADD");
    }
}
