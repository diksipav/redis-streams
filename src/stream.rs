use tokio::sync::Notify;
use std::sync::Arc;
use crate::errors::Result;
use crate::id::{IdGenerator, EntryId};
use crate::clock::Clock;

#[derive(Clone, Debug)]
pub struct Entry {
    pub id: EntryId,
    pub fields: Vec<(Box<str>, Vec<u8>)>,
}

impl Entry {
    pub fn new(id: EntryId, fields: Vec<(String, Vec<u8>)>) -> Self {
        Self {
            id,
            fields: fields.into_iter().map(|(k, v)| (k.into_boxed_str(), v)).collect(),
        }
    }
}

pub struct Stream {
    entries: Vec<Entry>,
    id_generator: IdGenerator,
    notify: Arc<Notify>
}

impl Stream {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            id_generator: IdGenerator::new(),
            notify: Arc::new(Notify::new())
        }
    }

    pub(crate) fn get_notify_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.notify)
    }

    pub fn xadd(
        &mut self,
        id_opt: Option<&str>,
        fields: Vec<(String, Vec<u8>)>,
        clock: &dyn Clock,
    ) -> Result<EntryId> {
        let id = match id_opt {
            Some(explicit_id) => self.id_generator.validate_id(explicit_id)?,
            None => self.id_generator.generate_id(clock.now_ms())
        };
        let entry = Entry::new(id, fields);
        self.entries.push(entry);
        self.notify.notify_waiters();
        Ok(id)
    }

    pub fn xrange(
        &self,
        start: RangeStart,
        end: RangeEnd,
        count: Option<usize>,
    ) -> Vec<Entry> {
        if self.entries.is_empty() {
            return Vec::new();
        }

        let start_idx = match start {
            RangeStart::Start => 0,
            RangeStart::Id(id) => {
                match self.entries.binary_search_by_key(&id, |e| e.id) {
                    Ok(idx) => idx, // index of the entry with id
                    Err(idx) => idx, // index where entry with id would be inserted
                }
            }
        };

        if start_idx >= self.entries.len() {
            return Vec::new();
        }

        self.entries[start_idx..]
        .iter()
        .take_while(|entry| match end {
            RangeEnd::End => true,
            RangeEnd::Id(end_id) => entry.id <= end_id,
        })
        .take(count.unwrap_or(usize::MAX))
        .cloned()
        .collect()
    }

    pub fn xread(&self, id: EntryId, count: Option<usize>) -> Vec<Entry> {
        if self.entries.is_empty() {
            return Vec::new();
        }

        let start_idx = match self.entries.binary_search_by(|e| e.id.cmp(&id)) {
            Ok(idx) => idx + 1,  
            Err(idx) => idx
        };

        if start_idx >= self.entries.len() {
            return Vec::new();
        }

        let end_idx = match count {
            Some(limit) => (start_idx + limit).min(self.entries.len()),
            None => self.entries.len(),
        };

        self.entries[start_idx..end_idx].to_vec()
    }

    pub fn xlen(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn get_latest_id(&self) -> Option<EntryId> {
        self.entries.last().map(|entry| entry.id)
    }
}

#[derive(Clone, Debug)]
pub enum RangeStart {
    Start,
    Id(EntryId),
}

#[derive(Clone, Debug)]
pub enum RangeEnd {
    End,
    Id(EntryId),
}

#[derive(Clone, Copy, Debug)]
pub enum ReadFrom {
    Latest,
    Id(EntryId),
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

    #[test]
    fn test_xadd_auto_id() {
        let mut stream = Stream::new();
        let clock = MockClock::new(1000);
        let id1 = stream.xadd(None, create_test_fields(), &clock).unwrap();
        assert_eq!(id1, EntryId::new(1000, 0));
        let id2 = stream.xadd(None, create_test_fields(), &clock).unwrap();
        assert_eq!(id2, EntryId::new(1000, 1));
        clock.set_time(1001);
        let id3 = stream.xadd(None, create_test_fields(), &clock).unwrap();
        assert_eq!(id3, EntryId::new(1001, 0));
    }

    #[test]
    fn test_xadd_explicit_id() {
        let mut stream = Stream::new();
        let clock = MockClock::new(1000);
        let id1 = EntryId::new(1000, 5);
        let result = stream.xadd(Some("1000-5"), create_test_fields(), &clock).unwrap();
        assert_eq!(result, id1);
        assert!(stream.xadd(Some("1000-4"), create_test_fields(), &clock).is_err());
    }

    #[test]
    fn test_xrange() {
        let mut stream = Stream::new();
        let clock = MockClock::new(1000);
        for i in 0..5 {
            clock.set_time(1000 + i);
            stream.xadd(None, create_test_fields(), &clock).unwrap();
        }
        let all = stream.xrange(RangeStart::Start, RangeEnd::End, None);
        assert_eq!(all.len(), 5);
        let limited = stream.xrange(RangeStart::Start, RangeEnd::End, Some(2));
        assert_eq!(limited.len(), 2);
        let range = stream.xrange(
            RangeStart::Id(EntryId::new(1001, 0)),
            RangeEnd::Id(EntryId::new(1003, 0)),
            None,
        );
        assert_eq!(range.len(), 3);
    }

    #[test]
    fn test_xread_non_blocking() {
        let mut stream = Stream::new();
        let clock = MockClock::new(1000);
        stream.xadd(None, create_test_fields(), &clock).unwrap();
        stream.xadd(None, create_test_fields(), &clock).unwrap();
        let result = stream
            .xread(EntryId::new(1000, 0), Some(1));
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, EntryId::new(1000, 1));
    }

    #[tokio::test]
    async fn test_xread_blocking() {
        use crate::Database;
        use std::sync::Arc;
        use std::time::Duration;
        
        let db = Arc::new(Database::new());
                
        let db_xread = Arc::clone(&db);
        let xread_handle = tokio::spawn(async move {
            db_xread.xread("teststream", "$", None, Some(Duration::from_millis(5000))).await
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let db_xadd = Arc::clone(&db);
        let xadd_handle = tokio::spawn(async move {
            let fields = vec![("field1".to_string(), b"value1".to_vec())];
            let id = db_xadd.xadd("teststream", None, fields).await.unwrap();
            (id, std::time::Instant::now())
        });

        let xread_result = xread_handle.await.unwrap();
        let xadd_result = xadd_handle.await.unwrap();

        assert!(xread_result.is_ok(), "XREAD should succeed");
        let entries = xread_result.unwrap();
        assert_eq!(entries.len(), 1, "XREAD should return one entry");
        assert_eq!(entries[0].id, xadd_result.0);
        assert_eq!(entries[0].fields, vec![(Box::from("field1"), b"value1".to_vec())]);
    }

    #[test]
    fn test_xlen() {
        let mut stream = Stream::new();
        let clock = MockClock::new(1000);
        assert_eq!(stream.xlen(), 0);
        stream.xadd(None, create_test_fields(), &clock).unwrap();
        assert_eq!(stream.xlen(), 1);
        stream.xadd(None, create_test_fields(), &clock).unwrap();
        assert_eq!(stream.xlen(), 2);
    }
}