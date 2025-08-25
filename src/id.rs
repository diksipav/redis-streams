use crate::errors::Error;
use std::fmt;
use std::str::FromStr;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct EntryId {
    pub ms: u64,
    pub seq: u64,
}

impl EntryId {
    pub fn new(ms: u64, seq: u64) -> Self {
        Self { ms, seq }
    }

    pub fn zero() -> Self {
        Self { ms: 0, seq: 0 }
    }

    pub fn is_valid(&self) -> bool {
        !(*self == Self::zero())
    }
}

impl fmt::Display for EntryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

impl FromStr for EntryId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() != 2 {
            return Err(Error::BadIdFormat);
        }

        let ms = parts[0].parse::<u64>().map_err(|_| Error::BadIdFormat)?;
        let seq = parts[1].parse::<u64>().map_err(|_| Error::BadIdFormat)?;

        let id = EntryId::new(ms, seq);

        Ok(id)
    }
}

pub struct IdGenerator {
    last_id: EntryId,
}

impl IdGenerator {
    pub fn new() -> Self {
        Self {
            last_id: EntryId::new(0, 0),
        }
    }

    pub fn generate_id(&mut self, current_ms: u64) -> EntryId {
        // Uses last recorded timestamp with incremented sequence when system time decreases.
        let ms = current_ms.max(self.last_id.ms);
        let seq = if ms == self.last_id.ms {
            self.last_id.seq + 1
        } else {
            0
        };

        let id = EntryId::new(ms, seq);
        self.last_id = id;
        id
    }

    pub fn validate_id(&mut self, id_str: &str) -> Result<EntryId, Error> {
        let id = EntryId::from_str(id_str)?;

        if !id.is_valid() {
            return Err(Error::IdShoulHavePositiveTimestamp);
        }

        if id <= self.last_id {
            return Err(Error::IdNotMonotonic);
        }

        self.last_id = id;
        Ok(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_id_ordering() {
        let id1 = EntryId::new(1000, 0);
        let id2 = EntryId::new(1000, 1);
        let id3 = EntryId::new(1001, 0);

        assert!(id1 < id2);
        assert!(id2 < id3);
        assert!(id1 < id3);
    }

    #[test]
    fn test_stream_id_parsing() {
        assert_eq!(EntryId::from_str("1000-5").unwrap(), EntryId::new(1000, 5));
        assert_eq!(EntryId::from_str("0-0").unwrap(), EntryId::zero());
        assert!(EntryId::from_str("invalid").is_err());
    }

    #[test]
    fn test_generate_id() {
        let mut generator = IdGenerator::new();

        let id1 = generator.generate_id(1000);
        assert_eq!(id1, EntryId::new(1000, 0));

        let id2 = generator.generate_id(1000);
        assert_eq!(id2, EntryId::new(1000, 1));

        let id3 = generator.generate_id(1001);
        assert_eq!(id3, EntryId::new(1001, 0));

        let id4 = generator.generate_id(999);
        assert_eq!(id4, EntryId::new(1001, 1));
    }

    #[test]
    fn test_validate_id() {
        let mut generator = IdGenerator::new();

        let id1 = generator.validate_id("1000-0").unwrap();
        assert_eq!(id1, EntryId::new(1000, 0));

        assert!(generator.validate_id("0-0").is_err());
        assert!(generator.validate_id("999-0").is_err());
        assert!(generator.validate_id("1000-0").is_err());

        let id2 = generator.validate_id("1000-1").unwrap();
        assert_eq!(id2, EntryId::new(1000, 1));
    }
}
