use std::time::{SystemTime, UNIX_EPOCH};

pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default() // Returns 0 if time went backwards
            .as_millis() as u64
    }
}

#[cfg(test)]
pub struct MockClock {
    pub time: std::sync::Mutex<u64>,
}

#[cfg(test)]
impl MockClock {
    pub fn new(initial_time: u64) -> Self {
        Self {
            time: std::sync::Mutex::new(initial_time),
        }
    }

    pub fn set_time(&self, time: u64) {
        *self.time.lock().unwrap() = time;
    }

    pub fn advance(&self, delta: u64) {
        *self.time.lock().unwrap() += delta;
    }
}

#[cfg(test)]
impl Clock for MockClock {
    fn now_ms(&self) -> u64 {
        *self.time.lock().unwrap()
    }
}