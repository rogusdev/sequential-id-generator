
use std::time::{SystemTime, UNIX_EPOCH};


pub trait TimeProvider : Clone {
    fn unix_ts_ms (&self) -> i64;
}

#[derive(Debug, Clone)]
pub struct SystemTimeProvider {
}

impl TimeProvider for SystemTimeProvider {
    fn unix_ts_ms (&self) -> i64 {
        let dur = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        ((dur.as_secs() * 1_000) + dur.subsec_millis() as u64) as i64
    }
}

#[derive(Debug, Clone)]
pub struct FixedTimeProvider {
    pub fixed_unix_ts_ms: i64,
}

impl TimeProvider for FixedTimeProvider {
    fn unix_ts_ms (&self) -> i64 {
        self.fixed_unix_ts_ms
    }
}
