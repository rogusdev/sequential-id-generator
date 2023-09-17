
use std::time::{SystemTime, UNIX_EPOCH};


pub trait TimeProvider {
    fn unix_ts_ms (&self) -> i64;
}

pub struct SystemTimeProvider {
}

// declare this and inject it everywhere in your real code paths, with FixedTimeProvider injected in tests:
// static SYSTEM_TIME_PROVIDER: SystemTimeProvider = SystemTimeProvider {};

impl TimeProvider for SystemTimeProvider {
    fn unix_ts_ms (&self) -> i64 {
        let dur = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        ((dur.as_secs() * 1_000) + dur.subsec_millis() as u64) as i64
    }
}

pub struct FixedTimeProvider {
    pub fixed_unix_ts_ms: i64,
}

impl FixedTimeProvider {
    #[allow(dead_code)]
    pub fn set_fixed_unix_ts_ms (&mut self, ms: i64) {
        self.fixed_unix_ts_ms = ms;
    }
}

impl TimeProvider for FixedTimeProvider {
    fn unix_ts_ms (&self) -> i64 {
        self.fixed_unix_ts_ms
    }
}

pub struct ZeroTimeProvider {
}

impl TimeProvider for ZeroTimeProvider {
    fn unix_ts_ms (&self) -> i64 {
        0
    }
}
