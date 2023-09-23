
use std::time::{SystemTime, UNIX_EPOCH};

use dyn_clone::{clone_trait_object, DynClone};


pub trait TimeProvider : DynClone {
    fn unix_ts_ms (&self) -> i64;
}

clone_trait_object!(TimeProvider); // only needed for Box, not Arc?

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct FixedTimeProvider {
    pub fixed_unix_ts_ms: i64,
}

impl FixedTimeProvider {
    pub fn new (fixed_unix_ts_ms: i64) -> Self {
        Self {
            fixed_unix_ts_ms,
        }
    }

    pub fn set (&mut self, ms: i64) {
        self.fixed_unix_ts_ms = ms;
    }

    pub fn add (&mut self, ms: i64) {
        self.fixed_unix_ts_ms += ms;
    }
}

impl TimeProvider for FixedTimeProvider {
    fn unix_ts_ms (&self) -> i64 {
        self.fixed_unix_ts_ms
    }
}

#[derive(Debug, Clone)]
pub struct ZeroTimeProvider {
}

impl TimeProvider for ZeroTimeProvider {
    fn unix_ts_ms (&self) -> i64 {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::{Arc, Mutex};

    // this is so we can change the contents of the time provider while state continues to hold it
    impl TimeProvider for Arc<Mutex<FixedTimeProvider>> {
        fn unix_ts_ms (&self) -> i64 {
            self.lock().unwrap().fixed_unix_ts_ms
        }
    }

    impl FixedTimeProvider {
        pub fn arc_new (fixed_unix_ts_ms: i64) -> Arc<Mutex<Self>> {
            Arc::new(Mutex::new(Self {
                fixed_unix_ts_ms,
            }))
        }

        pub fn arc_set (arc: &Arc<Mutex<Self>>, ms: i64) {
            arc.lock().unwrap().fixed_unix_ts_ms = ms
        }

        pub fn arc_add (arc: &Arc<Mutex<Self>>, ms: i64) {
            arc.lock().unwrap().fixed_unix_ts_ms += ms
        }
    }

    /*
    // in a test...
    let time_provider = FixedTimeProvider::arc_new(123);
    ...
    let time_provider_injected = time_provider.clone();  // pass this to an object to own
    ...
    // assert before
    FixedTimeProvider::arc_add(&time_provider, 1000);
    // assert after
    ... etc
    */
}
