
mod time_provider;
use time_provider::{TimeProvider, SystemTimeProvider};

use std::env;
use std::sync::{Arc, Mutex, MutexGuard};
use std::collections::{BTreeMap, VecDeque};

use axum::{
	routing::get,
	extract::{Path, State},
    response::Json,
	Router,
};

use serde_json::{Value, json};

use lazy_static::lazy_static;


const DEFAULT_PORT: u16 = 3000;
const DEFAULT_MAX: usize = 65535;
const DEFAULT_MIN: usize = 1;
const DEFAULT_TIMEOUT: i64 = 3000;

const ERROR_CODE_NO_ID_AVAILBLE: usize = 1;
const ERROR_CODE_ID_EXPIRED: usize = 2;
const ERROR_CODE_ID_NONEXISTENT: usize = 3;


lazy_static! {
    static ref ERROR_CODE_MSGS: BTreeMap<usize, &'static str> = vec![
        (ERROR_CODE_NO_ID_AVAILBLE, "No id available!"),
        (ERROR_CODE_ID_EXPIRED, "Id expired!"),
        (ERROR_CODE_ID_NONEXISTENT, "Id nonexistent!"),
    ].iter().copied().collect::<BTreeMap<_, _>>();
}

static SYSTEM_TIME_PROVIDER: SystemTimeProvider = SystemTimeProvider {};

struct AppState<'a> {
    timeout: i64,
    expires: BTreeMap<usize, i64>,
    availables: VecDeque<usize>,
    time_provider: &'a(dyn TimeProvider + Send + Sync),
}

fn env_var_parse<T: std::str::FromStr> (name: &str, default: T) -> T {
    match env::var(name) {
        Ok(s) => s.parse::<T>().unwrap_or(default),
        _ => default
    }
}

fn json_success (id: usize, exp: i64) -> Json<Value> {
    Json(json!({
        "id": id,
        "exp": exp,
    }))
}

fn json_error (code: usize) -> Json<Value> {
    Json(json!({
        "error": {
            "code": code,
            "msg": ERROR_CODE_MSGS.get(&code),
        }
    }))
}

fn clear_expired (state: &mut MutexGuard<AppState>) -> usize {
    let now = state.time_provider.unix_ts_ms();
    let mut expireds = vec![];
    for (&id, &expire) in state.expires.iter() {
        if expire <= now {
            expireds.push(id);
        }
    }
    for id in expireds.iter() {
        state.expires.remove(id);
        state.availables.push_back(*id);
    }
    // TODO: use https://doc.rust-lang.org/stable/std/collections/struct.BTreeMap.html#method.extract_if
    // let count_old = availables.len();
    // for (id, expire) in expires.extract_if(|&id, &mut expire| expire < now) {
    //     availables.push_back(id);
    // }
    // availables.len() - count_old
    expireds.len()
}

fn get_next_impl (mut state: MutexGuard<AppState>) -> Result<(usize, i64), usize> {
    clear_expired(&mut state);

    if let Some(id_next) = state.availables.pop_front() {
        let now = state.time_provider.unix_ts_ms();
        let expire = now + state.timeout;
        state.expires.insert(id_next, expire);
        Ok((id_next, expire))
    } else {
        Err(ERROR_CODE_NO_ID_AVAILBLE)
    }
}

async fn get_next (State(state): State<Arc<Mutex<AppState<'_>>>>) -> Json<Value> {
    let state = state.lock().expect("Poisoned get_next_impl mutex");
    match get_next_impl(state) {
        Ok((id_next, expire)) => json_success(id_next, expire),
        Err(code) => json_error(code)
    }
}

fn get_heartbeat_impl (id: usize, mut state: MutexGuard<AppState>) -> Result<i64, usize> {
    if let Some(&expire) = state.expires.get(&id) {
        let now = state.time_provider.unix_ts_ms();
        if expire > now {
            let expire = now + state.timeout;
            state.expires.insert(id, expire);
            Ok(expire)
        } else {
            // Connecting client should take this error and request a new (next) id
            // TODO: warn loudly! this means it potentially used a shared id for some period
            Err(ERROR_CODE_ID_EXPIRED)
        }
    } else {
        Err(ERROR_CODE_ID_NONEXISTENT)
    }
}

async fn get_heartbeat (Path(id): Path<usize>, State(state): State<Arc<Mutex<AppState<'_>>>>) -> Json<Value> {
    let state = state.lock().expect("Poisoned get_heartbeat mutex");
    match get_heartbeat_impl(id, state) {
        Ok(expire) => json_success(id, expire),
        Err(code) => json_error(code)
    }
}


#[tokio::main]
async fn main() {
    let port = env_var_parse("PORT", DEFAULT_PORT);
    let id_max = env_var_parse("MAX", DEFAULT_MAX);
    let id_min = env_var_parse("MIN", DEFAULT_MIN);
    let timeout = env_var_parse("TIMEOUT", DEFAULT_TIMEOUT);

    let state = Arc::new(Mutex::new(AppState {
        timeout,
        expires: BTreeMap::new(),
        availables: VecDeque::from((id_min..=id_max).collect::<Vec<usize>>()),
        time_provider: &SYSTEM_TIME_PROVIDER,
    }));

    let app = Router::new()
        .route("/next", get(get_next))
        .route("/heartbeat/:id", get(get_heartbeat))
        .with_state(state);

    axum::Server::bind(&format!("0.0.0.0:{}", port).parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}


#[cfg(test)]
mod tests {
    use std::ops::Range;

    use super::*;
    use time_provider::{FixedTimeProvider, ZeroTimeProvider};

    const TEST_TIMEOUT: i64 = 2000;

    // this is so we can change the contents of the time provider while state continues to hold it
    impl TimeProvider for Arc<Mutex<FixedTimeProvider>> {
        fn unix_ts_ms (&self) -> i64 {
            self.lock().unwrap().fixed_unix_ts_ms
        }
    }

    fn vec_to_btree<T: Ord, U> (v: Vec<(T, U)>) -> BTreeMap<T, U> {
        v.into_iter()
            .map(|x| (x.0, x.1))
            .collect::<BTreeMap<_, _>>()
    }

    fn availables_from_range (r: Range<usize>) -> VecDeque<usize> {
        VecDeque::from(r.collect::<Vec<usize>>())
    }

    #[test]
    fn get_next_impl_err () {
        let time_provider = FixedTimeProvider {
            fixed_unix_ts_ms: 123,
        };
        let now = time_provider.unix_ts_ms();
        let expires = vec_to_btree(vec![
            (1, now + TEST_TIMEOUT),
            (2, now + TEST_TIMEOUT),
        ]);
        let state = Arc::new(Mutex::new(AppState {
            timeout: TEST_TIMEOUT,
            expires,
            availables: availables_from_range(3..3),
            time_provider: &time_provider,
        }));
        let result = get_next_impl(state.lock().unwrap());
        assert_eq!(result, Err(ERROR_CODE_NO_ID_AVAILBLE));
    }

    #[test]
    fn get_next_impl_ok () {
        let time_provider = FixedTimeProvider {
            fixed_unix_ts_ms: 123,
        };
        let now = time_provider.unix_ts_ms();
        let expires = vec_to_btree(vec![
            (1, now + TEST_TIMEOUT),
            (2, now + TEST_TIMEOUT),
        ]);
        let state = Arc::new(Mutex::new(AppState {
            timeout: TEST_TIMEOUT,
            expires,
            availables: availables_from_range(3..4),
            time_provider: &time_provider,
        }));
        let result = get_next_impl(state.lock().unwrap());
        assert_eq!(result, Ok((3, now + TEST_TIMEOUT)));
    }

    #[test]
    fn get_next_impl_expireds () {
        let time_provider = Arc::new(Mutex::new(FixedTimeProvider {
            fixed_unix_ts_ms: 123,
        }));
        let now = time_provider.lock().unwrap().unix_ts_ms();
        let expires = vec_to_btree(vec![
            (1, now - TEST_TIMEOUT),
            (2, now + TEST_TIMEOUT),
        ]);
        let time_provider_state = time_provider.clone();
        let state = Arc::new(Mutex::new(AppState {
            timeout: TEST_TIMEOUT,
            expires,
            availables: availables_from_range(3..4),
            time_provider: &time_provider_state,
        }));

        {
            let result = clear_expired(&mut state.lock().unwrap());
            assert_eq!(result, 1);

            // expires has removed the old entry
            let state = state.lock().unwrap();
            assert_eq!(state.expires, vec_to_btree(vec![(2, now + TEST_TIMEOUT)]));
            // and now the old id is at the end of the queue
            assert_eq!(state.availables, VecDeque::from(vec![3,1]));
        }

        {
            time_provider.lock().unwrap().fixed_unix_ts_ms += TEST_TIMEOUT / 2;
            let result = get_next_impl(state.lock().unwrap());
            assert_eq!(result, Ok((3, now + TEST_TIMEOUT / 2 + TEST_TIMEOUT)));
            let result2 = get_next_impl(state.lock().unwrap());
            assert_eq!(result2, Ok((1, now + TEST_TIMEOUT / 2 + TEST_TIMEOUT)));
            let result3 = get_next_impl(state.lock().unwrap());
            assert_eq!(result3, Err(ERROR_CODE_NO_ID_AVAILBLE));
        }

        {
            time_provider.lock().unwrap().fixed_unix_ts_ms += TEST_TIMEOUT / 2;
            let result = get_next_impl(state.lock().unwrap());
            assert_eq!(result, Ok((2, now + TEST_TIMEOUT + TEST_TIMEOUT)));
        }
    }

    #[test]
    fn get_heartbeat_impl_missing () {
        let time_provider = ZeroTimeProvider {};
        let state = Arc::new(Mutex::new(AppState {
            timeout: TEST_TIMEOUT,
            expires: BTreeMap::new(),
            availables: availables_from_range(1..3),
            time_provider: &time_provider,
        }));
        let result = get_heartbeat_impl(1, state.lock().unwrap());
        assert_eq!(result, Err(ERROR_CODE_ID_NONEXISTENT));
    }

    #[test]
    fn get_heartbeat_impl_ok () {
        let mut time_provider = FixedTimeProvider {
            fixed_unix_ts_ms: 123,
        };
        let now = time_provider.unix_ts_ms();
        let expires = vec_to_btree(vec![
            (1, now + TEST_TIMEOUT),
            (2, now + TEST_TIMEOUT),
        ]);
        time_provider.fixed_unix_ts_ms += TEST_TIMEOUT / 2;
        let state = Arc::new(Mutex::new(AppState {
            timeout: TEST_TIMEOUT,
            expires,
            availables: availables_from_range(3..3),
            time_provider: &time_provider,
        }));
        let result = get_heartbeat_impl(1, state.lock().unwrap());
        assert_eq!(result, Ok(now + TEST_TIMEOUT + TEST_TIMEOUT / 2));
    }

    #[test]
    fn get_heartbeat_impl_expired () {
        let mut time_provider = FixedTimeProvider {
            fixed_unix_ts_ms: 123,
        };
        let now = time_provider.unix_ts_ms();
        let expires = vec_to_btree(vec![
            (1, now + TEST_TIMEOUT),
        ]);
        time_provider.fixed_unix_ts_ms += TEST_TIMEOUT * 2;
        let state = Arc::new(Mutex::new(AppState {
            timeout: TEST_TIMEOUT,
            expires,
            availables: availables_from_range(2..3),
            time_provider: &time_provider,
        }));
        let result = get_heartbeat_impl(1, state.lock().unwrap());
        assert_eq!(result, Err(ERROR_CODE_ID_EXPIRED));
    }
}
