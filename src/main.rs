
mod time_provider;

use time_provider::{TimeProvider, SystemTimeProvider};

use std::env;

use axum::{
	routing::get,
	extract::{Path, State},
    response::Json,
	Router,
};

use serde_json::{Value, json};

use std::collections::{BTreeMap, VecDeque};

use lazy_static::lazy_static;


const DEFAULT_PORT: u16 = 3000;
const DEFAULT_MAX: usize = 65535;
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


#[derive(Clone)]
struct AppState {
    timeout: i64,
    expires: BTreeMap<usize, i64>,
    availables: VecDeque<usize>,
    time_provider: Box<dyn TimeProvider + Send + Sync>,
}

fn env_var_parse<T: std::str::FromStr> (name: &str, default: T) -> T {
    match env::var(name) {
        Ok(s) => s.parse::<T>().unwrap_or(default),
        _ => default
    }
}

fn json_error (code: usize) -> Json<Value> {
    Json(json!({
        "error": {
            "code": code,
            "msg": ERROR_CODE_MSGS.get(&code),
        }
    }))
}

fn clear_expired (state: &mut AppState) -> usize {
    let mut expireds = vec![];
    let now = state.time_provider.unix_ts_ms();
    for (&id, &expire) in state.expires.iter() {
        if expire < now {
            expireds.push(id);
        }
    }
    for &id in expireds.iter() {
        state.expires.remove(&id);
        state.availables.push_back(id);
    }
    expireds.len()
}

fn get_next_impl (state: &mut AppState) -> Result<(usize, i64), usize> {
    clear_expired(state);
    if let Some(id_next) = state.availables.pop_front() {
        let expiry = state.time_provider.unix_ts_ms() + state.timeout;
        state.expires.insert(id_next, expiry);
        Ok((id_next, expiry))
    } else {
        Err(ERROR_CODE_NO_ID_AVAILBLE)
    }
}

async fn get_next (State(mut state): State<AppState>) -> Json<Value> {
    match get_next_impl(&mut state) {
        Ok((id_next, expiry)) => Json(json!({
            "id": id_next,
            "exp": expiry,
        })),
        Err(code) => json_error(code)
    }
}

fn get_heartbeat_impl (id: usize, state: &mut AppState) -> Result<i64, usize> {
    if let Some(&expiry) = state.expires.get(&id) {
        let now = state.time_provider.unix_ts_ms();
        if expiry > now {
            let expiry = now + state.timeout;
            state.expires.insert(id, expiry);
            Ok(expiry)
        } else {
            state.expires.remove(&id);
            state.availables.push_back(id);
            // TODO: warn loudly! this means it potentially used a shared id for some period
            Err(ERROR_CODE_ID_EXPIRED)
        }
    } else {
        Err(ERROR_CODE_ID_NONEXISTENT)
    }
}

async fn get_heartbeat (Path(id): Path<usize>, State(mut state): State<AppState>) -> Json<Value> {
    match get_heartbeat_impl(id, &mut state) {
        Ok(expiry) => Json(json!({
            "id": id,
            "exp": expiry,
        })),
        Err(code) => json_error(code)
    }
}


#[tokio::main]
async fn main() {
    let port = env_var_parse("PORT", DEFAULT_PORT);
    let id_max = env_var_parse("MAX", DEFAULT_MAX);
    let timeout = env_var_parse("TIMEOUT", DEFAULT_TIMEOUT);

    let state = AppState {
        timeout,
        expires: BTreeMap::new(),
        availables: VecDeque::from((1..id_max).collect::<Vec<usize>>()),
        time_provider: Box::new(SystemTimeProvider {}),
    };

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
    use super::*;
    use time_provider::{FixedTimeProvider, ZeroTimeProvider};

    const TEST_TIMEOUT: i64 = 2000;

    fn vec_to_btree<T: Ord, U> (v: Vec<(T, U)>) -> BTreeMap<T, U> {
        v.into_iter()
            .map(|x| (x.0, x.1))
            .collect::<BTreeMap<_, _>>()
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
        let mut state = AppState {
            timeout: TEST_TIMEOUT,
            expires,
            availables: VecDeque::from((3..3).collect::<Vec<usize>>()),
            time_provider: Box::new(ZeroTimeProvider {}),
        };
        let result = get_next_impl(&mut state);
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
        let mut state = AppState {
            timeout: TEST_TIMEOUT,
            expires,
            availables: VecDeque::from((3..4).collect::<Vec<usize>>()),
            time_provider: Box::new(time_provider),
        };
        let result = get_next_impl(&mut state);
        assert_eq!(result, Ok((3, now + TEST_TIMEOUT)));
    }

    #[test]
    fn get_next_impl_expireds () {
        let time_provider = FixedTimeProvider {
            fixed_unix_ts_ms: 123,
        };
        let now = time_provider.unix_ts_ms();
        let expires = vec_to_btree(vec![
            (1, now - TEST_TIMEOUT),
            (2, now + TEST_TIMEOUT),
        ]);
        let mut state = AppState {
            timeout: TEST_TIMEOUT,
            expires,
            availables: VecDeque::from((3..4).collect::<Vec<usize>>()),
            time_provider: Box::new(time_provider),
        };
        let result = clear_expired(&mut state);
        assert_eq!(result, 1);
        // NOTE: cannot set time_provider.fixed_unix_ts_ms without jumping through many more hoops
        // so cannot truly test time moving and ids expiring without significant refactoring for just that
    }

    #[test]
    fn get_heartbeat_impl_missing () {
        let mut state = AppState {
            timeout: TEST_TIMEOUT,
            expires: BTreeMap::new(),
            availables: VecDeque::from((1..3).collect::<Vec<usize>>()),
            time_provider: Box::new(SystemTimeProvider {}),
        };
        let result = get_heartbeat_impl(1, &mut state);
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
        let mut state = AppState {
            timeout: TEST_TIMEOUT,
            expires,
            availables: VecDeque::from((3..3).collect::<Vec<usize>>()),
            time_provider: Box::new(time_provider),
        };
        let result = get_heartbeat_impl(1, &mut state);
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
        let mut state = AppState {
            timeout: TEST_TIMEOUT,
            expires,
            availables: VecDeque::from((2..3).collect::<Vec<usize>>()),
            time_provider: Box::new(time_provider),
        };
        let result = get_heartbeat_impl(1, &mut state);
        assert_eq!(result, Err(ERROR_CODE_ID_EXPIRED));

        // expires has removed the previous entry
        assert_eq!(state.expires, vec_to_btree(vec![]));
        // and now the old id is at the end of the queue
        assert_eq!(state.availables, VecDeque::from(vec![2,1]));
    }
}
