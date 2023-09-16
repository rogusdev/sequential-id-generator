
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
    time_provider: Box<dyn TimeProvider>,
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

fn get_next_impl (mut state: AppState) -> Result<(usize, i64), usize> {
    if let Some(id_next) = state.availables.pop_front() {
        let expiry = state.time_provider.unix_ts_ms() + state.timeout;
        state.expires.insert(id_next, expiry);
        Ok((id_next, expiry))
    } else {
        Err(ERROR_CODE_NO_ID_AVAILBLE)
    }
}

async fn get_next (State(state): State<AppState>) -> Json<Value> {
    match get_next_impl(state) {
        Ok((id_next, expiry)) => Json(json!({
            "id": id_next,
            "exp": expiry,
        })),
        Err(code) => json_error(code)
    }
}

fn get_heartbeat_impl (id: usize, mut state: AppState) -> Result<i64, usize> {
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

async fn get_heartbeat (Path(id): Path<usize>, State(state): State<AppState>) -> Json<Value> {
    match get_heartbeat_impl(id, state) {
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
        availables: VecDeque::from((0..id_max).collect::<Vec<usize>>()),
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
    use time_provider::FixedTimeProvider;

    const TEST_TIMEOUT: i64 = 2000;

    fn vec_to_btree<T: Ord, U> (v: Vec<(T, U)>) -> BTreeMap<T, U> {
        v.into_iter()
            .map(|x| (x.0, x.1))
            .collect::<BTreeMap<_, _>>()
    }

    #[test]
    fn get_heartbeat_impl_missing () {
        let state = AppState {
            timeout: TEST_TIMEOUT,
            expires: BTreeMap::new(),
            availables: VecDeque::from((0..3).collect::<Vec<usize>>()),
            time_provider: Box::new(SystemTimeProvider {}),
        };
        let result = get_heartbeat_impl(1, state);
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
        time_provider.fixed_unix_ts_ms += TEST_TIMEOUT * 2;
        let state = AppState {
            timeout: TEST_TIMEOUT,
            expires: BTreeMap::new(),
            availables: VecDeque::from((0..3).collect::<Vec<usize>>()),
            time_provider: Box::new(time_provider),
        };
        let result = get_heartbeat_impl(1, state);
        assert_eq!(result, Ok(now + TEST_TIMEOUT));
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
        let state = AppState {
            timeout: TEST_TIMEOUT,
            expires: BTreeMap::new(),
            availables: VecDeque::from((0..3).collect::<Vec<usize>>()),
            time_provider: Box::new(time_provider),
        };
        let result = get_heartbeat_impl(1, state);
        assert_eq!(result, Ok(now + TEST_TIMEOUT));
    }

    // #[test]
    // fn get_next_empty () {
    //     let data: Vec<(usize, i64)> = vec![];
    //     let ids = data
    //         .into_iter()
    //         .map(|x| (x.0, x.1))
    //         .collect::<BTreeMap<_, _>>();
    //     for id_low in 1..6 {
    //         let result = find_lowest_available_key(ids.keys(), id_low, 256);
    //         assert_eq!(result, Some(id_low));
    //     }
    // }

    // #[test]
    // fn get_next_happy () {
    //     let data = vec![
    //         (1, 123),
    //         (2, 456),
    //     ];
    //     let ids = data
    //         .into_iter()
    //         .map(|x| (x.0, x.1))
    //         .collect::<BTreeMap<_, _>>();
    //     for id_low in 1..6 {
    //         let result = find_lowest_available_key(ids.keys(), id_low, 256);
    //         assert_eq!(result, Some(if id_low <= 3 { 3 } else { id_low }));
    //     }
    // }

    // #[test]
    // fn get_next_skip () {
    //     let data = vec![
    //         (1, 123),
    //         (3, 456),
    //     ];
    //     let ids = data
    //         .into_iter()
    //         .map(|x| (x.0, x.1))
    //         .collect::<BTreeMap<_, _>>();
    //     for id_low in 1..6 {
    //         let result = find_lowest_available_key(ids.keys(), id_low, 256);
    //         assert_eq!(result, Some(if id_low <= 2 { 2 } else if id_low <= 4 { 4 } else { id_low }));
    //     }
    // }

    // #[test]
    // fn get_next_max () {
    //     let data = vec![
    //         (1, 123),
    //         (2, 456),
    //     ];
    //     let ids = data
    //         .into_iter()
    //         .map(|x| (x.0, x.1))
    //         .collect::<BTreeMap<_, _>>();
    //     for id_low in 1..6 {
    //         let result = find_lowest_available_key(ids.keys(), id_low, 2);
    //         assert_eq!(result, None);

    //         let result2 = find_lowest_available_key(ids.keys(), id_low, 3);
    //         assert_eq!(result2, if id_low <= 3 { Some(3) } else { None });
    //     }
    // }
}
