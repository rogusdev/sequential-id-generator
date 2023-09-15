
use std::{env, collections::btree_map::Keys};

use axum::{
	routing::get,
	extract::{Path, State},
    response::Json,
	Router,
};

use serde_json::{Value, json};

use std::collections::BTreeMap;

use std::time::{SystemTime, UNIX_EPOCH};

#[macro_use]
extern crate lazy_static;


const DEFAULT_PORT: usize = 3000;
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


pub trait TimeProvider {
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


#[derive(Clone)]
struct AppState {
    id_max: usize,
    id_lowest_available: usize,
    ids: BTreeMap<usize, i64>,
    timeout: i64,
    //time_provider: impl TimeProvider,
    time_provider: SystemTimeProvider,
}

fn env_var_parse<T: std::str::FromStr> (name: &str, default: T) -> T {
    match env::var(name) {
        Ok(s) => s.parse::<T>().unwrap_or(default),
        _ => default
    }
}

fn find_lowest_available_key<T> (mut keys: Keys<usize, T>, from: usize, max: usize) -> Option<usize> {
    // TODO: actually want to switch to some sort of LRU of available ids
    //  perhaps pre-populate entire collection and always pick lowest (oldest) expiry for unused id
    //  i.e. keep a cursor into position after the last position used, and wrap around as needed
    //  skipping over any still in use ids
    // Going in sequence should always make oldest next?...
    //  NO because
    // Instead should have 2 lists: 1. BTreeSet of available ids, 2. BTreeMap of ids to expirations
    //  then move keys back and forth as ids expire or get renewed -- also using oldest

    // https://stackoverflow.com/questions/42052065/how-can-i-introduce-a-copied-variable-as-mutable-in-a-if-let-statement/42052916#42052916
    // https://stackoverflow.com/questions/31012923/what-is-the-difference-between-copy-and-clone
    let mut key_next = 0;
    if let Some(key_prev) = keys.next().copied() {
        key_next = key_prev + 1;
        for &key in keys {
            if key > key_next && key > from {
                return Some(key_next)
            }
            key_next = key;
        }
    }

    key_next = from.max(key_next + 1);
    if key_next <= max {
        Some(key_next)
    } else {
        None
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

async fn get_next (State(mut state): State<AppState>) -> Json<Value> {
    let opt_lowest_available_key = find_lowest_available_key(
        state.ids.keys(),
        state.id_lowest_available,
        state.id_max
    );

    if let Some(id_lowest_available) = opt_lowest_available_key {
        state.id_lowest_available = id_lowest_available + 1;
        let expiry = state.time_provider.unix_ts_ms() + state.timeout;
        state.ids.insert(id_lowest_available, expiry);
        Json(json!({
            "id": id_lowest_available
        }))
    } else {
        json_error(ERROR_CODE_NO_ID_AVAILBLE)
    }
}

fn get_heartbeat_impl (id: usize, mut state: AppState) -> Result<i64, usize> {
    if let Some(&expiry) = state.ids.get(&id) {
        let now = state.time_provider.unix_ts_ms();
        if expiry > now {
            let expiry = now + state.timeout;
            state.ids.insert(id, expiry);
            Ok(now)
        } else {
            state.ids.remove(&id);
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
        Err(code) => {
            json_error(code)
        }
    }
}


#[tokio::main]
async fn main() {
    let port = env_var_parse("PORT", DEFAULT_PORT);
    let id_max = env_var_parse("MAX", DEFAULT_MAX);
    let timeout = env_var_parse("TIMEOUT", DEFAULT_TIMEOUT);

    let state = AppState {
        id_max,
        id_lowest_available: 1,
        ids: BTreeMap::new(),
        timeout,
        time_provider: SystemTimeProvider {},
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

    #[test]
    fn find_lowest_available_key_empty () {
        let data: Vec<(usize, i64)> = vec![];
        let ids = data
            .into_iter()
            .map(|x| (x.0, x.1))
            .collect::<BTreeMap<_, _>>();
        for id_low in 1..6 {
            let result = find_lowest_available_key(ids.keys(), id_low, 256);
            assert_eq!(result, Some(id_low));
        }
    }

    #[test]
    fn find_lowest_available_key_happy () {
        let data = vec![
            (1, 123),
            (2, 456),
        ];
        let ids = data
            .into_iter()
            .map(|x| (x.0, x.1))
            .collect::<BTreeMap<_, _>>();
        for id_low in 1..6 {
            let result = find_lowest_available_key(ids.keys(), id_low, 256);
            assert_eq!(result, Some(if id_low <= 3 { 3 } else { id_low }));
        }
    }

    #[test]
    fn find_lowest_available_key_skip () {
        let data = vec![
            (1, 123),
            (3, 456),
        ];
        let ids = data
            .into_iter()
            .map(|x| (x.0, x.1))
            .collect::<BTreeMap<_, _>>();
        for id_low in 1..6 {
            let result = find_lowest_available_key(ids.keys(), id_low, 256);
            assert_eq!(result, Some(if id_low <= 2 { 2 } else if id_low <= 4 { 4 } else { id_low }));
        }
    }

    #[test]
    fn find_lowest_available_key_max () {
        let data = vec![
            (1, 123),
            (2, 456),
        ];
        let ids = data
            .into_iter()
            .map(|x| (x.0, x.1))
            .collect::<BTreeMap<_, _>>();
        for id_low in 1..6 {
            let result = find_lowest_available_key(ids.keys(), id_low, 2);
            assert_eq!(result, None);

            let result2 = find_lowest_available_key(ids.keys(), id_low, 3);
            assert_eq!(result2, if id_low <= 3 { Some(3) } else { None });
        }
    }
}
