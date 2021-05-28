use base64;
use byteorder::{ByteOrder, LittleEndian};
use color_eyre::eyre::{bail, Result, WrapErr};
use rand::{distributions::Alphanumeric, Rng};
use redis::{
    geo::{Coord, RadiusOptions, RadiusOrder, RadiusSearchResult, Unit},
    Commands, RedisResult,
};
use serde::Deserialize;
use std::{
    fs::File,
    io::{BufRead, BufReader},
    net::Ipv4Addr,
    thread,
    time::Instant,
};
use thread::JoinHandle;
use tracing::Level;
use tracing_subscriber;
use tracing_subscriber::{filter::LevelFilter, FmtSubscriber};

const VALUE_SIZE: usize = 2044;
const KEY_NAME: &str = "RESTAURANT";
const MAX_LATITUDE: f64 = 85.05112878;
const MAX_LONGITUDE: f64 = 180.0;
const TOP_K_VALUE: usize = 10;

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ReceivedPayload {
    rating: i64,
}

impl ReceivedPayload {
    fn from_string(payload: &str) -> Result<Self> {
        let encoding_length = payload.len() - VALUE_SIZE;
        let decoded =
            base64::decode(&payload.as_bytes()[VALUE_SIZE..(VALUE_SIZE + encoding_length)])
                .wrap_err(format!(
                    "Not able to decode: {:?}",
                    &payload.as_bytes()[VALUE_SIZE..(VALUE_SIZE + encoding_length)]
                ))?;
        Ok(ReceivedPayload {
            rating: LittleEndian::read_i64(decoded.as_slice()),
        })
    }

    fn get_rating(&self) -> i64 {
        self.rating
    }
}

#[repr(C)]
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Payload {
    arr: String,
    rating: i64,
}

impl Payload {
    fn new() -> Payload {
        let rand_s: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(VALUE_SIZE)
            .map(char::from)
            .collect();
        Payload {
            arr: rand_s,
            rating: rand::thread_rng().gen_range(1..6),
        }
    }

    fn get_rating(&self) -> i64 {
        self.rating
    }

    fn to_string(&self) -> Result<String> {
        let mut owned_string = self.arr.clone();
        let mut int_arr = vec![0u8; 8];
        LittleEndian::write_i64(int_arr.as_mut_slice(), self.rating);
        let encoded = base64::encode(int_arr.clone());
        owned_string.push_str(&encoded);
        assert!(owned_string.len() == VALUE_SIZE + 12);
        // test decoding
        let test_received = ReceivedPayload::from_string(owned_string.as_str())
            .wrap_err("Not able to decode received payload")?;
        assert!(test_received.get_rating() == self.rating);
        Ok(owned_string)
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum TraceLevel {
    Debug,
    Info,
    Warn,
    Error,
    Off,
}

impl std::str::FromStr for TraceLevel {
    type Err = color_eyre::eyre::Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(match s {
            "debug" => TraceLevel::Debug,
            "info" => TraceLevel::Info,
            "warn" => TraceLevel::Warn,
            "error" => TraceLevel::Error,
            "off" => TraceLevel::Off,
            x => bail!("unknown TRACE level {:?}", x),
        })
    }
}

pub fn global_debug_init(trace_level: TraceLevel) -> Result<()> {
    color_eyre::install()?;
    let subscriber = match trace_level {
        TraceLevel::Debug => FmtSubscriber::builder()
            .with_max_level(Level::DEBUG)
            .finish(),
        TraceLevel::Info => FmtSubscriber::builder()
            .with_max_level(Level::INFO)
            .finish(),
        TraceLevel::Warn => FmtSubscriber::builder()
            .with_max_level(Level::WARN)
            .finish(),
        TraceLevel::Error => FmtSubscriber::builder()
            .with_max_level(Level::ERROR)
            .finish(),
        TraceLevel::Off => FmtSubscriber::builder()
            .with_max_level(LevelFilter::OFF)
            .finish(),
    };
    tracing::subscriber::set_global_default(subscriber).expect("setting defualt subscriber failed");
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct QueriesResult {
    min_radius: usize,
    total_executed: usize,
    total_assigned_invalid: usize,
}

impl QueriesResult {
    pub fn new(
        min_radius: usize,
        total_executed: usize,
        total_assigned_invalid: usize,
    ) -> QueriesResult {
        QueriesResult {
            min_radius: min_radius,
            total_executed: total_executed,
            total_assigned_invalid: total_assigned_invalid,
        }
    }

    pub fn get_min_radius(&self) -> usize {
        self.min_radius
    }

    pub fn get_total_executed(&self) -> usize {
        self.total_executed
    }

    pub fn get_assigned_invalid(&self) -> usize {
        self.total_assigned_invalid
    }

    pub fn combine(&mut self, other: QueriesResult) {
        self.min_radius = std::cmp::max(self.min_radius, other.get_min_radius());
        self.total_assigned_invalid += other.total_assigned_invalid;
        self.total_executed += other.total_executed;
    }
}

pub fn run_bench(
    redis_server: &Ipv4Addr,
    redis_port: u16,
    trace_file: &str,
    machine_id: usize,
    num_machines: usize,
    num_processes: usize,
    radius: usize,
    search_for_radius: bool,
    cap: usize,
) -> Result<()> {
    // Retrieve the IDs of all active CPU cores.
    let core_ids = match core_affinity::get_core_ids() {
        Some(mut v) => {
            let len = v.len();
            if len > num_processes {
                v.drain(0..(len - num_processes));
            }
            v
        }
        None => {
            bail!("Failed to get core ids.");
        }
    };

    let actual_num_processes = core_ids.len();
    let start = Instant::now();

    // Create a thread for each active CPU core.
    let handles = core_ids
        .into_iter()
        .enumerate()
        .map(|(idx, core_id)| {
            let server_addr = format!("{:?}", redis_server);
            let filename = trace_file.to_string();
            thread::spawn(move || {
                // Pin this thread to a single CPU core.
                core_affinity::set_for_current(core_id);
                tracing::debug!("Spawn thread {}", idx);
                // make a redis connection and run the thread
                let client = redis::Client::open((server_addr, redis_port))?;
                tracing::debug!("made client");
                let mut con = client.get_connection()?;
                tracing::debug!("Got con");

                // open the file
                let mut rdr = csv::ReaderBuilder::new()
                    .has_headers(false)
                    .from_path(filename)?;

                // now run the queries
                let mut cur_count = 0;
                let mut cur_process = 0;
                let mut min_radius = 1;
                let mut total_evaluated = 0;
                let mut total_invalid = 0;

                for record in rdr.deserialize() {
                    if cur_count == cap {
                        break;
                    }
                    if !(cur_count % num_machines == machine_id) {
                        cur_count += 1;
                        continue;
                    }
                    if !(cur_process == idx) {
                    cur_process += 1;
                    cur_process = cur_process % actual_num_processes;
                    cur_count += 1;
                        continue;
                    }

                    // do the query
                    let point: Point = record?;
                    if !(-1.0 * MAX_LATITUDE < point.lat && point.lat < MAX_LATITUDE)
                        || !(-1.0 * MAX_LONGITUDE < point.long && point.long < MAX_LONGITUDE)
                    {
                        tracing::debug!("Point {:?} does not fit specifications.", point);
                        total_invalid += 1;
                    cur_process += 1;
                    cur_process = cur_process % actual_num_processes;
                    cur_count += 1;
                        continue;
                    }
                    total_evaluated += 1;

                    if total_evaluated % 1000 == 0 {
                        tracing::debug!(count = total_evaluated, core = idx, "Total evaluated so far");
                    }

                    if search_for_radius {
                        let mut restaurants = radius_func(&mut con, point.lat, point.long, min_radius)?;
                        while restaurants.len() < TOP_K_VALUE {
                            tracing::debug!("Increasing radius to {}; req_id: {}, cl_id: {}", min_radius + 1, cur_count, idx);
                            min_radius += 1;
                            restaurants = radius_func(&mut con, point.lat, point.long, min_radius)?;
                        }
                    } else {
                        tracing::debug!(id=cur_count, process = idx, lat=?point.lat, long=?point.long, "Request");
                        let restaurants = radius_func(&mut con, point.lat, point.long, radius)?;
                        if  restaurants.len() < TOP_K_VALUE {
                            bail!("Did not get enough restaurants for query # {}, cl # {}, lat # {:?}, long # {:?}", cur_count, idx, point.lat, point.long);
                        }
                        let payloads_result: Result<Vec<(usize, ReceivedPayload)>> = restaurants
                            .iter()
                            .enumerate()
                            .map(|(idx, resp)| Ok((idx, ReceivedPayload::from_string(&resp.name)?)))
                            .collect();
                        let mut payloads = payloads_result
                            .wrap_err("Unable to get vector of received payloads.")?;
                        payloads.sort_by(|a, b| {
                            a.1.get_rating().partial_cmp(&b.1.get_rating()).unwrap()
                        });
                    }
                    cur_process += 1;
                    cur_process = cur_process % actual_num_processes;
                    cur_count += 1;
                }
                if search_for_radius {
                    return Ok(QueriesResult::new(min_radius,  total_evaluated, total_invalid));
                }  else {
                    return Ok(QueriesResult::new(radius, total_evaluated, total_invalid));
                }

            })
        })
        .collect::<Vec<JoinHandle<Result<QueriesResult>>>>();

    let mut total_result = QueriesResult::default();
    for handle in handles.into_iter() {
        let result = handle
            .join()
            .unwrap()
            .wrap_err("Failed to join redis client thread.")?;
        total_result.combine(result);
    }
    let total_time = start.elapsed().as_millis();
    tracing::info!(
        min_radius = total_result.get_min_radius(),
        "Executed {} queries in {:?} milliseconds; {} invalid",
        total_result.get_total_executed(),
        total_time,
        total_result.get_assigned_invalid(),
    );

    Ok(())
}

fn radius_func(
    con: &mut redis::Connection,
    lat: f64,
    long: f64,
    radius: usize,
) -> RedisResult<Vec<RadiusSearchResult>> {
    let opts = RadiusOptions::default()
        .with_dist()
        .order(RadiusOrder::Asc)
        .limit(TOP_K_VALUE);
    con.geo_radius(KEY_NAME, long, lat, radius as _, Unit::Kilometers, opts)
}

pub fn load_redis_store(
    redis_server: &Ipv4Addr,
    redis_port: u16,
    input_file: &str,
    num_processes: usize,
) -> Result<()> {
    load_data(input_file, redis_server, redis_port, num_processes)
        .wrap_err("Failed to load in input data")?;
    tracing::debug!("Loaded input data");

    Ok(())
}

#[derive(Debug, Deserialize)]
struct Point {
    pub lat: f64,
    pub long: f64,
}

/// Loads the points in input_file into the redis store
fn load_data(
    input_file: &str,
    redis_server: &Ipv4Addr,
    redis_port: u16,
    num_processes: usize,
) -> Result<()> {
    // Retrieve the IDs of all active CPU cores.
    let core_ids = match core_affinity::get_core_ids() {
        Some(mut v) => {
            let len = v.len();
            if len > num_processes {
                v.drain(0..(len - num_processes));
            }
            v
        }
        None => {
            bail!("Failed to get core ids.");
        }
    };

    let total_cores = core_ids.len();
    // Create a thread for each active CPU core.
    let handles = core_ids
        .into_iter()
        .enumerate()
        .map(|(core_idx, core_id)| {
            let server_addr = format!("{:?}", redis_server);
            let filename = input_file.to_string();
            thread::spawn(move || {
                // Pin this thread to a single CPU core.
                core_affinity::set_for_current(core_id);

                // make a redis connection and run the thread
                let client = redis::Client::open((server_addr, redis_port))?;
                let mut con = client.get_connection()?;

                let file = File::open(&filename)?;
                let lines = BufReader::new(file).lines();
                let mut cur_core = 0;
                let mut ct = 0;
                for line_res in lines {
                    if cur_core != core_idx {
                        ct += 1;
                        cur_core += 1;
                        cur_core = cur_core % total_cores;
                        continue;
                    }
                    if ct % 100000 == 0 {
                        tracing::debug!("On {}th record", ct);
                    }
                    let line = line_res?;
                    let split = line.split(",").collect::<Vec<&str>>();
                    let a = split[0]
                        .parse::<f64>()
                        .wrap_err(format!("Failed to parse into f64: {}", split[0]))?;
                    let b = split[1]
                        .parse::<f64>()
                        .wrap_err(format!("Failed to parse into f64: {}", split[1]))?;
                    let point = Point { lat: a, long: b };
                    if !(-1.0 * MAX_LATITUDE < point.lat && point.lat < MAX_LATITUDE)
                        || !(-1.0 * MAX_LONGITUDE < point.long && point.long < MAX_LONGITUDE)
                    {
                        cur_core += 1;
                        cur_core = cur_core % total_cores;
                        ct += 1;
                        continue;
                    }

                    let payload = Payload::new();
                    con.geo_add(
                        KEY_NAME,
                        (Coord::lon_lat(point.long, point.lat), payload.to_string()?),
                    )
                    .wrap_err(format!(
                        "Core {} Failed to add input data point: {:?}",
                        core_idx,
                        (point.lat, point.long)
                    ))?;
                    tracing::debug!(
                        "Added {},{} with payload rating of {:?}",
                        point.lat,
                        point.long,
                        payload.get_rating(),
                    );

                    cur_core += 1;
                    cur_core = cur_core % total_cores;
                    ct += 1;
                }
                Ok(())
            })
        })
        .collect::<Vec<JoinHandle<Result<()>>>>();

    for handle in handles.into_iter() {
        handle
            .join()
            .unwrap()
            .wrap_err("Failed to join redis thread to load data.")?;
    }

    Ok(())
}
