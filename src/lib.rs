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
    io::{self, BufRead},
    net::Ipv4Addr,
    path::Path,
    thread,
    time::Instant,
};
use thread::JoinHandle;
use tracing::Level;
use tracing_subscriber;
use tracing_subscriber::{filter::LevelFilter, FmtSubscriber};

const VALUE_SIZE: usize = 2048;
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
            .take(2048)
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
        tracing::debug!(
            "int arr len: {}, encoded len: {}",
            int_arr.len(),
            encoded.len()
        );
        owned_string.push_str(&encoded);

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

pub fn run_bench(
    redis_server: &Ipv4Addr,
    redis_port: u16,
    trace_file: &str,
    machine_id: usize,
    num_machines: usize,
    num_processes: usize,
    cap: usize,
    radius: usize,
    search_for_radius: bool,
) -> Result<()> {
    let input_data_points =
        read_points(trace_file).wrap_err("Failed to read in input query data.")?;
    let max_points = (input_data_points.len() as f64 * (cap as f64) / 100.0) as usize;
    tracing::debug!("Read {} points", input_data_points.len());
    let input_data_points = input_data_points.as_slice()[0..max_points].to_vec();
    tracing::debug!("Reduced to size {}", input_data_points.len());

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

    let actual_num_processes = std::cmp::min(num_processes, core_ids.len());

    // transform to input data points for just this machine
    let mut final_points: Vec<(usize, f64, f64)> = Vec::default();
    let mut cur_proc_id = 0;
    for (i, pt) in input_data_points.iter().enumerate() {
        if i % num_machines == machine_id {
            final_points.push((cur_proc_id, pt.0, pt.1));
        }
        cur_proc_id += 1;
        cur_proc_id = cur_proc_id % actual_num_processes;
    }

    let total_num_queries = final_points.len();
    tracing::debug!("Final number of queries: {:?}", total_num_queries);
    let thread_traces: Vec<Vec<(usize, f64, f64)>> = vec![final_points; actual_num_processes];
    let start = Instant::now();

    // Create a thread for each active CPU core.
    let handles = core_ids
        .into_iter()
        .enumerate()
        .map(|(idx, core_id)| {
            let server_addr = format!("{:?}", redis_server);
            let trace = thread_traces[idx].clone();
            thread::spawn(move || {
                // Pin this thread to a single CPU core.
                core_affinity::set_for_current(core_id);

                // make a redis connection and run the thread
                let client = redis::Client::open((server_addr, redis_port))?;
                let mut con = client.get_connection()?;
                if search_for_radius {
                    let mut min_radius = 1;
                    // iterate through all of the requests, and make a request to the server
                    for (req_id, (cl_id, lat, long)) in trace.iter().enumerate() {
                        if *cl_id != idx {
                            continue;
                        }
                        //tracing::debug!(id=req_id, cl_id = cl_id, lat=?lat, long=?long, "Request");
                        let mut restaurants = radius_func(&mut con, *lat, *long, min_radius)?;
                        while restaurants.len() < TOP_K_VALUE {
                            tracing::debug!("Increasing radius to {}; req_id: {}, cl_id: {}", min_radius + 1, req_id, cl_id);
                            min_radius += 1;
                            restaurants = radius_func(&mut con, *lat, *long, min_radius)?;
                        }
                    }
                    Ok(min_radius)
                } else {
                    // iterate through all of the requests, and make a request to the server
                    for (req_id, (cl_id, lat, long)) in trace.iter().enumerate() {
                        if *cl_id != idx {
                            continue;
                        }
                        tracing::debug!(id=req_id, cl_id = cl_id, lat=?lat, long=?long, "Request");
                        let restaurants = radius_func(&mut con, *lat, *long, radius)?;
                        if  restaurants.len() < TOP_K_VALUE {
                            bail!("Did not get enough restaurants for query # {}, cl # {}, lat # {:?}, long # {:?}", req_id, cl_id, lat, long);
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
                    Ok(radius)
                }
            })
        })
        .collect::<Vec<JoinHandle<Result<usize>>>>();

    let mut overall_min_radius = 1;
    for handle in handles.into_iter() {
        let min_rad = handle
            .join()
            .unwrap()
            .wrap_err("Failed to join redis client thread.")?;
        if min_rad > overall_min_radius {
            overall_min_radius = min_rad
        }
    }
    let total_time = start.elapsed().as_millis();

    tracing::info!(
        min_radius = overall_min_radius,
        "Executed {} queries in {:?} milliseconds.",
        total_num_queries,
        total_time,
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

pub fn load_redis_store(redis_server: &Ipv4Addr, redis_port: u16, input_file: &str) -> Result<()> {
    load_data(input_file, redis_server, redis_port).wrap_err("Failed to load in input data")?;
    tracing::debug!("Loaded input data");

    Ok(())
}

#[derive(Debug, Deserialize)]
struct Point {
    pub lat: f64,
    pub long: f64,
}

/// Reads a query or input data file
fn read_points<P>(filename: P) -> Result<Vec<(f64, f64)>>
where
    P: AsRef<Path>,
{
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(filename)?;
    let mut final_array: Vec<(f64, f64)> = Vec::default();
    let mut i = 0;
    for record in rdr.deserialize() {
        if i % 100000 == 0 {
            tracing::debug!("On {}th record", i);
        }
        let point: Point = record?;
        if !(-1.0 * MAX_LATITUDE < point.lat && point.lat < MAX_LATITUDE)
            || !(-1.0 * MAX_LONGITUDE < point.long && point.long < MAX_LONGITUDE)
        {
            continue;
        }
        final_array.push((point.lat, point.long));
        i += 1;
    }
    tracing::debug!("Parsed points: {}", final_array.len());

    Ok(final_array)
}

/// Loads the points in input_file into the redis store
fn load_data(input_file: &str, redis_server: &Ipv4Addr, redis_port: u16) -> Result<()> {
    let points = read_points(input_file).wrap_err(format!(
        "Failed to read input file of points to add to geostore: {}",
        input_file
    ))?;
    tracing::debug!("Read {} points", points.len());

    // Retrieve the IDs of all active CPU cores.
    let core_ids = match core_affinity::get_core_ids() {
        Some(v) => v,
        None => {
            bail!("Failed to get core ids.");
        }
    };

    // transform to input data points for just this machine
    let mut final_points: Vec<(usize, f64, f64)> = Vec::default();
    let mut cur_proc_id = 0;
    for pt in points.iter() {
        final_points.push((cur_proc_id, pt.0, pt.1));
        cur_proc_id += 1;
        cur_proc_id = cur_proc_id % core_ids.len();
    }

    // Create a thread for each active CPU core.
    let handles = core_ids
        .into_iter()
        .enumerate()
        .map(|(_, core_id)| {
            let server_addr = format!("{:?}", redis_server);
            let trace = final_points.clone();
            thread::spawn(move || {
                // Pin this thread to a single CPU core.
                core_affinity::set_for_current(core_id);

                // make a redis connection and run the thread
                let client = redis::Client::open((server_addr, redis_port))?;
                let mut con = client.get_connection()?;

                // iterate through all of the requests, and make a request to the server
                for (cl_id, lat, long) in trace.iter() {
                    if *cl_id != core_id.id {
                        continue;
                    }
                    let payload = Payload::new();
                    con.geo_add(
                        KEY_NAME,
                        (Coord::lon_lat(*long, *lat), payload.to_string()?),
                    )
                    .wrap_err(format!(
                        "Failed to add input data point: {:?}",
                        (cl_id, lat, long)
                    ))?;
                    tracing::debug!(
                        "Added {},{} with payload rating of {:?}",
                        lat,
                        long,
                        payload.get_rating(),
                    );
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
