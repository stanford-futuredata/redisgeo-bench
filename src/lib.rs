use color_eyre::eyre::{bail, Result, WrapErr};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use redis::{
    geo::{Coord, RadiusOptions, RadiusOrder, RadiusSearchResult, Unit},
    Commands, RedisResult,
};
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

const VALUE_SIZE: usize = 10;
const KEY_NAME: &str = "RESTAURANT";
const MAX_LATITUDE: f64 = 85.05112878;
const MAX_LONGITUDE: f64 = 180.0;
const SEARCH_RADIUS: f64 = 100.0;
const TOP_K_VALUE: usize = 10;

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
) -> Result<()> {
    let input_data_points =
        read_points(trace_file).wrap_err("Failed to read in input query data.")?;

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

                // iterate through all of the requests, and make a request to the server
                for (id, lat, long) in trace.iter() {
                    if *id != idx {
                        continue;
                    }
                    let restaurants = radius(&mut con, *lat, *long)?;
                    tracing::debug!(
                        "Returned restaurant list of length: {:?} for query # {:?}",
                        restaurants.len(),
                        id
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
            .wrap_err("Failed to join redis client thread.")?;
    }
    let total_time = start.elapsed().as_millis();

    tracing::info!(
        "Executed {} queries in {:?} milliseconds.",
        total_num_queries,
        total_time
    );

    Ok(())
}

fn radius(
    con: &mut redis::Connection,
    lat: f64,
    long: f64,
) -> RedisResult<Vec<RadiusSearchResult>> {
    let opts = RadiusOptions::default()
        .with_dist()
        .order(RadiusOrder::Asc)
        .limit(TOP_K_VALUE);
    con.geo_radius(KEY_NAME, long, lat, SEARCH_RADIUS, Unit::Kilometers, opts)
}

pub fn load_redis_store(redis_server: &Ipv4Addr, redis_port: u16, input_file: &str) -> Result<()> {
    let input_data_points = load_data(input_file).wrap_err("Failed to load in input data")?;

    // make Redis connection
    let client = redis::Client::open((format!("{:?}", redis_server), redis_port))?;
    let mut con = client.get_connection()?;

    // for each datapoint, do a geoadd
    for pt in input_data_points {
        con.geo_add(KEY_NAME, (Coord::lon_lat(pt.1, pt.0), pt.2.clone()))
            .wrap_err(format!("Failed to add input data point: {:?}", pt))?;
    }
    tracing::debug!("Finished loading server.");

    Ok(())
}

/// Reads a query or input data file
fn read_points<P>(filename: P) -> Result<Vec<(f64, f64)>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    let lines = io::BufReader::new(file).lines();
    let mut ret: Vec<(f64, f64)> = Vec::default();
    for line_res in lines {
        let line = line_res?;
        let split = line.split(",").collect::<Vec<&str>>();
        if split.len() != 2 {
            bail!("Trace file line does not have two items: {:?}", split);
        }
        let a = split[0]
            .parse::<f64>()
            .wrap_err(format!("Failed to parse into f64: {}", split[0]))?;
        let b = split[1]
            .parse::<f64>()
            .wrap_err(format!("Failed to parse into f64: {}", split[1]))?;
        ret.push((a, b));
    }
    Ok(ret)
}

/// Loads the points in input_file into the redis store
fn load_data(input_file: &str) -> Result<Vec<(f64, f64, String)>> {
    let points = read_points(input_file).wrap_err(format!(
        "Failed to read input file of points to add to geostore: {}",
        input_file
    ))?;
    let points_with_random_value: Vec<(f64, f64, String)> = points
        .iter()
        .map(|(a, b)| {
            let rand_string: String = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(VALUE_SIZE)
                .map(char::from)
                .collect();
            (*a, *b, rand_string)
        })
        .filter(|(lat, long, _val)| {
            (-1.0 * MAX_LATITUDE < *lat && *lat < MAX_LATITUDE)
                && (-1.0 * MAX_LONGITUDE < *long && *long < MAX_LONGITUDE)
        })
        .collect();

    Ok(points_with_random_value)
}
