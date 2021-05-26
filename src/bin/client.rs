use color_eyre::eyre::{Result, WrapErr};
use redisgeo_bench::{global_debug_init, load_redis_store, run_bench, TraceLevel};
use std::net::Ipv4Addr;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "Redis Geo-Bench Client")]
struct Opt {
    #[structopt(
        short = "s",
        long = "server_ip",
        help = "Address of server",
        default_value = "127.0.0.1"
    )]
    server_ip: Ipv4Addr,
    #[structopt(
        short = "debug",
        long = "debug_level",
        help = "Configure tracing settings.",
        default_value = "warn"
    )]
    trace_level: TraceLevel,
    #[structopt(short = "tf", long = "trace_file", help = "Input trace-file")]
    trace_file: String,
    #[structopt(
        short = "if",
        long = "input_file",
        help = "Input file of input points."
    )]
    input_file: String,
    #[structopt(
        short = "m",
        long = "machine_id",
        help = "Client machine id",
        default_value = "0"
    )]
    machine_id: usize,
    #[structopt(
        short = "n",
        long = "num_machines",
        help = "Number of total machines",
        default_value = "1"
    )]
    num_machines: usize,
    #[structopt(
        short = "p",
        long = "num_processes",
        help = "Number of client processes per machine",
        default_value = "8"
    )]
    num_processes: usize,
    #[structopt(
        short = "r",
        long = "redis_port",
        help = "Port that redis server is running on.",
        default_value = "6379"
    )]
    redis_port: u16,
    #[structopt(short = "l", long = "load", help = "Just load keys")]
    load: bool,
    #[structopt(long = "run", help = "Just load keys")]
    run: bool,
    #[structopt(
        short = "c",
        long = "cap",
        help = "Cap the number of queries",
        default_value = "100"
    )]
    cap: usize,
    #[structopt(long = "radius", help = "Radius for search")]
    radius: usize,
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    global_debug_init(opt.trace_level)?;
    if opt.load {
        load_redis_store(&opt.server_ip, opt.redis_port, &opt.input_file)
            .wrap_err("Failed to load store.")?;
    }

    if opt.run {
        run_bench(
            &opt.server_ip,
            opt.redis_port,
            &opt.trace_file,
            opt.machine_id,
            opt.num_machines,
            opt.num_processes,
            opt.cap,
            opt.radius,
        )
        .wrap_err("Failed to run bench.")?;
    }
    Ok(())
}
