#[macro_use]
extern crate log;

use comm::Server as CommServer;
use protobuf::{ImageManagementServer, AlbumManagementServer, NodeManagementServer, TaskManagementServer};
use structopt::StructOpt;
use swarm::prelude::{DhtBuilder, Swarm};
use tonic::transport::Server;

mod album;
use album::AlbumManager;
mod index;
mod task;
use task::TaskManager;
mod rpc;
use rpc::album::AlbumManagementImpl;
use rpc::image::ImageManagementImpl;
use rpc::node::NodeManagementImpl;
use rpc::task::TaskManagementImpl;
mod transfer;
use transfer::TransferStreamHandler;

use std::net::{IpAddr, SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
//use std::thread;

pub const FILLED_SOURCE: &'static str = "filled";
pub const RAW_SOURCE: &'static str = "raw";
pub const SPLIT_SOURCE: &'static str = "split";

// count, geocode, platform, precision, source
pub type Extent = (i64, String, String, u8, String);

// cloud_coverage, geocode, platform, source, tile, timestamp
pub type Image = (Option<f64>, String, String, String, String, i64);

// path, pixel_coverage, subdataset
pub type StFile = (String, f64, u8);

fn main() {
    // initilaize logger
    env_logger::init();

    unsafe {
        // disable printing gdal errors to stdout
        gdal_sys::CPLSetErrorHandler(
            Some(gdal_sys::CPLQuietErrorHandler));
    }

    // parse arguments
    let opt = Opt::from_args();

    // initialize topology builder
    let dht_builder = DhtBuilder::new(opt.tokens);

    // initialize swarm
    let seed_address = match opt.seed_ip_addr {
        Some(ip_addr) => Some(SocketAddr::new(ip_addr, opt.seed_port)),
        None => None,
    };

    let (mut swarm, dht) = Swarm::new(opt.node_id,
        opt.ip_addr, opt.gossip_port, seed_address, dht_builder);

    // set swarm instance metadata
    swarm.set_metadata("rpc_port", &opt.rpc_port.to_string());
    swarm.set_metadata("xfer_port", &opt.xfer_port.to_string());

    // start swarm
    swarm.start(2, 50, 2000).expect("swarm start");

    // create storage directory
    if let Err(e) = std::fs::create_dir_all(&opt.directory) {
        panic!("failed to create storage directory '{:?}': {}",
            opt.directory, e);
    }

    // initialize AlbumManager and TaskManager
    let album_manager = match AlbumManager::new(opt.directory.clone()) {
        Ok(album_manager) => album_manager,
        Err(e) => panic!("initialize AlbumManager failed: {}", e),
    };

    let album_manager = Arc::new(RwLock::new(album_manager));
    let task_manager = Arc::new(RwLock::new(TaskManager::new()));

    // start transfer server
    debug!("binding xfer server [address={}:{}]",
        opt.ip_addr, opt.rpc_port);

    let listener = TcpListener::bind(format!("{}:{}",
        opt.ip_addr, opt.xfer_port)).expect("xfer service bind");
    let transfer_stream_handler =
        Arc::new(TransferStreamHandler::new(album_manager.clone()));
    let mut server = CommServer::new(listener,
        50, transfer_stream_handler);

    server.start().expect("transfer server start");

    // start GRPC server
    info!("starting grpc server [address=0.0.0.0:{}]", opt.rpc_port);
    let addr = SocketAddr::new("0.0.0.0".parse().unwrap(), opt.rpc_port);

    let album_management = AlbumManagementImpl::new(
        album_manager.clone(), dht.clone(), task_manager.clone());
    let image_management = ImageManagementImpl::new(
        album_manager, dht.clone(), task_manager.clone());
    let node_management = NodeManagementImpl::new(dht.clone());
    let task_management = TaskManagementImpl::new(dht, task_manager);

    if let Err(e) = start_rpc_server(addr, album_management,
            image_management, node_management, task_management) {
        panic!("failed to start rpc server: {}", e);
    }

    // wait indefinitely
    //thread::park();
}

#[tokio::main]
async fn start_rpc_server(addr: SocketAddr, 
        album_management: AlbumManagementImpl,
        image_management: ImageManagementImpl,
        node_management: NodeManagementImpl,
        task_management: TaskManagementImpl)
        -> Result<(), Box<dyn std::error::Error>> {
    Server::builder()
        .add_service(AlbumManagementServer::new(album_management))
        .add_service(ImageManagementServer::new(image_management))
        .add_service(NodeManagementServer::new(node_management))
        .add_service(TaskManagementServer::new(task_management))
        .serve(addr).await?;

    Ok(())
}

#[derive(Debug, StructOpt)]
#[structopt(name = "stipd", about="Node in the STIP framework.")]
struct Opt {
    #[structopt(name="NODE_ID", help="Integer node identifier.")]
    node_id: u32,

    #[structopt(short="d", long="directory", help="data storage directory.")]
    directory: PathBuf,

    #[structopt(short="l", long="load-thread-count",
        help="thread count to load existing data.", default_value="4")]
    load_thread_count: u8,

    #[structopt(short="i", long="ip-address",
        help="gossip ip address.", default_value="127.0.0.1")]
    ip_addr: IpAddr,

    #[structopt(short="p", long="port",
        help="gossip port.", default_value="15605")]
    gossip_port: u16,

    #[structopt(short="r", long="rpc-port",
        help="rpc port.", default_value="15606")]
    rpc_port: u16,

    #[structopt(short="s", long="seed-ip-address", help="seed ip address.")]
    seed_ip_addr: Option<IpAddr>,

    #[structopt(short="e", long="seed-port",
        help="seed port.", default_value="15605")]
    seed_port: u16,

    #[structopt(short="t", long="token", help="token list for dht.")]
    tokens: Vec<u64>,

    #[structopt(short="x", long="xfer-port",
        help="data transfer port.", default_value="15607")]
    xfer_port: u16,
}
