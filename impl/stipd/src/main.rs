#[macro_use]
extern crate log;

use comm::Server as CommServer;
use protobuf::{ClusterManagementServer, DataManagementServer, TaskManagementServer};
use structopt::StructOpt;
use swarm::prelude::{DhtBuilder, SwarmConfigBuilder};
use tonic::transport::Server;

mod image;
use image::ImageManager;
mod task;
use task::TaskManager;
mod rpc;
use rpc::cluster::ClusterManagementImpl;
use rpc::data::DataManagementImpl;
use rpc::task::TaskManagementImpl;
mod transfer;
use transfer::TransferStreamHandler;

use std::net::{IpAddr, SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
//use std::thread;

fn main() {
    // initilaize logger
    env_logger::init();

    // parse arguments
    let opt = Opt::from_args();

    // create storage directory
    if let Err(e) = std::fs::create_dir_all(&opt.directory) {
        panic!("failed to create storage directory '{:?}': {}",
            opt.directory, e);
    }

    // initialize ImageManager and TaskManager
    let image_manager = Arc::new(RwLock::new(
        ImageManager::new(opt.directory)));
    let task_manager = Arc::new(RwLock::new(TaskManager::new()));

    // build swarm config
    let swarm_config = SwarmConfigBuilder::new()
        .addr(SocketAddr::new(opt.ip_addr, opt.gossip_port))
        .gossip_interval_ms(2000)
        .build().expect("build swarm config");

    // build dht
    let dht_builder = DhtBuilder::new()
        .id(opt.node_id)
        .rpc_addr(SocketAddr::new(opt.ip_addr, opt.rpc_port))
        .swarm_config(swarm_config)
        .tokens(opt.tokens)
        .xfer_addr(SocketAddr::new(opt.ip_addr, opt.xfer_port));

    let dht_builder = if let Some(ip_addr) = opt.seed_ip_addr {
        dht_builder.seed_addr(SocketAddr::new(ip_addr, opt.seed_port))
    } else {
        dht_builder
    };

    let (mut swarm, dht) = dht_builder.build().expect("build dht");

    // start swarm
    swarm.start().expect("swarm start");

    // start transfer server
    let listener = TcpListener::bind(format!("{}:{}",
        opt.ip_addr, opt.xfer_port)).expect("xfer service bind");
    let transfer_stream_handler =
        Arc::new(TransferStreamHandler::new(image_manager.clone()));
    let mut server = CommServer::new(listener,
        50, transfer_stream_handler);

    server.start().expect("transfer server start");

    // compile vector is existing image paths
    let paths = {
        let image_manager = image_manager.read().unwrap();
        match image_manager.get_paths() {
            Ok(paths) => paths,
            Err(e) => panic!("failed to retrieve image paths: {}", e),
        }
    };

    // initialize image load channel
    let (sender, receiver) = crossbeam_channel::bounded(256);

    // start load theads
    for _ in 0..opt.load_thread_count {
        let image_manager_clone = image_manager.clone();
        let receiver_clone = receiver.clone();

        std::thread::spawn(move || {
            loop {
                // fetch next record
                let mut path: PathBuf = match receiver_clone.recv() {
                    Ok(record) => record,
                    Err(_) => break,
                };

                // parse image metadata
                match crate::image::to_image_metadata(&mut path) {
                    Ok((image, file)) => {
                        let mut image_manager = 
                            image_manager_clone.write().unwrap();
                        if let Err(e) = image_manager.load(
                                image.0, &file.0, &image.1, &file.1,
                                file.2, &image.2, &image.3, file.3,
                                &image.4, image.5) {
                            warn!("failed to load image: {}", e);
                        }
                    },
                    Err(e) => warn!("failed to parse image metadata: {}", e),
                }
            }
        });
    }
 
    // send existing image paths to image load channel
    let _ = std::thread::spawn(move || {
        for path in paths {
            if let Err(e) = sender.send(path) {
                warn!("failed to load image path: {}", e);
            }
        }

        drop(sender);
    });

    // start GRPC server
    let addr = SocketAddr::new("0.0.0.0".parse().unwrap(), opt.rpc_port);

    let cluster_management = ClusterManagementImpl::new(dht.clone());
    let data_management = DataManagementImpl::new(dht.clone(),
        image_manager, task_manager.clone());
    let task_management = TaskManagementImpl::new(dht, task_manager);

    if let Err(e) = start_rpc_server(addr, cluster_management,
            data_management, task_management) {
        panic!("failed to start rpc server: {}", e);
    }

    // wait indefinitely
    //thread::park();
}

#[tokio::main]
async fn start_rpc_server(addr: SocketAddr, 
        cluster_management: ClusterManagementImpl,
        data_management: DataManagementImpl,
        task_management: TaskManagementImpl)
        -> Result<(), Box<dyn std::error::Error>> {
    Server::builder()
        .add_service(ClusterManagementServer::new(cluster_management))
        .add_service(DataManagementServer::new(data_management))
        .add_service(TaskManagementServer::new(task_management))
        .serve(addr).await?;

    Ok(())
}

#[derive(Debug, StructOpt)]
#[structopt(name = "stipd", about="Node in the STIP framework.")]
struct Opt {
    #[structopt(name="NODE_ID", help="Integer node identifier.")]
    node_id: u16,

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
