use protobuf::{Node, NodeListReply, NodeListRequest, NodeShowReply, NodeShowRequest, ClusterManagement};
use swarm::prelude::Dht;
use tonic::{Request, Response, Status};

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

pub struct ClusterManagementImpl {
    dht: Arc<RwLock<Dht>>,
}

impl ClusterManagementImpl {
    pub fn new(dht: Arc<RwLock<Dht>>) -> ClusterManagementImpl {
        ClusterManagementImpl {
            dht: dht,
        }
    }
}

#[tonic::async_trait]
impl ClusterManagement for ClusterManagementImpl {
    async fn node_list(&self, request: Request<NodeListRequest>)
            -> Result<Response<NodeListReply>, Status> {
        trace!("NodeListRequest: {:?}", request);

        // populate cluster nodes from dht
        let mut nodes = Vec::new();
        {
            let dht = self.dht.read().unwrap();
            for (node_id, addrs) in dht.iter() {
                // convert Node to protobuf
                let node = to_protobuf(*node_id as u32, &addrs.1, &addrs.2);

                // add to nodes
                nodes.push(node);
            }
        }

        // initialize reply
        let reply = NodeListReply {
            nodes: nodes,
        };

        Ok(Response::new(reply))
    }

    async fn node_show(&self, request: Request<NodeShowRequest>)
            -> Result<Response<NodeShowReply>, Status> {
        trace!("NodeShowRequest: {:?}", request);
        let request = request.get_ref();

        // populate cluster node from dht
        let node = {
            let dht = self.dht.read().unwrap();
            match dht.get(request.id as u16) {
                None => None,
                Some(addrs) =>
                    Some(to_protobuf(request.id, addrs.0, addrs.1)),
            }
        };

        // initialize reply
        let reply = NodeShowReply {
            node: node,
        };

        Ok(Response::new(reply))
    }
}

fn to_protobuf(node_id: u32, rpc_addr: &Option<SocketAddr>,
        xfer_addr: &Option<SocketAddr>) -> Node {
    // initialize node protobuf
    Node {
        id: node_id,
        rpc_addr: format!("{}", rpc_addr.unwrap()),
        xfer_addr: format!("{}", xfer_addr.unwrap()),
    }
}
