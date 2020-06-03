use protobuf::{Node, NodeListReply, NodeListRequest, NodeManagement};
use swarm::prelude::Dht;
use tonic::{Request, Response, Status};

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

pub struct NodeManagementImpl {
    dht: Arc<RwLock<Dht>>,
}

impl NodeManagementImpl {
    pub fn new(dht: Arc<RwLock<Dht>>) -> NodeManagementImpl {
        NodeManagementImpl {
            dht: dht,
        }
    }
}

#[tonic::async_trait]
impl NodeManagement for NodeManagementImpl {
    async fn list(&self, request: Request<NodeListRequest>)
            -> Result<Response<NodeListReply>, Status> {
        trace!("NodeListRequest: {:?}", request);

        // populate cluster nodes from dht
        let mut nodes = Vec::new();
        {
            let dht = self.dht.read().unwrap();
            for (node_id, addrs) in dht.iter() {
                // convert Node to protobuf
                let node = to_protobuf_node(*node_id as u32,
                    &addrs.1, &addrs.2);

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
}

fn to_protobuf_node(node_id: u32, rpc_addr: &Option<SocketAddr>,
        xfer_addr: &Option<SocketAddr>) -> Node {
    // initialize node protobuf
    Node {
        id: node_id,
        rpc_addr: format!("{}", rpc_addr.unwrap()),
        xfer_addr: format!("{}", xfer_addr.unwrap()),
    }
}
