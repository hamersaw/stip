use protobuf::{Node, NodeListReply, NodeListRequest, NodeManagement};
use swarm::prelude::Dht;
use tonic::{Request, Response, Status};

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
                // add to nodes
                nodes.push(Node {
                    id: *node_id as u32,
                    rpc_addr: format!("{}", &addrs.1.unwrap()),
                    xfer_addr: format!("{}", &addrs.2.unwrap()),
                });
            }
        }

        // initialize reply
        let reply = NodeListReply {
            nodes: nodes,
        };

        Ok(Response::new(reply))
    }
}
