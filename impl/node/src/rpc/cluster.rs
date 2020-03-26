use protobuf::{Node, NodeListRequest, NodeListReply, NodeShowRequest, NodeShowReply, ClusterManagement};
use swarm::prelude::Dht;
use tonic::{Request, Response, Status};

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
            for (node_id, addrs) in dht.list() {
                let node = Node {
                    id: *node_id as u32,
                    rpc_addr: format!("{}", addrs.1.unwrap()),
                    xfer_addr: format!("{}", addrs.2.unwrap()),
                };

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

        // populate cluster nodes from dht
        let node = {
            let dht = self.dht.read().unwrap();
            match dht.get(request.id as u16) {
                None => None,
                Some(addrs) => {
                    Some(Node {
                        id: request.id,
                        rpc_addr: format!("{}", addrs.0.unwrap()),
                        xfer_addr: format!("{}", addrs.1.unwrap()),
                    })
                },
            }
        };

        // initialize reply
        let reply = NodeShowReply {
            node: node,
        };

        Ok(Response::new(reply))
    }
}
