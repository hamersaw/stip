use protobuf::{Node, NodeListReply, NodeListRequest, NodeLocateReply, NodeLocateRequest, NodeManagement};
use swarm::prelude::Dht;
use tonic::{Request, Response, Status};

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
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

    async fn locate(&self, request: Request<NodeLocateRequest>)
            -> Result<Response<NodeLocateReply>, Status> {
        trace!("NodeLocateRequest: {:?}", request);
        let request = request.get_ref();

        // compute geocode hash
        let mut hasher = DefaultHasher::new();
        hasher.write(request.geocode.as_bytes());
        let hash = hasher.finish();

        // discover hash location
        let dht = self.dht.read().unwrap(); 
        let node = match dht.locate(hash) {
            Some((node_id, addrs)) => {
                Some( Node {
                    id: *node_id as u32,
                    rpc_addr: format!("{}", &addrs.0.unwrap()),
                    xfer_addr: format!("{}", &addrs.1.unwrap()),
                })
            },
            None => None,
        };

        // initialize reply
        let reply = NodeLocateReply {
            node: node,
        };

        Ok(Response::new(reply))
    }
}
