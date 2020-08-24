use protobuf::{Node, NodeListReply, NodeListRequest, NodeLocateReply, NodeLocateRequest, NodeManagement};
use swarm::prelude::Dht;
use tonic::{Request, Response, Status};

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::sync::Arc;

pub struct NodeManagementImpl {
    dht: Arc<Dht>,
}

impl NodeManagementImpl {
    pub fn new(dht: Arc<Dht>) -> NodeManagementImpl {
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
        for node in self.dht.nodes() {
            // add to nodes
            nodes.push(Node {
                id: node.get_id(),
                rpc_addr: format!("{}:{}", node.get_ip_address(),
                    node.get_metadata("rpc_port").unwrap()),
                xfer_addr: format!("{}:{}", node.get_ip_address(),
                    node.get_metadata("xfer_port").unwrap()),
            });
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
        let node = match self.dht.locate(hash) {
            Some(node) => {
                Some( Node {
                    id: node.get_id(),
                    rpc_addr: format!("{}:{}", node.get_ip_address(),
                        node.get_metadata("rpc_port").unwrap()),
                    xfer_addr: format!("{}:{}", node.get_ip_address(),
                    node.get_metadata("xfer_port").unwrap()),
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
