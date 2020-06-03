use protobuf::{Environment, EnvironmentListReply, EnvironmentListRequest, EnvironmentShowReply, EnvironmentShowRequest, EnvironmentManagement};
use tonic::{Request, Response, Status};

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

pub struct EnvironmentManagementImpl {
}

impl EnvironmentManagementImpl {
    pub fn new(dht: Arc<RwLock<Dht>>) -> EnvironmentManagementImpl {
        EnvironmentManagementImpl {
        }
    }
}

#[tonic::async_trait]
impl EnvironmentManagement for EnvironmentManagementImpl {
    async fn list(&self, request: Request<EnvironmentListRequest>)
            -> Result<Response<EnvironmentListReply>, Status> {
        trace!("EnvironmentListRequest: {:?}", request);

        // populate cluster nodes from dht
        let mut nodes = Vec::new();
        {
            let dht = self.dht.read().unwrap();
            for (node_id, addrs) in dht.iter() {
                // convert Environment to protobuf
                let node = to_protobuf_node(*node_id as u32,
                    &addrs.1, &addrs.2);

                // add to nodes
                nodes.push(node);
            }
        }

        // initialize reply
        let reply = EnvironmentListReply {
            nodes: nodes,
        };

        Ok(Response::new(reply))
    }

    async fn node_show(&self, request: Request<EnvironmentShowRequest>)
            -> Result<Response<EnvironmentShowReply>, Status> {
        trace!("EnvironmentShowRequest: {:?}", request);
        let request = request.get_ref();

        // populate cluster node from dht
        let node = {
            let dht = self.dht.read().unwrap();
            match dht.get(request.id as u16) {
                None => None,
                Some(addrs) =>
                    Some(to_protobuf_node(request.id, addrs.0, addrs.1)),
            }
        };

        // initialize reply
        let reply = EnvironmentShowReply {
            node: node,
        };

        Ok(Response::new(reply))
    }
}

fn to_protobuf_node(node_id: u32, rpc_addr: &Option<SocketAddr>,
        xfer_addr: &Option<SocketAddr>) -> Environment {
    // initialize node protobuf
    Environment {
        id: node_id,
        rpc_addr: format!("{}", rpc_addr.unwrap()),
        xfer_addr: format!("{}", xfer_addr.unwrap()),
    }
}
