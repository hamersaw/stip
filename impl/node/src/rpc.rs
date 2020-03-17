use protobuf::{InitDatasetRequest, InitDatasetReply, DataManagement};
use tonic::{Request, Response, Status};

#[derive(Debug, Default)]
pub struct DataManagementImpl {
}

impl DataManagementImpl {
    pub fn new() -> DataManagementImpl {
        DataManagementImpl {
        }
    }
}

#[tonic::async_trait]
impl DataManagement for DataManagementImpl {
    async fn init_dataset(&self, request: Request<InitDatasetRequest>)
            -> Result<Response<InitDatasetReply>, Status> {
        println!("Got a request: {:?}", request);

        let reply = InitDatasetReply {
            //message: format!("Hello {}!", request.into_inner().name).into(), // We must use .into_inner() as the fields of gRPC requests and responses are private
        };

        Ok(Response::new(reply))
    }
}
