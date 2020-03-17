use protobuf::{InitDatasetRequest, MickierClient};
use tonic::Request;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = MickierClient::connect("http://127.0.0.1:15606").await?;

    let request = Request::new(InitDatasetRequest {
        id: "Tonic".into(),
    });

    let response = client.init_dataset(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}
