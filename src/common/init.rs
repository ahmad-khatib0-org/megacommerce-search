use std::time::Duration;

use megacommerce_proto::{common_service_client::CommonServiceClient, PingRequest};
use megacommerce_shared::models::errors::{BoxedErr, ErrorType, InternalError};
use tokio::time::timeout;
use tonic::{transport::Channel, Request};

use super::Common;

impl Common {
  pub async fn init_client(&mut self) -> Result<CommonServiceClient<Channel>, BoxedErr> {
    let return_err = |msg: &str, err: BoxedErr| {
      Box::new(InternalError {
        err,
        err_type: ErrorType::Internal,
        temp: false,
        msg: msg.into(),
        path: "search.common.init_client".into(),
      }) as BoxedErr
    };

    let url = self.service_config.service.common_service_grpc_url.clone();
    let mut client = CommonServiceClient::connect(url)
      .await
      .map_err(|err| return_err("failed to connect to common client", Box::new(err)))?;

    let request = Request::new(PingRequest {});
    let response = timeout(Duration::from_secs(5), client.ping(request)).await;
    match response {
      Ok(Ok(_)) => {}
      Ok(Err(e)) => {
        return Err(return_err("failed to ping the common client service", Box::new(e)));
      }
      Err(e) => return Err(return_err("the ping to common client service timedout", Box::new(e))),
    };

    Ok(client)
  }
}
