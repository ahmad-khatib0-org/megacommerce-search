use std::{sync::Arc, time::Duration};

use megacommerce_proto::{config_get_response::Response, Config as SharedConfig, ConfigGetRequest};
use megacommerce_shared::models::{
  context::Context,
  errors::{app_error_from_proto_app_error, BoxedErr, ErrorType, InternalError},
};
use tokio::time::timeout;
use tonic::Request;

use super::Common;

impl Common {
  pub(super) async fn config_get(&mut self) -> Result<SharedConfig, BoxedErr> {
    let err_msg = "failed to get configurations from common service";
    let mk_err = |msg: &str, err: BoxedErr| {
      Box::new(InternalError {
        err,
        err_type: ErrorType::Internal,
        temp: false,
        msg: msg.into(),
        path: "auth.common.config_get".into(),
      })
    };

    let req = Request::new(ConfigGetRequest {});
    let res = timeout(Duration::from_secs(5), self.client().unwrap().config_get(req)).await;

    match res {
      Ok(Ok(res)) => match res.into_inner().response {
        Some(Response::Data(res)) => return Ok(res),
        Some(Response::Error(res)) => {
          let err = app_error_from_proto_app_error(Arc::new(Context::default()), &res);
          return Err(mk_err(err_msg, Box::new(err)));
        }
        None => {
          return Err(mk_err("missing response field in config_get", "empty".into()));
        }
      },
      Ok(Err(e)) => {
        return Err(mk_err(err_msg, Box::new(e)));
      }
      Err(e) => {
        return Err(mk_err("failed to get configurations: request timeout", Box::new(e)));
      }
    }
  }
}
