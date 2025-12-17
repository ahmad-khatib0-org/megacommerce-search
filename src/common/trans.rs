use std::{collections::HashMap, error::Error, sync::Arc, time::Duration};

use megacommerce_proto::{TranslationElements, TranslationsGetRequest};
use megacommerce_shared::models::{
  context::Context,
  errors::{app_error_from_proto_app_error, BoxedErr, ErrorType, InternalError},
};
use tokio::time::timeout;
use tonic::Request;

use super::Common;

impl Common {
  pub(super) async fn translations_get(
    &mut self,
  ) -> Result<HashMap<String, TranslationElements>, BoxedErr> {
    let err_msg = "failed to get translations from common service".to_string();
    let path = "search.common.config_get".to_string();
    let ie = |msg: String, err: Box<dyn Error + Send + Sync>| {
      Box::new(InternalError { err_type: ErrorType::Internal, temp: false, err, msg, path })
    };

    let req = Request::new(TranslationsGetRequest {});
    let res = timeout(Duration::from_secs(5), self.client().unwrap().translations_get(req)).await;

    match res {
      Ok(Ok(res)) => {
        if res.get_ref().error.is_some() {
          let err = res.get_ref().error.as_ref().unwrap();
          let ctx = Arc::new(Context::default());
          return Err(ie(err_msg, Box::new(app_error_from_proto_app_error(ctx, err))));
        } else {
          Ok(res.get_ref().data.clone())
        }
      }
      Ok(Err(e)) => {
        return Err(ie(err_msg, Box::new(e)));
      }
      Err(e) => {
        return Err(ie("failed to get translations: request timeout".to_string(), Box::new(e)));
      }
    }
  }
}
