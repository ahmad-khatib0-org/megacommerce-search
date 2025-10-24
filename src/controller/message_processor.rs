use std::{
  io::{Error, ErrorKind},
  time::Duration,
};

use megacommerce_proto::{product_outbox::Payload, ChangeFeed, ConfigMeilisearch};
use megacommerce_shared::models::errors::{BoxedErr, ErrorType, InternalError};
use reqwest::Client;
use tokio::time::sleep;
use tracing::error;

use crate::controller::metrics::inc_meili_retries;

use super::task_processor::{delete_doc_from_meili, push_doc_to_meili};

// TODO: see if we want to skip message processing if an endpoint failed
pub async fn message_processor(
  payload: &str,
  http: &Client,
  cfg: &ConfigMeilisearch,
) -> Result<(), BoxedErr> {
  let ie = |err: BoxedErr, msg: &str, t: Option<ErrorType>| {
    InternalError::new(
      "search.controller.message_processor".to_string(),
      err,
      t.unwrap_or(ErrorType::Internal),
      false,
      msg.to_string(),
    )
  };

  let envelope: ChangeFeed = serde_json::from_str(payload).map_err(|err| {
    ie(Box::new(err), "failed to deserialize message payload", Some(ErrorType::JsonUnmarshal))
  })?;

  // ignore resolved marker
  if envelope.resolved > 0 {
    return Ok(());
  }

  if envelope.after.is_none() {
    return Err(Box::new(ie(
      Box::new(Error::new(ErrorKind::Other, "missing field")),
      "the after object is missing from the message!",
      None,
    )));
  }

  let after = envelope.after.unwrap();
  if after.payload.is_none() {
    return Err(Box::new(ie(
      Box::new(Error::new(ErrorKind::Other, "missing field")),
      "the after.payload object is missing from the message!",
      None,
    )));
  }

  let payload = after.payload.unwrap();
  let max_retries = cfg.task_max_retries();
  let mut backoff_ms = cfg.task_backoff_base_ms() as u64;

  for endpoint in cfg.server_urls.iter() {
    let mut ok = false;
    let mut tries = 0i32;

    while tries < max_retries {
      tries += 1;

      let response = match payload {
        Payload::Created(_) => push_doc_to_meili(&payload, http, endpoint, cfg).await,
        Payload::Updated(_) => push_doc_to_meili(&payload, http, endpoint, cfg).await,
        Payload::Deleted(_) => delete_doc_from_meili(&after.product_id, http, endpoint, cfg).await,
      };

      match response {
        Ok(true) => {
          ok = true;
          break;
        }
        Ok(false) => {
          error!(
            "Meili endpoint {} returned task failed (try {}/{})",
            endpoint, tries, max_retries
          );
          inc_meili_retries(endpoint);
        }
        Err(e) => {
          error!("Error pushing to Meili {}: {} (try {}/{})", endpoint, e, tries, max_retries);
          inc_meili_retries(endpoint);
        }
      }

      sleep(Duration::from_millis(backoff_ms)).await;
      // exponential backoff with cap and jitter
      backoff_ms = (backoff_ms.saturating_mul(2)).min(10_000);
      // small jitter
      let jitter = rand::random::<u8>() as u64 % 100;
      sleep(Duration::from_millis(jitter)).await;
    }

    if !ok {
      return Err(Box::new(ie(
        Box::new(Error::new(ErrorKind::Other, "task_failed")),
        "Failed to process  task for endpoint: {}",
        Some(ErrorType::TaskFailed),
      )));
    }
  }

  Ok(())
}
