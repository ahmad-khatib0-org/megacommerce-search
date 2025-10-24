use std::{
  io::{Error, ErrorKind},
  time::Duration,
};

use hyper::header::CONTENT_TYPE;
use megacommerce_proto::{
  product_outbox::Payload, ConfigMeilisearch, TaskCreateResponse, TaskDeleteResponse,
  TaskGetResponse,
};
use megacommerce_shared::models::errors::{BoxedErr, ErrorType, InternalError};
use reqwest::Client;
use tokio::time::sleep;
use tracing::error;

use crate::models::tasks::Pretty;

pub async fn push_doc_to_meili(
  payload: &Payload,
  http: &Client,
  endpoint: &str,
  cfg: &ConfigMeilisearch,
) -> Result<bool, BoxedErr> {
  let url = format!("{}/indexes/products/documents", endpoint);
  let ie = |err: BoxedErr, msg: &str, t: Option<ErrorType>| {
    return InternalError::new(
      "search.controller.push_doc_to_meili".to_string(),
      err,
      t.unwrap_or(ErrorType::Internal),
      true,
      msg.to_string(),
    );
  };

  let mut req = http.post(&url).header(CONTENT_TYPE, "application/json").json(payload);
  if let Some(key) = &cfg.master_key {
    req = req.bearer_auth(key);
  }

  let resp = req.send().await.map_err(|e| {
    ie(
      Box::new(e),
      format!("failed to post a document to meilisearch endpoint: {}", endpoint).as_str(),
      Some(ErrorType::HttpRequestError),
    )
  })?;

  let status = resp.status();
  if !status.is_success() {
    let txt = resp.text().await.unwrap_or_default();
    return Err(Box::new(ie(
      Box::new(Error::new(ErrorKind::Other, "http_response_error")),
      &format!(
        "meilisearch didn't return a success status!: status: {}, response: {}",
        txt, status
      ),
      Some(ErrorType::HttpResponseError),
    )));
  }

  let response: TaskCreateResponse = resp.json().await.map_err(|err| {
    ie(Box::new(err), "failed to unmarshal meilisearch response ", Some(ErrorType::JsonUnmarshal))
  })?;

  let status_url = format!("{}/tasks/{}", endpoint, response.task_uid);
  let mut waited = 0u64;
  let poll_interval_ms = 200u64;
  let max_wait_ms = cfg.task_max_wait_ms() as u64;
  let master_key = cfg.master_key();

  while waited < max_wait_ms {
    sleep(Duration::from_millis(poll_interval_ms)).await;
    waited += poll_interval_ms;

    let r_builder = http.get(&status_url);
    let res = if !master_key.is_empty() {
      r_builder.bearer_auth(master_key).send().await
    } else {
      r_builder.send().await
    }
    .map_err(|e| {
      ie(
        Box::new(e),
        "meilisearch status poll returned an error",
        Some(ErrorType::HttpResponseError),
      )
    })?;

    let status = res.status();
    if !status.is_success() {
      let txt = res.text().await.unwrap_or_default();
      error!("Non-200 polling response: {} -> {}", status, txt);
      continue;
    }

    let task_res: TaskGetResponse = res.json().await.map_err(|err| {
      ie(Box::new(err), "failed to unmarshal meilisearch response", Some(ErrorType::JsonUnmarshal))
    })?;

    match task_res.status.as_str() {
      "succeeded" => return Ok(true),
      "failed" => {
        error!("Meilisearch task failed: response body: {}", task_res.pretty());
        return Ok(false);
      }
      _ => {} // enqueued / processing -> continue polling
    }
  }

  Err(Box::new(ie(
    Box::new(Error::new(ErrorKind::TimedOut, "task_timedout")),
    "meilisearch status poll returned an error",
    Some(ErrorType::TimedOut),
  )))
}

pub async fn delete_doc_from_meili(
  product_id: &str,
  http: &Client,
  endpoint: &str,
  cfg: &ConfigMeilisearch,
) -> Result<bool, BoxedErr> {
  let ie = |err: BoxedErr, msg: &str, t: Option<ErrorType>| {
    return InternalError::new(
      "search.controller.push_doc_to_meili".to_string(),
      err,
      t.unwrap_or(ErrorType::Internal),
      true,
      msg.to_string(),
    );
  };

  let url = format!("{}/indexes/products/documents/{}", endpoint, product_id);

  let mut req = http.delete(&url);
  if let Some(key) = &cfg.master_key {
    req = req.bearer_auth(key);
  }

  let resp = req.send().await.map_err(|e| {
    ie(
      Box::new(e),
      format!("failed to delete a document from meilisearch endpoint: {}", endpoint).as_str(),
      Some(ErrorType::HttpRequestError),
    )
  })?;

  let status = resp.status();
  if !status.is_success() {
    let txt = resp.text().await.unwrap_or_default();
    return Err(Box::new(ie(
      Box::new(Error::new(ErrorKind::Other, "http_response_error")),
      &format!(
        "meilisearch didn't return a success status!: status: {}, response: {}",
        txt, status
      ),
      Some(ErrorType::HttpResponseError),
    )));
  }

  let response: TaskDeleteResponse = resp.json().await.map_err(|err| {
    ie(Box::new(err), "failed to unmarshal meilisearch response ", Some(ErrorType::JsonUnmarshal))
  })?;

  // Poll until done
  let status_url = format!("{}/tasks/{}", endpoint, response.task_uid);
  let mut waited = 0u64;
  let poll_interval_ms = 200u64;
  let max_wait_ms = cfg.task_max_wait_ms() as u64;
  let master_key = cfg.master_key();

  while waited < max_wait_ms {
    sleep(Duration::from_millis(poll_interval_ms)).await;
    waited += poll_interval_ms;

    let r_builder = http.get(&status_url);
    let res = if !master_key.is_empty() {
      r_builder.bearer_auth(master_key).send().await
    } else {
      r_builder.send().await
    }
    .map_err(|err| {
      ie(
        Box::new(err),
        "meilisearch status poll returned an error",
        Some(ErrorType::HttpResponseError),
      )
    })?;

    let status = res.status();
    if !status.is_success() {
      let txt = res.text().await.unwrap_or_default();
      error!("Non-200 polling response: {} -> {}", status, txt);
      continue;
    }

    let task_res: TaskGetResponse = res.json().await.map_err(|err| {
      ie(Box::new(err), "failed to unmarshal meilisearch response", Some(ErrorType::JsonUnmarshal))
    })?;

    match task_res.status.as_str() {
      "succeeded" => return Ok(true),
      "failed" => {
        error!("Meilisearch task failed: response body: {}", task_res.pretty());
        return Ok(false);
      }
      _ => {} // still processing
    }
  }

  Err(Box::new(ie(
    Box::new(Error::new(ErrorKind::TimedOut, "task_timedout")),
    "meilisearch status poll returned an error",
    Some(ErrorType::TimedOut),
  )))
}
