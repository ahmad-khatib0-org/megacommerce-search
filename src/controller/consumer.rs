use base64::engine::{general_purpose, Engine as _};
use rdkafka::{
  consumer::{CommitMode, Consumer},
  producer::FutureRecord,
  Message,
};
use serde_json::Value;
use std::{
  str::from_utf8,
  sync::atomic::Ordering,
  time::{Duration, Instant},
};
use tokio::{select, time::sleep};
use tracing::{debug, error, info};

use crate::controller::{
  message_processor::message_processor,
  metrics::{
    dec_inflight, inc_failed, inc_inflight, inc_processed, observe_meili_duration_secs,
    set_committed_offset,
  },
};

use super::Controller;

impl Controller {
  // TODO: track errors, replace serde_json::json!(x) with generated proto messages...
  pub async fn messages_consumer(&self) {
    let shutdown_notify = self.shutdown_notify.clone();
    let cfg = self.config.get().await.meilisearch.as_ref().unwrap().clone();
    let dlq_topic = cfg.kafak_topic_dlq.clone();
    let consumer = self.consumer.clone();

    loop {
      select! {
        _ = shutdown_notify.notified() => {
          info!("Shutdown requested — breaking consumption loop.");
          break;
        }
        maybe_msg = consumer.recv() => {
          match maybe_msg {
            Err(e) => {
              error!("Kafka receive error: {}", e);
              sleep(Duration::from_secs(1)).await;
            }
            Ok(msg) => {
              let http = self.http.clone();
              // Extract payload and convert to String early to avoid lifetime issues
              let payload_str = if let Some(payload_bytes) = msg.payload() {
                match from_utf8(payload_bytes) {
                  Ok(s) => Some(s.to_string()),
                  Err(utf8_err) => {
                    error!("Invalid UTF-8 payload: {}", utf8_err);
                    // Handle UTF-8 error and continue
                    let dlq_obj = serde_json::json!({
                      "original_bytes_base64": general_purpose::STANDARD.encode(payload_bytes),
                      "error": format!("invalid utf8: {}", utf8_err),
                      "ts": chrono::Utc::now().timestamp_millis()
                    });
                    let dlq_payload = dlq_obj.to_string();
                    let _ = self.producer.send(
                      FutureRecord::to(&dlq_topic).payload(&dlq_payload).key(""),
                      Duration::from_secs(1)
                    ).await;
                    if let Err(e) = consumer.commit_message(&msg, CommitMode::Async) {
                      error!("Failed to commit offset for invalid-utf8 message: {}", e);
                    }
                    inc_failed("invalid_utf8");
                    continue;
                  }
                }
              } else {
                // Empty payload - commit and continue
                if let Err(e) = consumer.commit_message(&msg, CommitMode::Async) {
                  error!("Failed to commit offset for empty payload: {}", e);
                }
                continue;
              };

              // Safe because we continue on error above
              let payload_str = payload_str.unwrap();

              // Clone what we need for the spawned task
              let key_topic = msg.topic().to_string();
              let key_partition = msg.partition();
              let key_offset = msg.offset();
              let dlq_topic = dlq_topic.clone();
              let cfg = cfg.clone();

              if self.task_accepting.load(Ordering::SeqCst) {
                // Clone all the Arc fields we need for the spawned task
                let semaphore = self.semaphore.clone();
                let highest = self.highest_offset.clone();
                let join_set = self.join_set.clone();
                let producer = self.producer.clone();

                // Parse op for metrics
                let permit_fut = semaphore.acquire_owned();

                // Spawn the task

                join_set.lock().await.spawn(async move {
                  let _permit = match permit_fut.await {
                    Ok(p) => p,
                    Err(_) => {
                      error!("Semaphore closed unexpectedly");
                      return;
                    }
                  };

                  inc_inflight();
                  let start = Instant::now();

                  let process_res = message_processor(&payload_str, &http, &cfg).await;
                  match process_res {
                    Ok(()) => {
                      // Update highest offset
                      let mut guard = highest.lock().await;
                      let key = (key_topic.clone(), key_partition);
                      let prev = guard.get(&key).copied().unwrap_or(-1);
                      if key_offset > prev {
                        guard.insert(key, key_offset);
                      }
                      debug!("Marked processed offset {} for {}[{}]", key_offset, key_topic, key_partition);
                      inc_processed();
                    }
                    Err(err) => {
                      error!("Processing failed for message {}[{}] @ {}: {}", key_topic, key_partition, key_offset, err);
                      // Send to DLQ
                      let original_json = serde_json::from_str::<Value>(&payload_str)
                        .unwrap_or(Value::String(payload_str.clone()));
                      let dlq_obj = serde_json::json!({
                        "original": original_json,
                        "error": format!("{}", err),
                        "ts": chrono::Utc::now().timestamp_millis()
                      });
                      let dlq_payload = dlq_obj.to_string();
                      let _ = producer.send(
                        FutureRecord::to(&dlq_topic).payload(&dlq_payload).key(""),
                        Duration::from_secs(1)
                      ).await;
                      // Still mark as processed to avoid infinite retry
                      let mut guard = highest.lock().await;
                      let key = (key_topic.clone(), key_partition);
                      let prev = guard.get(&key).copied().unwrap_or(-1);
                      if key_offset > prev {
                        guard.insert(key, key_offset);
                      }
                      inc_failed("processing");
                    }
                  }

                  let elapsed = start.elapsed();
                  observe_meili_duration_secs(elapsed.as_secs_f64());
                  dec_inflight();
                });
              } else {
                // Draining mode - process inline (using &self)
                info!("Draining mode: processing message inline before shutdown.");
                inc_inflight();
                let start = Instant::now();
                let res_inline = message_processor(&payload_str, &http, &cfg).await;

                match res_inline {
                  Ok(()) => {
                    if let Err(e) = consumer.commit_message(&msg, CommitMode::Async) {
                      error!("Failed to commit offset (inline processing): {}", e);
                    } else {
                      set_committed_offset(&key_topic, key_partition, key_offset);
                      inc_processed();
                    }
                  }
                  Err(err) => {
                    error!("Inline processing failed during drain: {}", err);
                    let original_json = serde_json::from_str::<Value>(&payload_str)
                      .unwrap_or(Value::String(payload_str.clone()));
                    let dlq_obj = serde_json::json!({
                      "original": original_json,
                      "error": format!("{}", err),
                      "ts": chrono::Utc::now().timestamp_millis()
                    });

                    let dlq_payload = dlq_obj.to_string();
                    let _ = self.producer.send(
                      FutureRecord::to(&dlq_topic).payload(&dlq_payload).key(""),
                      Duration::from_secs(1)
                    ).await;
                    if let Err(e) = consumer.commit_message(&msg, CommitMode::Async) {
                      error!("Failed to commit offset after inline DLQ: {}", e);
                    }
                    inc_failed("processing");
                  }
                }

                let elapsed = start.elapsed();
                observe_meili_duration_secs(elapsed.as_secs_f64());
                dec_inflight();
              }
            }
          }
        }
      }
    }
  }
}
