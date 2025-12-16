use std::time::Duration;

use rdkafka::{
  consumer::{CommitMode, Consumer},
  Offset, TopicPartitionList,
};
use tokio::time::{timeout, Instant};
use tracing::{error, info};

use crate::controller::metrics::set_committed_offset;

use super::Controller;

impl Controller {
  pub async fn consumer_shutdown(&self) {
    let cfg = self.config.get().await.meilisearch.as_ref().unwrap().clone();
    let shutdown_wait_secs = cfg.shutdown_wait_secs() as u64;
    info!("Stopped consuming; waiting up to {}s for in-flight tasks...", shutdown_wait_secs);

    // Wait for join_set tasks to finish up to shutdown_wait_secs
    let deadline = Instant::now() + Duration::from_secs(shutdown_wait_secs);
    let join_set = self.join_set.clone();
    loop {
      // try to join remaining tasks with a small timeout so we can check overall deadline
      match timeout(Duration::from_secs(1), join_set.lock().await.join_next()).await {
        /* a task finished; loop again until none left or deadline */
        Ok(Some(_)) => {}
        // no more tasks
        Ok(None) => break,
        // timed out 1s waiting, check deadline
        Err(_) => {
          if Instant::now() > deadline {
            error!("Timeout waiting for in-flight tasks; exiting anyway.");
            break;
          }
        }
      }
    }

    // final coordinated commit of anything left in highest map (use Sync)
    let mut guard = self.highest_offset.lock().await;
    let consumer = self.consumer.clone();
    if !guard.is_empty() {
      let mut tpl = TopicPartitionList::new();
      for ((topic, partition), &offset) in guard.iter() {
        let commit_off = Offset::from_raw(offset + 1);
        let _ = tpl.add_partition_offset(topic, *partition, commit_off);
      }

      if let Err(err) = consumer.commit(&tpl, CommitMode::Sync) {
        error!("Final commit error: {}", err);
      } else {
        info!("Final batch commit done: {:?}", tpl);
        for ((topic, partition), &offset) in guard.iter() {
          set_committed_offset(topic, *partition, offset);
        }
        guard.clear();
      }
    }
  }
}
