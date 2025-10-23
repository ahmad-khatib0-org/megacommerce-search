use std::{mem::take, time::Duration};

use rdkafka::{
  Offset, TopicPartitionList,
  consumer::{CommitMode, Consumer},
};
use tokio::{spawn, time::interval};
use tracing::{debug, error};

use crate::controller::metrics::set_committed_offset;

use super::Controller;

impl Controller {
  pub fn periodic_commit(&self) {
    let highest = self.highest_offset.clone();
    let consumer = self.consumer.clone();
    let commit_interval_ms = 1000u64;

    spawn(async move {
      let mut ticker = interval(Duration::from_millis(commit_interval_ms));
      loop {
        ticker.tick().await;

        // snapshot-and-clear by swapping map out
        let snapshot_map = {
          let mut guard = highest.lock().await;
          if guard.is_empty() {
            continue;
          }
          take(&mut *guard)
        };

        let mut tpl = TopicPartitionList::new();
        for ((topic, partition), offset) in snapshot_map.iter() {
          let commit_off = Offset::from_raw(*offset + 1);
          let _ = tpl.add_partition_offset(topic, *partition, commit_off);
        }

        if tpl.count() > 0 {
          match consumer.commit(&tpl, CommitMode::Async) {
            Ok(_) => {
              debug!("Periodic batched commit dispatched: {:?}", tpl);
              // update metrics for committed offsets
              for ((topic, partition), offset) in snapshot_map.iter() {
                set_committed_offset(topic, *partition, *offset);
              }
            }
            Err(err) => {
              error!("Periodic commit error: {} — remerging snapshot", err);
              // remerge snapshot_map into highest (taking max offsets)
              let mut guard = highest.lock().await;
              for (k, v) in snapshot_map.into_iter() {
                let prev = guard.get(&k).copied().unwrap_or(-1);
                if v > prev {
                  guard.insert(k, v);
                }
              }
            }
          }
        }
      }
    });
  }
}
