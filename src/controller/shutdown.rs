use std::sync::atomic::Ordering;

use rdkafka::consumer::Consumer;
use tokio::{signal::ctrl_c, spawn};
use tracing::{error, info};

use super::Controller;

impl Controller {
  /// Ctrl+C listener to coordinate graceful shutdown
  pub fn shutdown_listener(&self) {
    let shutdown_notify = self.shutdown_notify.clone();
    let accepting = self.task_accepting.clone();
    let consumer = self.consumer.clone();
    let tx_metrics_shutdown = self.tx_metrics_shutdown.clone();

    spawn(async move {
      if let Err(err) = ctrl_c().await {
        error!("Error waiting for ctrl_c: {}", err);
        return;
      }

      info!("Shutdown signal received (Ctrl+C). Initiating staged drain...");
      let _ = tx_metrics_shutdown.send(());
      drop(tx_metrics_shutdown);

      accepting.store(false, Ordering::SeqCst);
      // pause assigned partitions if possible to stop further deliveries
      match consumer.assignment() {
        Ok(tpl) => {
          if tpl.count() > 0 {
            if let Err(e) = consumer.pause(&tpl) {
              error!("Failed to pause consumer partitions: {}", e);
            } else {
              info!("Paused consumer partitions during shutdown.");
            }
          }
        }
        Err(err) => {
          error!("Could not get consumer assignment to pause: {}", err);
        }
      }

      // notify main loop to break
      shutdown_notify.notify_waiters();
    });
  }
}
