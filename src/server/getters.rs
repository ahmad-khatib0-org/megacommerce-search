use deadpool_redis::Pool as RedisPool;
use megacommerce_proto::Config as SharedConfig;
use megacommerce_shared::models::r_lock::RLock;
use sqlx::{Pool, Postgres};

use super::Server;

impl Server {
  /// Return a read-only config to pass downstream
  pub fn config(&self) -> RLock<SharedConfig> {
    RLock::<SharedConfig>(self.shared_config.clone())
  }

  /// Return a read-only redis to pass downstream
  pub fn redis(&self) -> RLock<RedisPool> {
    RLock::<RedisPool>(self.redis.as_ref().unwrap().clone())
  }

  /// Return a read-only postgres database instance to pass downstream
  pub fn db(&self) -> RLock<Pool<Postgres>> {
    RLock::<Pool<Postgres>>(self.db.as_ref().unwrap().clone())
  }
}
