mod config;

use std::sync::Arc;

use deadpool_redis::Pool as RedisPool;
use megacommerce_proto::Config as SharedConfig;
use megacommerce_shared::models::errors::InternalError;
use sqlx::{Pool, Postgres};
use tokio::sync::{mpsc::Sender, Mutex, RwLock};

use crate::{config::Common, models::config::Config};

#[derive(Debug, Clone)]
pub struct Server {
  pub(crate) common: Common,
  pub(crate) errors_send: Sender<InternalError>,
  pub(crate) service_config: Arc<Mutex<Config>>,
  pub(crate) shared_config: Arc<RwLock<SharedConfig>>,
  pub(crate) redis: Option<Arc<RwLock<RedisPool>>>,
  pub(crate) db: Option<Arc<RwLock<Pool<Postgres>>>>,
}
