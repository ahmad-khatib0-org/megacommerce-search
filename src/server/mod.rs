mod config;
mod getters;
mod init;

use std::sync::Arc;

use deadpool_redis::Pool as RedisPool;
use megacommerce_proto::Config as SharedConfig;
use megacommerce_shared::models::{
  errors::{BoxedErr, ErrorType, InternalError},
  translate::translations_init,
};
use sqlx::{Pool, Postgres};
use tokio::{
  spawn,
  sync::{
    mpsc::{channel, Receiver, Sender},
    Mutex, RwLock,
  },
};

use crate::{
  common::{Common, CommonArgs},
  controller::{Controller, ControllerArgs},
  models::config::Config,
};

#[derive(Debug, Clone)]
pub struct Server {
  pub(crate) common: Common,
  pub(crate) errors_send: Sender<InternalError>,
  pub(crate) service_config: Arc<Mutex<Config>>,
  pub(crate) shared_config: Arc<RwLock<SharedConfig>>,
  pub(crate) redis: Option<Arc<RwLock<RedisPool>>>,
  pub(crate) db: Option<Arc<RwLock<Pool<Postgres>>>>,
}

impl Server {
  pub async fn new() -> Result<Server, BoxedErr> {
    let (tx, rx) = channel::<InternalError>(100);

    let mut srv: Server = Server {
      common: Common::default(),
      errors_send: tx,
      service_config: Arc::new(Mutex::new(Config::default())),
      shared_config: Arc::new(RwLock::new(SharedConfig::default())),
      redis: None,
      db: None,
    };

    srv.init_service_config().await?;

    let common_args = CommonArgs { service_config: srv.service_config.lock().await.clone() };
    srv.common = Common::new(common_args).await?;

    {
      let cfg = srv.common.config(|config| config.clone()).await;
      let mut prev_cfg = srv.shared_config.write().await;
      *prev_cfg = cfg;
    }

    let srv_clone = srv.clone();
    spawn(async move { srv_clone.errors_listener(rx).await });

    Ok(srv)
  }

  pub async fn run(&mut self) -> Result<(), BoxedErr> {
    let ie = |msg: &str, err: BoxedErr| InternalError {
      err_type: ErrorType::Internal,
      temp: false,
      err,
      msg: msg.into(),
      path: "search.server.run".into(),
    };

    self.redis = Some(Arc::new(RwLock::new(self.init_redis().await?)));
    // self.db = Some(Arc::new(RwLock::new(self.init_database().await?)));

    let translations = self.common.translations(|trans| trans.clone()).await;
    translations_init(
      translations,
      5,
      cfg.default_client_locale.unwrap_or_default(),
      cfg.available_locales,
    )
    .map_err(|err| ie("error init trans", Box::new(err)))?;

    let ctr_args = ControllerArgs { redis: self.redis(), config: self.config() };
    let ctr = Controller::new(ctr_args).await?;
    ctr.run().await?;

    Ok(())
  }

  pub async fn errors_listener(&self, mut receiver: Receiver<InternalError>) {
    while let Some(msg) = receiver.recv().await {
      println!("received an internal error: {}", msg)
    }
  }
}
