mod commit_coordinator;
mod consumer;
mod consumer_shutdown;
mod message_processor;
mod metrics;
mod shutdown;
mod task_processor;

use std::{
  collections::HashMap,
  net::SocketAddr,
  sync::{atomic::AtomicBool, Arc},
  time::Duration,
};

use deadpool_redis::Pool as RedisPool;
use derive_more::Debug;
use megacommerce_proto::Config;
use megacommerce_shared::models::{
  errors::{BoxedErr, ErrorType, InternalError},
  r_lock::RLock,
};
use metrics::start_metrics_server;
use rdkafka::{
  consumer::{Consumer, StreamConsumer},
  producer::FutureProducer,
  ClientConfig,
};
use reqwest::Client;
use tokio::{
  sync::{
    watch::{self, Receiver, Sender},
    Mutex, Notify, Semaphore,
  },
  task::JoinSet,
};

type Key = (String, i32);

pub struct ControllerArgs {
  pub config: RLock<Config>,
  pub redis: RLock<RedisPool>,
}

#[derive(Debug, Clone)]
pub struct Controller {
  pub config: RLock<Config>,
  pub redis: RLock<RedisPool>,
  #[debug(skip)]
  pub(crate) consumer: Arc<StreamConsumer>,
  #[debug(skip)]
  pub(crate) producer: Arc<FutureProducer>,
  pub(crate) http: Arc<Client>,
  pub(crate) shutdown_notify: Arc<Notify>,
  // Task tracking via JoinSet and concurrency limiter
  pub(crate) semaphore: Arc<Semaphore>,
  pub(crate) join_set: Arc<Mutex<JoinSet<()>>>,
  // Accepting flag: if false, we won't spawn new tasks (we'll process inline).
  pub(crate) task_accepting: Arc<AtomicBool>,
  // Commit-coordinator shared state: map (topic, partition) -> highest_offset_seen (i64)
  pub(crate) highest_offset: Arc<Mutex<HashMap<Key, i64>>>,
  pub(crate) tx_metrics_shutdown: Sender<()>,
  pub(crate) rx_metrics_shutdown: Receiver<()>,
}

impl Controller {
  pub async fn new(ca: ControllerArgs) -> Result<Controller, BoxedErr> {
    let cfg = ca.config.get().await.meilisearch.as_ref().unwrap().clone();
    let ie = |err: BoxedErr, msg: String| {
      InternalError::new("search.controller.new".to_string(), err, ErrorType::Internal, false, msg)
    };

    let consumer: Arc<StreamConsumer> = Arc::new(
      ClientConfig::new()
        .set("bootstrap.servers", cfg.kafka_broker.clone())
        .set("group.id", cfg.kafka_group_id.clone())
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .map_err(|err| ie(Box::new(err), "failed to create kafka consumer".to_string()))?,
    );
    consumer
      .subscribe(&[&cfg.kafak_topic])
      .map_err(|err| ie(Box::new(err), "failed to subscribe to kafka topic".to_string()))?;

    let producer: Arc<FutureProducer> = Arc::new(
      ClientConfig::new()
        .set("bootstrap.servers", &cfg.kafka_broker)
        .create()
        .map_err(|err| ie(Box::new(err), "failed to create kafka producer ".to_string()))?,
    );

    let http = Arc::new(reqwest::Client::new());
    let shutdown_notify = Arc::new(Notify::new());
    let semaphore = Arc::new(Semaphore::new(cfg.max_concurrency() as usize));
    let join_set = Arc::new(Mutex::new(JoinSet::new()));
    let task_accepting = Arc::new(AtomicBool::new(true));
    let highest_offset: Arc<Mutex<HashMap<Key, i64>>> = Arc::new(Mutex::new(HashMap::new()));
    let (tx_metrics_shutdown, rx_metrics_shutdown) = watch::channel(());

    let ctr = Controller {
      config: ca.config,
      redis: ca.redis,
      consumer,
      producer,
      http,
      shutdown_notify,
      semaphore,
      join_set,
      task_accepting,
      highest_offset,
      tx_metrics_shutdown,
      rx_metrics_shutdown,
    };

    Ok(ctr)
  }

  pub async fn run(&self) -> Result<(), BoxedErr> {
    let prom_url =
      self.config.get().await.services.as_ref().unwrap().search_service_prometheus_url.clone();

    start_metrics_server(
      prom_url.parse::<SocketAddr>().unwrap(),
      self.rx_metrics_shutdown.clone(),
      Some(Duration::from_secs(10)),
      Some(700),
    )
    .await?;

    self.periodic_commit();

    self.shutdown_listener();

    self.messages_consumer().await;

    self.consumer_shutdown().await;

    Ok(())
  }
}
