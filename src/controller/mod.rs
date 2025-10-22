mod metrics;

use deadpool_redis::Pool as RedisPool;
use megacommerce_proto::Config;
use megacommerce_shared::models::{errors::BoxedErr, r_lock::RLock};

pub struct ControllerArgs {
  pub config: RLock<Config>,
  pub redis: RLock<RedisPool>,
}

#[derive(Debug)]
pub struct Controller {
  pub config: RLock<Config>,
  pub redis: RLock<RedisPool>,
}

impl Controller {
  pub async fn new(ca: ControllerArgs) -> Result<Controller, BoxedErr> {
    let ctr = Controller { config: ca.config, redis: ca.redis };

    Ok(ctr)
  }

  pub async fn run(self) -> Result<(), BoxedErr> {
    Ok(())
  }
}
