use std::{env, fs};

use megacommerce_shared::models::errors::{BoxedErr, ErrorType, InternalError};

use crate::models::config::Config;

use super::Server;

impl Server {
  pub async fn init_servie_config(&self) -> Result<(), BoxedErr> {
    let env = env::var("ENV").unwrap_or_else(|_| "dev".into());
    let ie = |msg: String, err: BoxedErr| InternalError {
      err_type: ErrorType::ConfigError,
      temp: false,
      path: "search.server.init_service_config".to_string(),
      msg,
      err,
    };

    let yaml_string = fs::read_to_string(format!("config.{}.yaml", env))
      .map_err(|err| ie("failed to load service config file".to_string(), Box::new(err)))?;

    let parsed_config: Config = serde_yaml::from_str(&yaml_string)
      .map_err(|err| ie("failed to parse config data".to_string(), Box::new(err)))?;

    let mut config = self.service_config.lock().await;
    *config = parsed_config;

    Ok(())
  }
}
