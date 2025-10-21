use derive_more::Display;
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize, Display)]
pub struct Config {
  pub service: ServiceConfig,
}

#[derive(Clone, Debug, Deserialize, Display)]
#[display("ServiceConfig {env} {common_service_grpc_url}")]
pub struct ServiceConfig {
  pub env: String,
  pub common_service_grpc_url: String,
}

impl Default for Config {
  fn default() -> Self {
    Config {
      service: ServiceConfig {
        env: "".to_string(),
        common_service_grpc_url: "".to_string(),
      },
    }
  }
}
