mod config;
mod init;
mod trans;

use std::{collections::HashMap, error::Error, sync::Arc};

use derive_more::Display;
use megacommerce_proto::{
  common_service_client::CommonServiceClient, Config as SharedConfig, TranslationElements,
};
use megacommerce_shared::models::errors::BoxedErr;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::models::config::Config;

#[derive(Debug, Display)]
pub struct CommonArgs {
  pub service_config: Config,
}

#[derive(Debug, Clone)]
pub struct Common {
  pub(super) client: Option<CommonServiceClient<Channel>>,
  pub(super) service_config: Config,
  pub(super) shared_config: Arc<Mutex<SharedConfig>>,
  pub(super) translations: Arc<Mutex<HashMap<String, TranslationElements>>>,
}

impl Common {
  /// New initialize connection to the common service, initialize configurations
  pub async fn new(ca: CommonArgs) -> Result<Common, BoxedErr> {
    let mut com = Common {
      client: None,
      service_config: ca.service_config,
      shared_config: Arc::new(Mutex::new(SharedConfig::default())),
      translations: Arc::new(Mutex::new(HashMap::new())),
    };

    com.client = Some(com.init_client().await?);

    match com.config_get().await {
      Ok(res) => {
        let mut config = com.shared_config.lock().await;
        *config = res;
      }
      Err(err) => return Err(err),
    }

    {
      let res = com.translations_get().await?;
      let mut tr = com.translations.lock().await;
      *tr = res;
    }

    Ok(com)
  }

  /// Close the client connection by dropping it
  /// When `client` is dropped, the underlying Channel drops and closes the connection.
  pub fn close(&mut self) {
    self.client = None;
  }

  /// Reconnect: close old client if any, then create new one
  pub async fn reconnect(&mut self) -> Result<(), BoxedErr> {
    self.close(); // drop old client if present
    let client = self.init_client().await?;
    self.client = Some(client);
    Ok(())
  }

  /// Accessor for client with error if not connected
  pub fn client(&mut self) -> Result<&mut CommonServiceClient<Channel>, Box<dyn Error>> {
    self.client.as_mut().ok_or_else(|| "common service client is not connected".into())
  }

  /// Config returns a read only access to config
  pub async fn config<T, F>(&self, f: F) -> T
  where
    F: FnOnce(&SharedConfig) -> T,
  {
    let config = self.shared_config.lock().await;
    f(&config)
  }

  /// Translations returns a read only access to translations
  pub async fn translations<T, F>(&self, f: F) -> T
  where
    F: FnOnce(&HashMap<String, TranslationElements>) -> T,
  {
    let trans = self.translations.lock().await;
    f(&trans)
  }
}
