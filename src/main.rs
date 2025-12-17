mod common;
mod controller;
pub mod models;
mod server;

use megacommerce_shared::models::errors::BoxedErr;
use server::Server;
use tracing::{subscriber::set_global_default, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), BoxedErr> {
  let subscriber = FmtSubscriber::builder().with_max_level(Level::DEBUG).finish();
  set_global_default(subscriber).expect("failed to set log subscriber");

  let server = Server::new().await;
  match server {
    Ok(mut srv) => return srv.run().await,
    Err(e) => Err(e),
  }
}
