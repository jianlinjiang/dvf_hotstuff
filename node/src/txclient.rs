use anyhow::{Context, Result};
use bytes::Bytes;
use clap::{crate_name, crate_version, App, AppSettings};
use env_logger::Env;
use futures::sink::SinkExt as _;
use log::{warn};
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use network::DvfMessage;
use network::ReliableSender;
#[tokio::main]
async fn main() -> Result<()> {
  let matches = App::new(crate_name!())
    .version(crate_version!())
    .about("client for HotStuff nodes.")
    .args_from_usage("<ADDR> 'The network address of the node where to send tx transaction'")
    .setting(AppSettings::ArgRequiredElseHelp)
    .get_matches();
  env_logger::Builder::from_env(Env::default().default_filter_or("info"))
    .format_timestamp_millis()
    .init();
  let target = matches
    .value_of("ADDR")
    .unwrap()
    .parse::<SocketAddr>()
    .context("Invalid socket address format")?;
  
  // for i in 0..100 {
    // let mut network = SimpleSender::new();
    // let msg = testInfo {id:0, msg:vec![1,2,3]};
    // network.send(target, Bytes::from(bincode::serialize(&msg).unwrap())).await;
    // let mut network = ReliableSender::new();
    let stream = TcpStream::connect(target)
    .await
    .context(format!("failed to connect to {}", target))?;
    let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
    // let stream = TcpStream::connect(target)
    // .await
    // .context(format!("failed to connect to {}", target))?;
    // let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
    let message = "hello";
    let dvf_message = DvfMessage { validator_id: 1, message: message.as_bytes().to_vec() };
    let serialized_msg = bincode::serialize(&dvf_message).unwrap();
    if let Err(e) = transport.send(Bytes::from(serialized_msg)).await {
      warn!("Failed to send dvf command: {}", e);
    }
    // let _ = network.send(target, Bytes::from(serialized_msg)).await;
    // if let Err(e) = transport.send(Bytes::from(serialized_msg)).await {
    //   warn!("Failed to send transaction: {}", e);
    // } 
    // println!("{}", i);dsfasdlfk;alsdkf;alsdf
  // }
    
  Ok(())
}