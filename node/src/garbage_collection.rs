use tokio::sync::mpsc::{Receiver};
use consensus::{Block};
use store::Store;
use std::vec::Vec;
use crypto::Hash;

pub struct GarbageCollection {
  rx_store: Receiver<Block>,
  gc_round: u64,
  store: Store,
  block_key: Vec<Vec<u8>>
}

impl GarbageCollection {
  pub fn spawn(
    rx_store: Receiver<Block>,
    store: Store,
    gc_round: u64,
  ) {
    tokio::spawn(async move {
      Self {
        rx_store,
        gc_round,
        store,
        block_key : Vec::new(),
      }
      .run()
      .await;
    });
    
  }

  async fn run(&mut self) {
    let mut current_round = 0;
    while let Some(block) = self.rx_store.recv().await {
      current_round = current_round + 1;
      self.block_key.push(block.digest().to_vec());
      if current_round == self.gc_round {
        while let Some(key) = self.block_key.pop() {
          self.store.delete(key).await;
        }
        current_round = 0;
      }
    }
  }
}