use crate::config::Export as _;
use crate::config::{Committee, ConfigError, Parameters, Secret};
use consensus::{Block, Consensus};
use crypto::SignatureService;
use log::{info, error, debug};
use mempool::Mempool;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use crate::garbage_collection::GarbageCollection;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;
use std::net::SocketAddr;
use network::{Receiver as NetworkReceiver};
use futures::future;
/// The default channel capacity for this module.
pub const CHANNEL_CAPACITY: usize = 1_000;
pub const DEFAULT_GC_ROUND: u64 = 1_000_00;
pub struct Node {
    pub commit: Receiver<Block>,
    // pub tx_store: Sender<Block>,
    pub signal: exit_future::Signal
}


fn with_wildcard_ip(mut addr: SocketAddr) -> SocketAddr {
    addr.set_ip("0.0.0.0".parse().unwrap());
    addr
}

impl Node {
    pub async fn new(
        committee_file: &str,
        key_file: &str,
        store_path: &str,
        parameters: Option<&str>,
    ) -> Result<Self, ConfigError> {
        
        // Read the committee and secret key from file.
        let committee = Committee::read(committee_file)?;
        let secret = Secret::read(key_file)?;
        let name = secret.name;
        let secret_key = secret.secret;

        // Load default parameters if none are specified.
        let parameters = match parameters {
            Some(filename) => Parameters::read(filename)?,
            None => Parameters::default(),
        };

        // Run the signature service.
        
        let tx_handler_map = Arc::new(RwLock::new(HashMap::new()));
        let mempool_handler_map = Arc::new(RwLock::new(HashMap::new()));
        let consensus_handler_map = Arc::new(RwLock::new(HashMap::new()));
        let (signal, exit) = exit_future::signal();

        let transaction_address = with_wildcard_ip(committee.mempool.transactions_address(&name).expect("Our public key is not in the committee"));
        NetworkReceiver::spawn(transaction_address, Arc::clone(&tx_handler_map), "transaction");
        info!("Node {} listening to client transactions on {}", secret.name, transaction_address);

        let mempool_address = with_wildcard_ip(committee.mempool.mempool_address(&name).expect("Our public key is not in the committee"));
        NetworkReceiver::spawn(mempool_address, Arc::clone(&mempool_handler_map), "mempool");
        info!("Node {} listening to mempool messages on {}", secret.name, mempool_address);

        let consensus_address = with_wildcard_ip(committee.consensus.address(&name).expect("Our public key is not in the committee"));
        NetworkReceiver::spawn(consensus_address, Arc::clone(&consensus_handler_map), "consensus");
        info!(
            "Node {} listening to consensus messages on {}",
            secret.name, consensus_address
        );


        let store_path = format!("db_{}", 0);
        let store = Store::new(&store_path).expect("Failed to create store");
        let tmp_parameters = Parameters::default();
        let tmp_committee = Committee::read(committee_file)?;
        // Make a new mempool.
        let (tx_commit, rx_commit) = channel(CHANNEL_CAPACITY);
        let (tx_consensus_to_mempool, rx_consensus_to_mempool) = channel(CHANNEL_CAPACITY);
        let (tx_mempool_to_consensus, rx_mempool_to_consensus) = channel(CHANNEL_CAPACITY);
        let signature_service = SignatureService::new(secret_key.clone());
        // let (tx_store, rx_store) = channel(CHANNEL_CAPACITY);
        Mempool::spawn(
            name,
            tmp_committee.mempool,
            tmp_parameters.mempool,
            store.clone(),
            rx_consensus_to_mempool,
            tx_mempool_to_consensus,
            0,
            tx_handler_map.clone(),
            mempool_handler_map.clone(),
            exit.clone()
        );

        // Run the consensus core.
        Consensus::spawn(
            name,
            tmp_committee.consensus,
            tmp_parameters.consensus,
            signature_service,
            store.clone(),
            rx_mempool_to_consensus,
            tx_consensus_to_mempool,
            tx_commit,
            0,
            consensus_handler_map.clone(),
            exit.clone()
        );

        for i in 1..15 {
            let store_path = format!("db_{}", i);
            let store = Store::new(&store_path).expect("Failed to create store");
            let tmp_parameters = Parameters::default();
            let tmp_committee = Committee::read(committee_file)?;
            // Make a new mempool.
            let (tx_commit, mut rx_commit) = channel(CHANNEL_CAPACITY);
            let (tx_consensus_to_mempool, rx_consensus_to_mempool) = channel(CHANNEL_CAPACITY);
            let (tx_mempool_to_consensus, rx_mempool_to_consensus) = channel(CHANNEL_CAPACITY);
            let signature_service = SignatureService::new(secret_key.clone());
            // let (tx_store, rx_store) = channel(CHANNEL_CAPACITY);
            Mempool::spawn(
                name,
                tmp_committee.mempool,
                tmp_parameters.mempool,
                store.clone(),
                rx_consensus_to_mempool,
                tx_mempool_to_consensus,
                i,
                tx_handler_map.clone(),
                mempool_handler_map.clone(),
                exit.clone()
            );

            // Run the consensus core.
            Consensus::spawn(
                name,
                tmp_committee.consensus,
                tmp_parameters.consensus,
                signature_service,
                store.clone(),
                rx_mempool_to_consensus,
                tx_consensus_to_mempool,
                tx_commit,
                i,
                consensus_handler_map.clone(),
                exit.clone()
            );

            tokio::spawn(async move {
                while let Some(block) = rx_commit.recv().await {
                    if block.payload.is_empty() {
                        error!("{} va {} round has transaction", i, block.round);
                    }
                }
            });
        }

        // Make the data store.
        // let store = Store::new(store_path).expect("Failed to create store");
        // GarbageCollection::spawn(
        //     rx_store,
        //     store,
        //     DEFAULT_GC_ROUND
        // );

        info!("Node {} successfully booted", name);
        Ok(Self { commit: rx_commit, signal: signal })
    }

    pub fn print_key_file(filename: &str) -> Result<(), ConfigError> {
        Secret::new().write(filename)
    }

    pub async fn analyze_block(&mut self) {
        while let Some(_block) = self.commit.recv().await {
            // self.tx_store.send(_block.clone()).await.unwrap();

            // This is where we can further process committed block.
            if _block.round == DEFAULT_GC_ROUND {
                error!("{} round", DEFAULT_GC_ROUND);
            }
            if _block.payload.is_empty() {
                debug!("empty block");
            }
            else {
                error!("{} round", _block.round);
                error!("has transaction");
            }
        }
    }
}
