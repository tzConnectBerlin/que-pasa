use crate::octez::block::{Block, LevelMeta};
use crate::octez::node;
use anyhow::Result;
use std::thread;

#[derive(Clone)]
pub struct ConcurrentBlockGetter {
    node_cli: node::NodeClient,
    workers: usize,
}

impl ConcurrentBlockGetter {
    pub fn new(node_cli: node::NodeClient, workers: usize) -> Self {
        Self { node_cli, workers }
    }

    pub fn run(
        &self,
        recv_ch: flume::Receiver<u32>,
        send_ch: flume::Sender<Box<(LevelMeta, Block)>>,
    ) -> Vec<thread::JoinHandle<()>> {
        let mut threads = vec![];

        for _ in 0..self.workers {
            let w_node_cli = self.node_cli.clone();
            let w_recv_ch = recv_ch.clone();
            let w_send_ch = send_ch.clone();
            threads.push(thread::spawn(move || {
                Self::worker_fn(w_node_cli, w_recv_ch, w_send_ch).unwrap();
            }));
        }

        threads
    }

    fn worker_fn(
        node_cli: node::NodeClient,
        recv_ch: flume::Receiver<u32>,
        send_ch: flume::Sender<Box<(LevelMeta, Block)>>,
    ) -> Result<()> {
        for level_height in recv_ch {
            let (_, level, block) = node_cli
                .level_json(level_height)
                .unwrap();
            send_ch.send(Box::new((level, block)))?;
        }
        Ok(())
    }
}
