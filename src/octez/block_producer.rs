use crate::octez::bcd;
use crate::octez::block::{Block, LevelMeta};
use crate::octez::node;
use anyhow::Result;
use std::sync::mpsc::Sender;

#[derive(Clone)]
pub struct BlockProducer {
    node_cli: node::NodeClient,
}

impl BlockProducer {
    pub fn new(node_cli: &node::NodeClient) -> Self {
        Self {
            node_cli: node_cli.clone(),
        }
    }

    pub fn run(
        &self,
        recv_ch: flume::Receiver<u32>,
        send_ch: flume::Sender<Box<(LevelMeta, Block)>>,
    ) -> Result<()> {
        for level_height in recv_ch {
            let level = self.node_cli.level(level_height)?;
            let (_, block) = self.node_cli.level_json(level_height)?;
            println!("got block data for level {}", level_height);

            send_ch.send(Box::new((level, block)))?;
        }
        Ok(())
    }
}
