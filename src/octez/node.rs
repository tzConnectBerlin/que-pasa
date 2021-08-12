use crate::err;
use crate::error::Res;
use crate::octez::block::Block;
use chrono::{DateTime, Utc};
use curl::easy::Easy;
use json::JsonValue;
use std::error::Error;

pub struct NodeClient {
    node_url: String,
}

#[derive(Clone, Debug)]
pub struct Level {
    pub _level: u32,
    pub hash: Option<String>,
    pub baked_at: Option<DateTime<Utc>>,
}

impl NodeClient {
    pub fn new(node_url: String) -> NodeClient {
        NodeClient { node_url }
    }

    /// Return the highest level on the chain
    pub(crate) fn head(&self) -> Res<Level> {
        let json = self.load(&format!("{}/chains/main/blocks/head", self.node_url))?;
        Ok(Level {
            _level: json["header"]["level"]
                .as_u32()
                .ok_or_else(|| err!("Couldn't get level from node"))?,
            hash: Some(json["hash"].to_string()),
            baked_at: Some(Self::timestamp_from_block(&json)?),
        })
    }

    pub(crate) fn level(&self, level: u32) -> Res<Level> {
        let (json, block) = self.level_json(level)?;
        Ok(Level {
            _level: block.header.level as u32,
            hash: Some(block.hash),
            baked_at: Some(Self::timestamp_from_block(&json)?),
        })
    }

    pub(crate) fn level_json(&self, level: u32) -> Res<(JsonValue, Block)> {
        let res = self.load(&format!("/chains/main/blocks/{}", level))?;
        let block: Block = serde_json::from_str(&res.to_string())?;
        Ok((res, block))
    }

    /// Get all of the data for the contract.
    pub(crate) fn get_contract_script(
        &self,
        contract_id: &str,
        level: Option<u32>,
    ) -> Result<JsonValue, Box<dyn Error>> {
        let level = match level {
            Some(x) => format!("{}", x),
            None => "head".to_string(),
        };
        self.load(&format!(
            "chains/main/blocks/{}/context/contracts/{}/script",
            level, contract_id
        ))
    }

    fn parse_rfc3339(rfc3339: &str) -> Res<DateTime<Utc>> {
        let fixedoffset = chrono::DateTime::parse_from_rfc3339(rfc3339)?;
        Ok(fixedoffset.with_timezone(&Utc))
    }

    fn timestamp_from_block(json: &JsonValue) -> Res<DateTime<Utc>> {
        Self::parse_rfc3339(
            json["header"]["timestamp"]
                .as_str()
                .ok_or_else(|| err!("Couldn't parse string {:?}", json["header"]["timestamp"]))?,
        )
    }

    fn load(&self, endpoint: &str) -> Result<JsonValue, Box<dyn Error>> {
        let uri = format!("{}/{}", self.node_url, endpoint);

        debug!("Loading: {}", uri,);
        let mut response = Vec::new();
        let mut handle = Easy::new();
        handle.timeout(std::time::Duration::from_secs(20))?;
        handle.url(&uri)?;
        {
            let mut transfer = handle.transfer();
            transfer.write_function(|new_data| {
                response.extend_from_slice(new_data);
                Ok(new_data.len())
            })?;
            transfer.perform()?;
        }
        let json = json::parse(std::str::from_utf8(&response)?)?;
        Ok(json)
    }
}
