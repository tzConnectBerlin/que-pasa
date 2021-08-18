use crate::octez::block::{Block, LevelMeta};
use anyhow::{anyhow, Context, Result};
use backoff::{retry, Error, ExponentialBackoff};
use chrono::{DateTime, Utc};
use curl::easy::Easy;
use json::JsonValue;
use std::time::Duration;

#[derive(Clone)]
pub struct NodeClient {
    node_url: String,
    chain: String,
    timeout: Duration,
}

impl NodeClient {
    pub fn new(node_url: String, chain: String) -> Self {
        Self {
            node_url,
            chain,
            timeout: Duration::from_secs(5),
        }
    }

    /// Return the highest level on the chain
    pub(crate) fn head(&self) -> Result<LevelMeta> {
        let json = self
            .load("blocks/head")
            .with_context(|| "failed to get block head")?;
        Ok(LevelMeta {
            _level: json["header"]["level"]
                .as_u32()
                .ok_or_else(|| anyhow!("Couldn't get level from node"))?,
            hash: Some(json["hash"].to_string()),
            baked_at: Some(Self::timestamp_from_block(&json)?),
        })
    }

    pub(crate) fn level(&self, level: u32) -> Result<LevelMeta> {
        let (json, block) = self.level_json(level)?;
        Ok(LevelMeta {
            _level: block.header.level as u32,
            hash: Some(block.hash),
            baked_at: Some(Self::timestamp_from_block(&json)?),
        })
    }

    pub(crate) fn level_json(&self, level: u32) -> Result<(JsonValue, Block)> {
        let resp = self
            .load(&format!("blocks/{}", level))
            .with_context(|| {
                format!("failed to get level_json for level={}", level)
            })?;
        let block: Block = serde_json::from_str(&resp.to_string())
            .with_context(|| {
                format!(
                    "failed to parse level_json into Block for level={}",
                    level
                )
            })?;
        Ok((resp, block))
    }

    /// Get all of the data for the contract.
    pub(crate) fn get_contract_script(
        &self,
        contract_id: &str,
        level: Option<u32>,
    ) -> Result<JsonValue> {
        let level = match level {
            Some(x) => format!("{}", x),
            None => "head".to_string(),
        };

        self.load(&format!(
            "blocks/{}/context/contracts/{}/script",
            level, contract_id
        ))
        .with_context(|| {
            format!(
                "failed to get script data for contract='{}', level={}",
                contract_id, level
            )
        })
    }

    fn parse_rfc3339(rfc3339: &str) -> Result<DateTime<Utc>> {
        let fixedoffset = chrono::DateTime::parse_from_rfc3339(rfc3339)?;
        Ok(fixedoffset.with_timezone(&Utc))
    }

    fn timestamp_from_block(json: &JsonValue) -> Result<DateTime<Utc>> {
        Self::parse_rfc3339(
            json["header"]["timestamp"]
                .as_str()
                .ok_or_else(|| {
                    anyhow!(
                        "Couldn't parse string {:?}",
                        json["header"]["timestamp"]
                    )
                })?,
        )
    }

    fn load(&self, endpoint: &str) -> Result<JsonValue> {
        let op = || -> Result<JsonValue> {
            let uri =
                format!("{}/chains/{}/{}", self.node_url, self.chain, endpoint);
            debug!("loading: {}", uri);

            let mut response = Vec::new();
            let mut handle = Easy::new();
            handle.timeout(self.timeout)?;
            handle.url(&uri)?;
            {
                let mut transfer = handle.transfer();
                transfer.write_function(|new_data| {
                    response.extend_from_slice(new_data);
                    Ok(new_data.len())
                })?;
                transfer.perform()?;
            }
            let body = std::str::from_utf8(&response).with_context(|| {
                format!("failed to read response for uri='{}'", uri)
            })?;
            let json = json::parse(body).with_context(|| {
                format!(
                    "failed to parse json for uri='{}', body: {}",
                    uri, body
                )
            })?;
            Ok(json)
        };
        retry(ExponentialBackoff::default(), || {
            op().map_err(Error::Transient)
        })
        .map_err(|e| anyhow!(e))
    }
}
