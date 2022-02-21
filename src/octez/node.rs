use crate::octez::block::{Block, LevelMeta};
use anyhow::{anyhow, Context, Result};
use backoff::{retry, Error, ExponentialBackoff};
use chrono::{DateTime, Utc};
use curl::easy::Easy;
use serde::Deserialize;
use std::str::FromStr;
use std::time::Duration;

#[derive(Clone)]
pub struct NodeClient {
    node_urls: Vec<String>,
    chain: String,
    timeout: Duration,
}

impl NodeClient {
    pub fn new(node_urls: Vec<String>, chain: String) -> Self {
        Self {
            node_urls,
            chain,
            timeout: Duration::from_secs(20),
        }
    }

    /// Return the highest level on the chain
    pub(crate) fn head(&self) -> Result<LevelMeta> {
        let (meta, _) = self.level_json_internal("head")?;
        Ok(meta)
    }

    pub(crate) fn level_json(&self, level: u32) -> Result<(LevelMeta, Block)> {
        self.level_json_internal(&format!("{}", level))
    }

    fn level_json_internal(&self, level: &str) -> Result<(LevelMeta, Block)> {
        let (body, _) = self
            .load(
                &format!("blocks/{}", level),
                Self::load_from_node_retry_on_nonjson,
            )
            .with_context(|| {
                format!("failed to get level_json for level={}", level)
            })?;

        let mut deserializer = serde_json::Deserializer::from_str(&body);
        deserializer.disable_recursion_limit();
        let block: Block = Block::deserialize(&mut deserializer)
            .with_context(|| anyhow!("failed to deserialize block json"))?;

        let meta = LevelMeta {
            level: block.header.level as u32,
            hash: Some(block.hash.clone()),
            prev_hash: Some(block.header.predecessor.clone()),
            baked_at: Some(Self::timestamp_from_block(&block)?),
        };
        Ok((meta, block))
    }

    /// Get all of the data for the contract.
    pub(crate) fn get_contract_storage_definition(
        &self,
        contract_id: &str,
        level: Option<u32>,
    ) -> Result<serde_json::Value> {
        let level = match level {
            Some(x) => format!("{}", x),
            None => "head".to_string(),
        };

        let (_, json) = self
            .load(
                &format!(
                    "blocks/{}/context/contracts/{}/script",
                    level, contract_id
                ),
                Self::load_from_node_retry_on_nonjson,
            )
            .with_context(|| {
                format!(
                    "failed to get script data for contract='{}', level={}",
                    contract_id, level
                )
            })?;

        for entry in json["code"].as_array().ok_or_else(|| {
            anyhow!("malformed script response (missing 'code' field)")
        })? {
            if let Some(prim) = entry.as_object().ok_or_else(|| anyhow!("malformed script response ('code' array element is not an object)"))?.get("prim") {
                if prim == &serde_json::Value::String("storage".to_string()) {
                return Ok(entry["args"].as_array().ok_or_else(|| anyhow!("malformed script response ('storage' entry does not have 'args' field)"))?[0].clone());
                }
            } else {
                return Err(anyhow!("malformed script response ('code' array element does not have a field 'prim')"));
            }
        }

        Err(anyhow!("malformed script response ('code' array does not have 'storage' entry)"))
    }

    fn parse_rfc3339(rfc3339: &str) -> Result<DateTime<Utc>> {
        let fixedoffset = chrono::DateTime::parse_from_rfc3339(rfc3339)?;
        Ok(fixedoffset.with_timezone(&Utc))
    }

    fn timestamp_from_block(block: &Block) -> Result<DateTime<Utc>> {
        Self::parse_rfc3339(block.header.timestamp.as_str())
    }

    fn load<F, O>(&self, endpoint: &str, from_node_func: F) -> Result<O>
    where
        F: Fn(&NodeClient, &str, &str) -> Result<O>,
        O: std::fmt::Debug,
    {
        for node_url in &self.node_urls {
            let res = from_node_func(&self, endpoint, node_url);
            if res.is_ok() {
                return res;
            }
            warn!("failed to call tezos node RPC endpoint on node_url {} (endpoint={}), err: {:?}", node_url, endpoint, res.unwrap_err());
        }
        Err(anyhow!("failed to call tezos node RPC endpoint on all node_urls (endpoint={}", endpoint))
    }

    fn load_from_node_retry_on_nonjson(
        &self,
        endpoint: &str,
        node_url: &str,
    ) -> Result<(String, serde_json::Value)> {
        fn transient_err(e: anyhow::Error) -> Error<anyhow::Error> {
            if e.is::<curl::Error>() {
                let curl_err = e.downcast::<curl::Error>();
                if curl_err.is_err() {
                    let downcast_err = curl_err.err().unwrap();
                    error!("unexpected err on possibly transcient err downcast: {}", downcast_err);
                    return Error::Permanent(downcast_err);
                }

                match curl_err.as_ref().ok().unwrap().code() {
                    // 7: CONNECTION REFUSED
                    // 28: TIMEOUT
                    // 56: RECEIVE ERROR
                    7 | 28 | 56 => {
                        warn!("transient node communication error, retrying.. err={:?}", curl_err);
                        return Error::Transient(anyhow!("{:?}", curl_err));
                    }
                    _ => {}
                };

                let curl_err_val = curl_err.ok().unwrap();
                return Error::Permanent(anyhow!(
                    "{} {} (curl status code: {})",
                    curl_err_val.description(),
                    curl_err_val
                        .extra_description()
                        .map(|descr| format!("(verbose: {})", descr))
                        .unwrap_or_else(|| "".to_string()),
                    curl_err_val.code(),
                ));
            }
            warn!("transient node communication error, retrying.. err={:?}", e);
            Error::Transient(e)
        }
        let op = || -> Result<(String, serde_json::Value)> {
            let body = self.load_from_node(endpoint, node_url)?;

            let mut deserializer = serde_json::Deserializer::from_str(&body);
            deserializer.disable_recursion_limit();
            let deserializer =
                serde_stacker::Deserializer::new(&mut deserializer);
            let json = serde_json::Value::deserialize(deserializer)?;

            Ok((body, json))
        };
        retry(ExponentialBackoff::default(), || {
            op().map_err(transient_err)
        })
        .map_err(|e| anyhow!(e))
    }

    fn load_from_node(&self, endpoint: &str, node_url: &str) -> Result<String> {
        let uri = format!("{}/chains/{}/{}", node_url, self.chain, endpoint);
        debug!("loading: {}", uri);

        let mut resp_data = Vec::new();
        let mut handle = Easy::new();
        handle
            .timeout(self.timeout)
            .with_context(|| {
                format!(
                    "failed to set timeout to curl handle for uri='{}'",
                    uri
                )
            })?;
        handle.url(&uri).with_context(|| {
            format!("failed to call endpoint, uri='{}'", uri)
        })?;
        {
            let mut transfer = handle.transfer();
            transfer.write_function(|new_data| {
                resp_data.extend_from_slice(new_data);
                Ok(new_data.len())
            })?;
            transfer.perform().with_context(|| {
                format!("failed load response for uri='{}'", uri)
            })?;
        }
        let body = std::str::from_utf8(&resp_data).with_context(|| {
            format!("failed to parse response as utf8 for uri='{}'", uri)
        })?;

        Ok(body.to_string())
    }
}

pub(crate) trait StorageGetter {
    fn get_contract_storage(
        &self,
        contract_id: &str,
        level: u32,
    ) -> Result<serde_json::Value>;

    fn get_bigmap_value(
        &self,
        level: u32,
        bigmap_id: i32,
        keyhash: &str,
    ) -> Result<Option<serde_json::Value>>;
}

impl StorageGetter for NodeClient {
    fn get_contract_storage(
        &self,
        contract_id: &str,
        level: u32,
    ) -> Result<serde_json::Value> {
        self.load(
            &format!(
                "blocks/{}/context/contracts/{}/storage",
                level, contract_id
            ),
            Self::load_from_node_retry_on_nonjson,
        )
        .map(|(_, json)| json)
        .with_context(|| {
            format!(
                "failed to get storage for contract='{}', level={}",
                contract_id, level
            )
        })
    }

    fn get_bigmap_value(
        &self,
        level: u32,
        bigmap_id: i32,
        keyhash: &str,
    ) -> Result<Option<serde_json::Value>> {
        let body = self.load(&format!(
            "blocks/{}/context/big_maps/{}/{}",
            level, bigmap_id, keyhash,
        ), Self::load_from_node)
        .with_context(|| {
            format!(
                "failed to get value for bigmap (level={}, bigmap_id={}, keyhash={})",
                level, bigmap_id, keyhash,
            )
        })?;

        Ok(serde_json::Value::from_str(&body).ok())
    }
}
