// bcd => better-call.dev
use crate::config::ContractID;
use crate::stats::StatsLogger;
use anyhow::{anyhow, Result};
use backoff::{retry, Error, ExponentialBackoff};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

pub struct BCDClient {
    api_url: String,
    network: String,
    timeout: Duration,
    contract_id: ContractID,
}

impl BCDClient {
    pub(crate) fn new(
        api_url: String,
        network: String,
        contract_id: ContractID,
    ) -> Self {
        Self {
            api_url,
            network,
            timeout: Duration::from_secs(20),
            contract_id,
        }
    }

    pub(crate) fn populate_levels_chan<F>(
        &self,
        node_at_height: F,
        stats: &StatsLogger,
        height_send: &flume::Sender<u32>,
        exclude_levels: &[u32],
    ) -> Result<()>
    where
        F: Fn() -> Result<u32>,
    {
        let mut exclude: HashMap<u32, ()> = HashMap::new();
        for l in exclude_levels {
            exclude.insert(*l, ());
        }

        let mut send_level = |l: u32| -> Result<()> {
            if exclude.contains_key(&l) {
                return Ok(());
            }
            height_send.send(l)?;
            exclude.insert(l, ());
            Ok(())
        };

        let latest_level = self.get_latest_level()?;
        loop {
            let node_height = node_at_height()?;
            if node_height >= latest_level {
                break;
            }
            warn!("waiting for the node to reach the same height ({}) as BCD (currently the node is at height {}). The node is {} levels behind the first block to be indexed. trying again in 1 second..", latest_level, node_height, latest_level - node_height);
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
        send_level(latest_level)?;

        let report_name =
            &format!("better-call.dev '{}'", &self.contract_id.name);

        let mut last_id = None;
        loop {
            let (levels, new_last_id) = self.get_levels_page_with_contract(
                &self.contract_id.address,
                last_id,
            )?;
            if levels.is_empty() {
                break;
            }

            for level in levels {
                send_level(level)?;
            }

            stats.add(report_name, "pages", 1)?;
            stats.set(report_name, "last_id", new_last_id.clone())?;

            last_id = Some(new_last_id);
        }
        Ok(())
    }

    fn get_levels_page_with_contract(
        &self,
        contract_addr: &str,
        last_id: Option<String>,
    ) -> Result<(Vec<u32>, String)> {
        let mut params = vec![("status".to_string(), "applied".to_string())];
        if let Some(last_id) = last_id {
            params.push(("last_id".to_string(), last_id))
        }

        #[derive(Deserialize)]
        struct Operation {
            level: u32,
        }
        #[derive(Deserialize)]
        struct Parsed {
            pub operations: Vec<Operation>,
            #[serde(default)]
            pub last_id: String,
        }
        let parsed: Parsed = self.load(
            format!("contract/{}/{}/operations", self.network, contract_addr),
            &params,
            |resp| {
                let parsed: Parsed = serde_json::from_str(resp)?;
                Ok(parsed)
            },
        )?;

        let mut levels: Vec<u32> = parsed
            .operations
            .iter()
            .map(|op| op.level)
            .collect();
        levels.dedup();

        Ok((levels, parsed.last_id))
    }

    fn get_latest_level(&self) -> Result<u32> {
        #[derive(Deserialize)]
        struct Parsed {
            network: String,
            level: u32,
        }
        let parsed: Vec<Parsed> =
            self.load("head".to_string(), &[], |resp| {
                let parsed: Vec<Parsed> = serde_json::from_str(resp)?;
                Ok(parsed)
            })?;
        match parsed
            .iter()
            .find(|elem| elem.network == self.network)
        {
            Some(elem) => Ok(elem.level),
            None => Err(anyhow!(
                "better-call.dev /head call has no entry for network={}",
                self.network
            )),
        }
    }

    fn load<F, O>(
        &self,
        endpoint: String,
        query_params: &[(String, String)],
        parse_func: F,
    ) -> Result<O>
    where
        F: Fn(&str) -> Result<O>,
    {
        fn transient_err(e: anyhow::Error) -> Error<anyhow::Error> {
            warn!("transient better-call.dev communication error, retrying.. err={}", e);
            Error::Transient {
                err: e,
                retry_after: None,
            }
        }
        let op = || -> Result<O> {
            let uri = format!("{}/{}", self.api_url, endpoint);
            debug!("GET {}..", uri);

            let cli = reqwest::blocking::Client::new();
            let body = cli
                .get(uri)
                .query(query_params)
                .timeout(self.timeout)
                .send()?
                .text()?;
            let parsed: O = parse_func(&body)?;
            Ok(parsed)
        };
        retry(ExponentialBackoff::default(), || {
            op().map_err(transient_err)
        })
        .map_err(|e| anyhow!(e))
    }
}
