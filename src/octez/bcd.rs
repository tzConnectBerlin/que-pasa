// bcd => better-call.dev
use anyhow::{anyhow, Result};
use backoff::{retry, Error, ExponentialBackoff};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

pub struct BCDClient {
    api_url: String,
    network: String,
    timeout: Duration,
    contract_id: String,
    exclude_levels: HashMap<u32, ()>,
}

impl BCDClient {
    pub fn new(
        api_url: String,
        network: String,
        contract_id: String,
        exclude_levels: &[u32],
    ) -> Self {
        let mut excl: HashMap<u32, ()> = HashMap::new();
        for l in exclude_levels {
            excl.insert(*l, ());
        }
        Self {
            api_url,
            network,
            timeout: Duration::from_secs(20),
            contract_id,
            exclude_levels: excl,
        }
    }

    pub fn populate_levels_chan(
        &self,
        height_send: flume::Sender<u32>,
    ) -> Result<()> {
        let mut last_id = None;
        let latest_level = self.get_latest_level()?;
        if !self
            .exclude_levels
            .contains_key(&latest_level)
        {
            height_send.send(latest_level)?;
        }

        loop {
            let (levels, new_last_id) = self.get_levels_page_with_contract(
                self.contract_id.to_string(),
                last_id,
            )?;
            if levels.is_empty() {
                break;
            }
            last_id = Some(new_last_id);

            for level in levels {
                if self.exclude_levels.contains_key(&level) {
                    continue;
                }
                height_send.send(level)?;
            }
        }
        Ok(())
    }

    fn get_levels_page_with_contract(
        &self,
        contract_id: String,
        last_id: Option<String>,
    ) -> Result<(Vec<u32>, String)> {
        let mut params = vec![("status".to_string(), "applied".to_string())];
        if let Some(last_id) = last_id {
            params.push(("last_id".to_string(), last_id))
        }
        let resp = self.load(
            format!("contract/{}/{}/operations", self.network, contract_id),
            &params,
        )?;

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
        let parsed: Parsed = serde_json::from_str(&resp)?;

        let mut levels: Vec<u32> = parsed
            .operations
            .iter()
            .map(|op| op.level)
            .collect();
        levels.dedup();

        Ok((levels, parsed.last_id))
    }

    fn get_latest_level(&self) -> Result<u32> {
        let resp = self.load("head".to_string(), &[])?;
        #[derive(Deserialize)]
        struct Parsed {
            network: String,
            level: u32,
        }
        let parsed: Vec<Parsed> = serde_json::from_str(&resp)?;
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

    fn load(
        &self,
        endpoint: String,
        query_params: &[(String, String)],
    ) -> Result<String> {
        fn transient_err(e: anyhow::Error) -> Error<anyhow::Error> {
            warn!("transient better-call.dev communication error, retrying.. err={}", e);
            Error::Transient(e)
        }
        let op = || -> Result<String> {
            let uri = format!("{}/{}", self.api_url, endpoint);
            info!("GET {}..", uri);

            let cli = reqwest::blocking::Client::new();
            let body = cli
                .get(uri)
                .query(query_params)
                .timeout(self.timeout)
                .send()?
                .text()?;
            Ok(body)
        };
        retry(ExponentialBackoff::default(), || {
            op().map_err(transient_err)
        })
        .map_err(|e| anyhow!(e))
    }
}
