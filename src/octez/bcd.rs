// bcd => better-call.dev
use anyhow::Result;
use serde::Deserialize;
use std::time::Duration;

pub struct BCDClient {
    api_url: String,
    network: String,
    timeout: Duration,
    contract_id: String,
}

impl BCDClient {
    pub fn new(api_url: String, network: String, contract_id: String) -> Self {
        Self {
            api_url,
            network,
            timeout: Duration::from_secs(20),
            contract_id,
        }
    }

    pub fn populate_levels_chan(&self, height_send: flume::Sender<u32>) {
        let mut last_id = None;
        loop {
            let (levels, new_last_id) = self
                .get_levels_page_with_contract(
                    self.contract_id.to_string(),
                    last_id,
                )
                .unwrap();
            if levels.is_empty() {
                break;
            }
            last_id = Some(new_last_id);

            for level in levels {
                height_send.send(level).unwrap();
            }
        }
    }

    fn get_levels_page_with_contract(
        &self,
        contract_id: String,
        last_id: Option<String>,
    ) -> Result<(Vec<u32>, String)> {
        let mut params = vec![];
        if let Some(last_id) = last_id {
            params.push(("last_id".to_string(), last_id))
        }
        let resp = self.load(format!("{}/operations", contract_id), &params)?;

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

    fn load(
        &self,
        endpoint: String,
        query_params: &[(String, String)],
    ) -> Result<String> {
        let uri =
            format!("{}/contract/{}/{}", self.api_url, self.network, endpoint);
        info!("GET {}..", uri);

        let cli = reqwest::blocking::Client::new();
        let body = cli
            .get(uri)
            .query(query_params)
            .timeout(self.timeout)
            .send()?
            .text()?;
        Ok(body)
    }
}
