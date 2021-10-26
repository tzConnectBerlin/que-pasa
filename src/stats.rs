use anyhow::{anyhow, ensure, Context, Result};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct StatsLogger {
    ident: String,
    interval: Duration,

    stats: Arc<Mutex<Stats>>,
}

#[derive(Debug)]
struct Stats {
    counters: HashMap<String, usize>,
    values: HashMap<String, String>,
}

impl Stats {
    pub(crate) fn new() -> Self {
        Self {
            counters: HashMap::new(),
            values: HashMap::new(),
        }
    }

    pub(crate) fn print_report(&self, ident: &str, at_interval: &Duration) {
        let mut counters = self
            .counters
            .clone()
            .into_iter()
            .collect::<Vec<(String, usize)>>();
        counters.sort_by_key(|(f, _)| f.clone());

        let counters_log: String = counters
            .iter()
            .map(|(field, c)| {
                format!(
                    "\n\t{}:\t{} total,\t{} per/minute",
                    field,
                    c,
                    ((c * 60) as u64) / at_interval.as_secs()
                )
            })
            .collect::<Vec<String>>()
            .join("");

        let mut values = self
            .values
            .clone()
            .into_iter()
            .collect::<Vec<(String, String)>>();
        values.sort_by_key(|(f, _)| f.clone());

        let values_log: String = values
            .iter()
            .map(|(field, value)| format!("\n\t{}: {}", field, value))
            .collect::<Vec<String>>()
            .join("");

        info!(
            "{} {:?} report:{}{}",
            ident, at_interval, counters_log, values_log
        );
    }
}

impl StatsLogger {
    pub(crate) fn new(ident: String, interval: Duration) -> Self {
        Self {
            ident,
            interval,

            stats: Arc::new(Mutex::new(Stats::new())),
        }
    }

    pub(crate) fn add(&self, field: String, n: usize) -> Result<()> {
        let mut stats = self
            .stats
            .lock()
            .map_err(|_| anyhow!("failed to lock level_floor mutex"))?;
        let c = stats.counters.get(&field).unwrap_or(&0);
        let c_ = c + n;
        (*stats).counters.insert(field, c_);
        Ok(())
    }

    pub(crate) fn set(&self, field: String, value: String) -> Result<()> {
        let mut stats = self
            .stats
            .lock()
            .map_err(|_| anyhow!("failed to lock level_floor mutex"))?;
        (*stats).values.insert(field, value);
        Ok(())
    }

    pub(crate) fn run(&self) -> thread::JoinHandle<()> {
        info!("starting {:?} reporter for {}", self.interval, self.ident);
        let cl = self.clone();
        thread::spawn(move || cl.exec().unwrap())
    }

    fn exec(&self) -> Result<()> {
        loop {
            thread::sleep(self.interval);

            let stats = self.drain_stats()?;
            stats.print_report(&self.ident, &self.interval);
        }
    }

    fn drain_stats(&self) -> Result<Stats> {
        let mut stats = self
            .stats
            .lock()
            .map_err(|_| anyhow!("failed to lock level_floor mutex"))?;

        let c: HashMap<String, usize> = stats.counters.drain().collect();
        let v: HashMap<String, String> = stats.values.drain().collect();
        Ok(Stats {
            counters: c,
            values: v,
        })
    }
}
