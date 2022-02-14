use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct StatsLogger {
    interval: Duration,

    stats: Arc<Mutex<HashMap<String, Stats>>>,

    is_cancelled: Arc<AtomicBool>,
}

impl StatsLogger {
    pub(crate) fn new(interval: Duration) -> Self {
        Self {
            interval,

            stats: Arc::new(Mutex::new(HashMap::new())),

            is_cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn add(
        &self,
        report: &str,
        field: &str,
        n: usize,
    ) -> Result<()> {
        self.touch_report(report)?;
        let mut stats = self
            .stats
            .lock()
            .map_err(|_| anyhow!("failed to lock level_floor mutex"))?;

        stats
            .get_mut(report)
            .unwrap()
            .add(field, n);

        Ok(())
    }

    fn touch_report(&self, report: &str) -> Result<()> {
        let mut stats = self
            .stats
            .lock()
            .map_err(|_| anyhow!("failed to lock level_floor mutex"))?;

        if !stats.contains_key(report) {
            stats.insert(report.to_string(), Stats::new());
        }
        Ok(())
    }

    pub(crate) fn set(
        &self,
        report: &str,
        field: &str,
        value: String,
    ) -> Result<()> {
        self.touch_report(report)?;
        let mut stats = self
            .stats
            .lock()
            .map_err(|_| anyhow!("failed to lock level_floor mutex"))?;
        stats
            .get_mut(report)
            .unwrap()
            .set(field, value);
        Ok(())
    }

    pub(crate) fn unset(&self, report: &str, field: &str) -> Result<()> {
        let mut stats = self
            .stats
            .lock()
            .map_err(|_| anyhow!("failed to lock level_floor mutex"))?;
        if !stats.contains_key(report) {
            return Ok(());
        }
        stats
            .get_mut(report)
            .unwrap()
            .unset(field);

        if stats
            .get_mut(report)
            .unwrap()
            .is_empty()
        {
            stats.remove(report);
        }

        Ok(())
    }

    pub(crate) fn run(&self) -> thread::JoinHandle<()> {
        let cl = self.clone();
        thread::spawn(move || cl.exec().unwrap())
    }

    pub(crate) fn stop(&self) {
        self.is_cancelled
            .swap(true, Ordering::Relaxed);
    }

    pub(crate) fn reset(&mut self) -> Result<()> {
        let mut stats = self
            .stats
            .lock()
            .map_err(|_| anyhow!("failed to lock level_floor mutex"))?;

        stats.clear();
        self.is_cancelled
            .swap(false, Ordering::Relaxed);

        Ok(())
    }

    fn exec(&self) -> Result<()> {
        if self.interval == Duration::new(0, 0) {
            return Ok(());
        }

        info!("reporting statistics every {:?}", self.interval);
        while !self.cancelled() {
            thread::park_timeout(self.interval);

            let stats = self.drain_stats()?;
            Self::print_report(&self.interval, stats);
        }
        Ok(())
    }

    fn drain_stats(&self) -> Result<HashMap<String, Stats>> {
        let mut stats = self
            .stats
            .lock()
            .map_err(|_| anyhow!("failed to lock level_floor mutex"))?;

        let mut res: HashMap<String, Stats> = HashMap::new();
        for (report, stats) in stats.iter_mut() {
            let c: HashMap<String, (usize, u64)> = stats.counters.clone();
            for (_, (c, _)) in stats.counters.iter_mut() {
                *c = 0
            }
            let v: HashMap<String, String> = stats.values.clone();

            res.insert(
                report.clone(),
                Stats {
                    counters: c,
                    values: v,
                },
            );
        }
        Ok(res)
    }

    fn cancelled(&self) -> bool {
        self.is_cancelled
            .load(Ordering::Relaxed)
    }

    fn print_report(at_interval: &Duration, stats: HashMap<String, Stats>) {
        let mut stats_ordered: Vec<(String, Stats)> =
            stats.into_iter().collect();
        stats_ordered.sort_by_key(|(section_name, _)| section_name.clone());

        let sections = stats_ordered
            .iter()
            .map(|(ident, stats)| stats.generate_report(ident))
            .collect::<Vec<String>>()
            .join("\n");

        info!("\n=============\n{:?} report\n{}\n", at_interval, sections);
    }
}

#[derive(Debug)]
struct Stats {
    counters: HashMap<String, (usize, u64)>,
    values: HashMap<String, String>,
}

impl Stats {
    pub(crate) fn new() -> Self {
        Self {
            counters: HashMap::new(),
            values: HashMap::new(),
        }
    }

    pub(crate) fn add(&mut self, field: &str, n: usize) {
        let (c, total) = self
            .counters
            .get(field)
            .unwrap_or(&(0, 0));
        let c = c + n;
        let total = total + (n as u64);
        self.counters
            .insert(field.to_string(), (c, total));
    }

    pub(crate) fn set(&mut self, field: &str, value: String) {
        self.values
            .insert(field.to_string(), value);
    }

    pub(crate) fn unset(&mut self, field: &str) {
        self.values.remove(field);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.counters.is_empty() && self.values.is_empty()
    }

    pub(crate) fn generate_report(&self, ident: &str) -> String {
        let mut counters = self
            .counters
            .clone()
            .into_iter()
            .collect::<Vec<(String, (usize, u64))>>();
        counters.sort_by_key(|(f, _)| f.clone());

        let counters_log: String = counters
            .iter()
            .map(|(field, (c, t))| {
                let field_ = if field.len() <= 20 {
                    field.clone()
                } else {
                    format!("{}..", &field[..18])
                };
                format!("\n\t{:<20}| {:<9}| {}", field_, c, t,)
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

        let header = format!("\t{:<20}  {:<9}  {}", "", "interval", "total");
        format!(
            "\n{}:\n{}{}\n\t--{}",
            ident, header, counters_log, values_log
        )
    }
}
