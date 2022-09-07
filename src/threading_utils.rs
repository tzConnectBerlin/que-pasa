use std::sync::{Arc, Condvar, Mutex};

#[derive(Clone)]
pub(crate) struct AtomicCondvar {
    flag: Arc<(Mutex<bool>, Condvar)>,
}

impl AtomicCondvar {
    pub fn new() -> Self {
        Self {
            flag: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }

    pub fn wait(&self) {
        debug!("AtomicCondvar::wait (enter)");
        let (mutex, cvar) = &*self.flag;
        let mut started = mutex.lock().unwrap();
        while !*started {
            started = cvar.wait(started).unwrap();
        }
        debug!("AtomicCondvar::wait (exit)");
    }

    pub fn notify_all(&self) {
        debug!("AtomicCondvar::notify_all");
        let (mutex, cvar) = &*self.flag;
        let mut started = mutex.lock().unwrap();
        *started = true;
        cvar.notify_all();
    }
}
