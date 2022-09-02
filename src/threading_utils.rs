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
        info!("waiting for condvar..");
        let (mutex, cvar) = &*self.flag;
        let mut started = mutex.lock().unwrap();
        while !*started {
            started = cvar.wait(started).unwrap();
        }
        info!("waiting for condva done.");
    }

    pub fn notify_all(&self) {
        info!("notify.");
        let (mutex, cvar) = &*self.flag;
        let mut started = mutex.lock().unwrap();
        *started = true;
        cvar.notify_all();
    }
}
