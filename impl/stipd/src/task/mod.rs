use swarm::prelude::Dht;

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::collections::hash_map::Iter;
use std::error::Error;
use std::hash::Hasher;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};

pub mod fill;
pub mod load;
pub mod split;

pub trait Task {
    fn start(&self) -> Result<Arc<RwLock<TaskHandle>>, Box<dyn Error>>;
}

pub struct TaskHandle {
    items_completed: Arc<AtomicU32>,
    items_skipped: Arc<AtomicU32>,
    items_total: u32,
    status: TaskStatus,
}

impl TaskHandle {
    pub fn new(items_completed: Arc<AtomicU32>, items_skipped: Arc<AtomicU32>,
            items_total: u32, status: TaskStatus) -> TaskHandle {
        TaskHandle {
            items_completed: items_completed,
            items_skipped: items_skipped,
            items_total: items_total,
            status: status,
        }
    }

    pub fn get_items_completed(&self) -> u32 {
        self.items_completed.load(Ordering::SeqCst)
    }

    pub fn get_items_skipped(&self) -> u32 {
        self.items_skipped.load(Ordering::SeqCst)
    }

    pub fn get_items_total(&self) -> u32 {
        self.items_total
    }

    pub fn get_status(&self) -> &TaskStatus {
        &self.status
    }

    pub fn set_status(&mut self, status: TaskStatus) {
        self.status = status;
    }
}

pub struct TaskManager {
    tasks: HashMap<u64, Arc<RwLock<TaskHandle>>>,
}

impl TaskManager {
    pub fn new() -> TaskManager {
        TaskManager {
            tasks: HashMap::new(),
        }
    }

    pub fn execute(&mut self, t: impl Task, task_id: Option<u64>)
            -> Result<u64, Box<dyn Error>> {
        // initialize task id
        let task_id = match task_id {
            Some(task_id) => task_id,
            None => rand::random::<u64>(),
        };

        // start task and add to map
        let task_handle = t.start()?;
        self.tasks.insert(task_id, task_handle);

        // return task id
        Ok(task_id)
    }

    pub fn get(&self, task_id: &u64) -> Option<&Arc<RwLock<TaskHandle>>> {
        self.tasks.get(task_id)
    }

    pub fn iter(&self) -> Iter<u64, Arc<RwLock<TaskHandle>>> {
        self.tasks.iter()
    }
}

pub enum TaskStatus {
    Complete,
    Failure(String),
    Running,
}

fn dht_lookup(dht: &Arc<RwLock<Dht>>, geohash: &str)
        -> Result<SocketAddr, Box<dyn Error>> {
    // compute geohash hash
    let mut hasher = DefaultHasher::new();
    hasher.write(geohash.as_bytes());
    let hash = hasher.finish();

    // discover hash location
    let dht = dht.read().unwrap(); 
    match dht.locate(hash) {
        Some((node_id, addrs)) => {
            match addrs.1 {
                Some(addr) => Ok(addr.clone()),
                None => Err(format!("dht node {} has no xfer_addr",
                    node_id).into()),
            }
        },
        None => Err(format!("no dht node for hash {}", hash).into()),
    }
}
