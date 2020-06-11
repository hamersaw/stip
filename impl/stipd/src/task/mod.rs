use swarm::prelude::Dht;

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::collections::hash_map::Iter;
use std::error::Error;
use std::hash::Hasher;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};

pub mod coalesce;
//pub mod fill;
pub mod split;
pub mod store;
pub mod open;

#[tonic::async_trait]
pub trait Task {
    async fn start(&self) -> Result<Arc<RwLock<TaskHandle>>, Box<dyn Error>>;
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

    pub fn clear(&mut self) -> Result<(), Box<dyn Error>> {
        // retrieve list of 'complete' ids    
        let complete_ids: Vec<u64> = self.tasks.iter()
            .filter(|(_, task_handle)|
                task_handle.read().unwrap().get_status()
                    == &TaskStatus::Complete)
            .map(|(id, _)| id.clone())
            .collect();

        // remove complete ids
        for complete_id in complete_ids.iter() {
            self.tasks.remove(complete_id);
        }

        Ok(())
    }

    pub fn iter(&self) -> Iter<u64, Arc<RwLock<TaskHandle>>> {
        self.tasks.iter()
    }

    pub fn register(&mut self, task_handle: Arc<RwLock<TaskHandle>>,
            task_id: Option<u64>) -> Result<u64, Box<dyn Error>> {
        // initialize task id
        let task_id = match task_id {
            Some(task_id) => task_id,
            None => rand::random::<u64>(),
        };

        // add TaskHandle to map
        self.tasks.insert(task_id, task_handle);

        // return task id
        Ok(task_id)
    }
}

#[derive(PartialEq)]
pub enum TaskStatus {
    Complete,
    Failure(String),
    Running,
}

fn dht_lookup(dht: &Arc<RwLock<Dht>>, dht_key_length: i8,
        geocode: &str) -> Result<SocketAddr, Box<dyn Error>> {
    // compute dht geocode using dht_key_length
    let geocode = match dht_key_length {
        0 => geocode,
        x if x > 0 && x < geocode.len() as i8 =>
            &geocode[x as usize..],
        x if x < 0 && x > (-1 * geocode.len() as i8) =>
            &geocode[..(geocode.len() as i8 + x) as usize],
        _ => return Err(format!("dht key length '{}' invalid for '{}'",
                dht_key_length, geocode).into()),
    };

    // compute geocode hash
    let mut hasher = DefaultHasher::new();
    hasher.write(geocode.as_bytes());
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
