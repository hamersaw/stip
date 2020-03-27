use std::collections::HashMap;
use std::error::Error;
use std::sync::{Arc, RwLock};
use std::sync::atomic::AtomicU32;

pub mod load;

pub trait Task {
    fn start(&self) -> Result<Arc<RwLock<TaskHandle>>, Box<dyn Error>>;
}

pub struct TaskHandle {
    items_completed: Arc<AtomicU32>,
    items_total: u32,
    status: TaskStatus,
}

impl TaskHandle {
    pub fn new(items_completed: Arc<AtomicU32>,
            items_total: u32, status: TaskStatus) -> TaskHandle {
        TaskHandle {
            items_completed: items_completed,
            items_total: items_total,
            status: status,
        }
    }

    pub fn set_status(&mut self, status: TaskStatus) {
        self.status = status;
    }
}

pub struct TaskManager {
    current_id: u64,
    tasks: HashMap<u64, Arc<RwLock<TaskHandle>>>,
}

impl TaskManager {
    pub fn new() -> TaskManager {
        TaskManager {
            current_id: 999,
            tasks: HashMap::new(),
        }
    }

    pub fn execute(&mut self, t: impl Task)
            -> Result<u64, Box<dyn Error>> {
        // increment current id
        self.current_id += 1;

        // start task and add to map
        let task_handle = t.start()?;
        self.tasks.insert(self.current_id, task_handle);

        // return task id
        Ok(self.current_id)
    }
}

pub enum TaskStatus {
    Completed,
    Failed(String),
    Running,
}
