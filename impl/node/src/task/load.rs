use crate::task::{Task, TaskHandle};

use std::error::Error;
use std::sync::{Arc, RwLock};

struct LoadTask {
    channels: Vec<String>,
    directory: String,
    satellite: String,
    thread_count: u8,
}

impl Task for LoadTask {
    fn start(&self) -> Result<Arc<RwLock<TaskHandle>>, Box<dyn Error>> {
        unimplemented!();
    }
}
