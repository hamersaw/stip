use crate::task::{Task, TaskHandle, TaskStatus};

use std::error::Error;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU32, Ordering};

pub struct LoadEarthExplorerTask {
    channels: Vec<String>,
    directory: String,
    file: String,
    satellite: String,
    thread_count: u8,
}

impl LoadEarthExplorerTask {
    pub fn new() -> LoadEarthExplorerTask { // TODO - pass arguments
        LoadEarthExplorerTask {
            channels: Vec::new(),
            directory: String::from(""),
            file: String::from(""),
            satellite: String::from(""),
            thread_count: 2,
        }
    }
}

impl Task for LoadEarthExplorerTask {
    fn start(&self) -> Result<Arc<RwLock<TaskHandle>>, Box<dyn Error>> {
        // start worker threads
        let items_completed = Arc::new(AtomicU32::new(0));
        let mut join_handles = Vec::new();
        for _ in 0..self.thread_count {
            let items_completed = items_completed.clone();
            let join_handle = std::thread::spawn(move || {
                // TODO - do something worthwhile
                println!("hello world!");
                items_completed.fetch_add(1, Ordering::SeqCst);
            });

            join_handles.push(join_handle);
        }

        // initialize TaskHandle
        let task_handle = Arc::new( RwLock::new(
            TaskHandle::new(
                items_completed,
                self.thread_count as u32,
                TaskStatus::Running
            )));

        // start management thread
        let task_handle_clone = task_handle.clone();
        let _ = std::thread::spawn(move || {
            // TODO - add items to pipeline
 
            // TODO - add poison pills to pipeline

            // join worker threads
            for join_handle in join_handles {
                if let Err(e) = join_handle.join() {
                    // set TaskHandle status to 'failed'
                    let mut task_handle =
                        task_handle_clone.write().unwrap();
                    task_handle.set_status(
                        TaskStatus::Failure(format!("{:?}", e)));

                    return;
                }
            }

            // set TaskHandle status to 'completed'
            let mut task_handle = task_handle_clone.write().unwrap();
            task_handle.set_status(TaskStatus::Complete);
        });

        // return task handle
        Ok(task_handle)
    }
}
