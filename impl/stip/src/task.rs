use clap::ArgMatches;
use protobuf::{TaskBroadcastRequest, TaskBroadcastType, TaskManagementClient, TaskListRequest, TaskShowRequest, TaskStatus};
use tonic::Request;

use std::{error, io};
use std::collections::HashMap;

pub fn process(matches: &ArgMatches, task_matches: &ArgMatches) {
    let result: Result<(), Box<dyn error::Error>> 
            = match task_matches.subcommand() {
        ("list", Some(list_matches)) => {
            list(&matches, &task_matches, &list_matches)
        },
        ("show", Some(show_matches)) => {
            show(&matches, &task_matches, &show_matches)
        },
        (cmd, _) => Err(Box::new(io::Error::new(io::ErrorKind::Other,
            format!("unknown subcommand '{}'", cmd)))),
    };

    if let Err(e) = result {
        println!("{}", e);
    }
}

#[tokio::main]
async fn list(matches: &ArgMatches, _: &ArgMatches,
        _list_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // initialize grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = TaskManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize request
    let request = Request::new(TaskBroadcastRequest {
        message_type: TaskBroadcastType::TaskList as i32,
        list_request: Some(TaskListRequest {}),
    });

    // retrieve reply
    let reply = client.broadcast(request).await?;
    let reply = reply.get_ref();

    // compute an agglomerated view of data
    let mut tasks = HashMap::new();
    for (_node_id, task_list_reply) in reply.list_replies.iter() {
        for task in task_list_reply.tasks.iter() {
            let mut task_tuple = tasks.entry(task.id).or_insert(
                (0u16, 0u16, 0u16, 0u32, 0u32, 0u32));

            match TaskStatus::from_i32(task.status).unwrap() {
                TaskStatus::Complete => task_tuple.0 += 1,
                TaskStatus::Failure => task_tuple.1 += 1,
                TaskStatus::Running => task_tuple.2 += 1,
            }

            task_tuple.3 += task.items_completed;
            task_tuple.4 += task.items_skipped;
            task_tuple.5 += task.items_total;
        }
    }

    // print information
    println!("{:<24}{:<12}{:<12}{:<12}{:<24}", "task_id",
        "completed", "failure", "running", "progress");
    println!("------------------------------------------------------------------------------------");
    for (task_id, task_tuple) in tasks.iter() {
        println!("{:<24}{:<12}{:<12}{:<12}{:<24}", task_id,
            task_tuple.0, task_tuple.1, task_tuple.2,
            compute_progress(task_tuple.3, task_tuple.4, task_tuple.5));
    }

    Ok(())
}

#[tokio::main]
async fn show(matches: &ArgMatches, _: &ArgMatches,
        show_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // initialize grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = TaskManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize request
    let task_id = show_matches.value_of("ID").unwrap().parse::<u64>()?;
    let request = Request::new(TaskShowRequest {
        id: task_id,
    });

    // retrieve reply
    let reply = client.show(request).await?;
    let reply = reply.get_ref();

    // print information
    match &reply.task {
        Some(task) => {
            println!("task_id: {}", task.id);
            println!("items_completed: {}", task.items_completed);
            println!("items_skipped: {}", task.items_skipped);
            println!("items_total: {}", task.items_total);
            println!("status: {}", convert_status(task.status));
        },
        None => println!("task with id '{}' does not exist", task_id),
    }

    Ok(())
}

fn convert_status(status: i32) -> String {
    match TaskStatus::from_i32(status).unwrap() {
        TaskStatus::Complete => "completed".to_string(),
        TaskStatus::Failure => "failure".to_string(),
        TaskStatus::Running => "running".to_string(),
    }
}

fn compute_progress(items_completed: u32,
        items_skipped: u32, items_total: u32) -> f32 {
    let done_count = items_completed + items_skipped;
    match done_count {
        0 => 1f32,
        _ => done_count as f32 / items_total as f32,
    }
}
