use clap::ArgMatches;
use protobuf::{TaskBroadcastRequest, TaskBroadcastType, TaskClearRequest, TaskManagementClient, TaskListRequest};
use tonic::Request;

use std::{error, io};
use std::collections::HashMap;

pub fn process(matches: &ArgMatches, task_matches: &ArgMatches) {
    let result: Result<(), Box<dyn error::Error>> 
            = match task_matches.subcommand() {
        ("clear", Some(clear_matches)) =>
            clear(&matches, &task_matches, &clear_matches),
        ("list", Some(list_matches)) =>
            list(&matches, &task_matches, &list_matches),
        (cmd, _) => Err(Box::new(io::Error::new(io::ErrorKind::Other,
            format!("unknown subcommand '{}'", cmd)))),
    };

    if let Err(e) = result {
        println!("{}", e);
    }
}

#[tokio::main]
async fn clear(matches: &ArgMatches, _: &ArgMatches,
        _clear_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // initialize grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = TaskManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize request
    let request = Request::new(TaskBroadcastRequest {
        message_type: TaskBroadcastType::TaskClear as i32,
        clear_request: Some(TaskClearRequest {}),
        list_request: None,
    });

    // retrieve reply
    let _ = client.broadcast(request).await?;

    Ok(())
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
        clear_request: None,
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
                (0u16, 0u16, 0u16, 0u16, 0u32, 0u32, 0u32));

            // compile task status
            match (task.running, task.completed_count, task.total_count) {
                (true, _, 0) => task_tuple.0 += 1,
                (true, _, _) => task_tuple.1 += 1,
                (false, x, y) if x < y => task_tuple.2 += 1,
                (false, _, _) => task_tuple.3 += 1,
            };

            task_tuple.4 += task.completed_count;
            task_tuple.5 += task.skipped_count;
            task_tuple.6 += task.total_count;
        }
    }

    // print information
    println!("{:<24}{:<16}{:<12}{:<12}{:<12}{:<24}", "task_id",
        "initializing", "running", "failed", "completed", "progress");
    println!("----------------------------------------------------------------------------------------------------");
    for (task_id, task_tuple) in tasks.iter() {
        println!("{:<24}{:<16}{:<12}{:<12}{:<12}{:<24}", task_id,
            task_tuple.0, task_tuple.1, task_tuple.2, task_tuple.3,
            compute_progress(task_tuple.4, task_tuple.5, task_tuple.6));
    }

    Ok(())
}

fn compute_progress(completed_count: u32,
        skipped_count: u32, total_count: u32) -> f32 {
    match total_count {
        0 => 1f32,
        _ => (completed_count + skipped_count) as f32 / total_count as f32,
    }
}
