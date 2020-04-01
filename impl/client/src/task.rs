use clap::ArgMatches;
use protobuf::{DataManagementClient, TaskListRequest, TaskShowRequest, TaskStatus};
use tonic::Request;

use std::{error, io};

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
    // listialize grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = DataManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize request
    let request = Request::new(TaskListRequest {});

    // retrieve reply
    let reply = client.task_list(request).await?;
    let reply = reply.get_ref();

    // print information
    println!("{:<8}{:<24}{:<8}", "id", "completion percent", "status");
    println!("--------------------------------------------");
    for task in reply.tasks.iter() {
        println!("{:<8}{:<24}{:<8}", task.id,
            task.completion_percent, convert_status(task.status));
    }

    Ok(())
}

#[tokio::main]
async fn show(matches: &ArgMatches, _: &ArgMatches,
        show_matches: &ArgMatches) -> Result<(), Box<dyn error::Error>> {
    // listialize grpc client
    let ip_address = matches.value_of("ip_address").unwrap();
    let port = matches.value_of("port").unwrap().parse::<u16>()?;
    let mut client = DataManagementClient::connect(
        format!("http://{}:{}", ip_address, port)).await?;

    // initialize request
    let task_id = show_matches.value_of("ID").unwrap().parse::<u64>()?;
    let request = Request::new(TaskShowRequest {
        id: task_id,
    });

    // retrieve reply
    let reply = client.task_show(request).await?;
    let reply = reply.get_ref();

    // print information
    match &reply.task {
        Some(task) => {
            println!("task_id: {}", task.id);
            println!("completion_percent: {}", task.completion_percent);
            println!("status: {}", convert_status(task.status));
        },
        None => println!("task with id '{}' does not exist", task_id),
    }

    Ok(())
}

fn convert_status(id: i32) -> String {
    match TaskStatus::from_i32(id).unwrap() {
        TaskStatus::Complete => String::from("complete"),
        TaskStatus::Failure => String::from("failure"),
        TaskStatus::Running => String::from("running"),
    }
}
