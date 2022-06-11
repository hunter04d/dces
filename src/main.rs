extern crate core;

use std::sync::Arc;
use crate::activity::{Activity, Communicator};
use std::thread;
use std::time::Duration;
use wasmtime::{Caller, Engine, Linker, Module, Store};

use crate::cli::CliCommand;
use crate::client::Client;
use crate::msg::types::LogValue;
use crate::replication_log::ReplicationLog;
use crate::server::Server;

mod activity;
mod cli;
mod client;
mod msg;
mod proto;
mod replication_log;
mod server;

fn main() -> anyhow::Result<()> {
    let config = cli::Cli::parse();

    let com = match config.command {
        CliCommand::Start(config) => {
            println!("Start with\n{config:#?}");
            let server = Server::start(&config, ReplicationLog::new());
            println!("Server started");
            Communicator::Server(server)
        }
        CliCommand::Connect(config) => {
            println!("Connect with\n{config:#?}");
            let client = Client::start(&config, None);
            println!("Client started");
            Communicator::Client(client)
        }
    };

    let engine = Engine::default();
    let module = Module::from_file(&engine, "dces_wasm.wasm")?;
    // Create a `Linker` and define our host functions
    let mut linker = Linker::new(&engine);

    let tls_connector = Arc::new(native_tls::TlsConnector::new()?);
    let agent = ureq::builder()
        .tls_connector(Arc::clone(&tls_connector))
        .build();
    linker.func_wrap(
        "rt",
        "report_num",
        move |caller: Caller<'_, Activity>, n: u64| {
            Activity::execute_or_sync(caller, || {
                println!("Calculated value is: {}", n);
                let _resp = agent
                    .post("https://localhost:7233/num")
                    .send_json(ureq::json!({
                      "Value": n,
                    }))
                    .unwrap();
            });
        },
    )?;

    linker.func_wrap("rt", "sleep", |caller: Caller<'_, Activity>, ms: u32| {
        Activity::execute_or_sync(caller, || {
            thread::sleep(Duration::from_millis(ms as u64));
        });
    })?;

    // Use the `linker` to instantiate the module, which will automatically
    // resolve the imports of the module using name-based resolution.
    let mut store = Store::new(&engine, Activity::new(com));
    let instance = linker.instantiate(&mut store, &module)?;

    let main = instance.get_typed_func::<(), (), _>(&mut store, "main")?;
    main.call(&mut store, ())?;

    Ok(())
}
