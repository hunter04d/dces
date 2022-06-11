#![allow(dead_code)]

use std::ffi::OsString;
use std::fmt::{Display, Formatter};
use std::{
    iter::Cloned,
    net::{SocketAddr, ToSocketAddrs},
    num::NonZeroU32,
    path::PathBuf,
    str::FromStr,
};

use crate::client::ClientStartOptions;
use crate::server::ServerStartOptions;
use clap::{command, Arg, ArgMatches, Command, ErrorKind};

#[derive(Debug)]
pub struct CliStart {
    pub addr: HostAddr,
    pub entry_point: PathBuf,
    pub start_threshold: Option<NonZeroU32>,
    pub node_name: String,
}

#[derive(Debug)]
pub struct CliConnect {
    pub server_addr: HostAddr,
    pub entry_point: PathBuf,
    pub node_name: String,
    pub client_rebuilding_addr: HostAddr,
}

#[derive(Debug)]
pub enum CliCommand {
    Start(CliStart),
    Connect(CliConnect),
}

#[derive(Debug)]
pub struct Cli {
    pub command: CliCommand,
}

#[derive(Debug, Clone)]
pub struct HostAddr {
    string_value: String,
    addrs: Vec<SocketAddr>,
}

impl FromStr for HostAddr {
    type Err = std::io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let addrs: Vec<_> = s.to_socket_addrs()?.collect();
        Ok(Self {
            string_value: s.to_string(),
            addrs,
        })
    }
}

impl Display for HostAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.string_value)
    }
}

impl<'a> ToSocketAddrs for &'a HostAddr {
    type Iter = Cloned<std::slice::Iter<'a, SocketAddr>>;

    fn to_socket_addrs(&self) -> std::io::Result<Self::Iter> {
        Ok(self.addrs.iter().cloned())
    }
}

impl Cli {
    pub fn try_parse() -> Result<Self, clap::Error> {
        let command = Self::command();
        let matches = command.try_get_matches()?;
        Self::from_arg_matches(&matches)
    }

    pub fn try_parse_from<I, T>(itr: I) -> Result<Self, clap::Error>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let command = Self::command();
        let matches = command.try_get_matches_from(itr)?;
        Self::from_arg_matches(&matches)
    }

    pub fn parse() -> Self {
        Self::try_parse().unwrap_or_else(|e| e.exit())
    }

    pub fn command() -> Command<'static> {
        let entry_point_arg = Arg::new("entry_point")
            .takes_value(true)
            .required(true)
            .value_name("ENTRY_POINT")
            .help("wasm (or wat) file, the entrypoint of the application, must have exported main() -> () function ");

        let node_name_arg = Arg::new("node_name")
            .short('n')
            .long("--name")
            .takes_value(true)
            .required(true)
            .value_name("NODE_NAME")
            .help("Name of the node in the cluster");

        let start = Command::new("start")
            .visible_alias("s")
            .arg(entry_point_arg.clone())
            .arg(node_name_arg.clone())
            .arg(
                Arg::new("address")
                    .short('a')
                    .long("address")
                    .takes_value(true)
                    .value_name("ADDRESS")
                    .default_value("0.0.0.0:5001")
                    .help("Specifies host address in format <HOSTNAME>:<PORT>")
                    .value_delimiter(';'),
            )
            .arg(
                Arg::new("start_threshold")
                    .short('t')
                    .long("threshold")
                    .takes_value(true)
                    .required(false)
                    .value_name("THRESHOLD")
                    .help("Threshold of connected cluster nodes before the execution starts (default is start immediately)")
            )
            .about("Starts the dces main cluster node");

        let connect = Command::new("connect")
            .visible_alias("c")
            .arg(entry_point_arg)
            .arg(node_name_arg)
            .arg(
                Arg::new("address")
                    .short('a')
                    .long("address")
                    .required(true)
                    .takes_value(true)
                    .help("Cluster address"),
            )
            .arg(
                Arg::new("rebuild")
                    .short('r')
                    .long("rebuild-at")
                    .required(true)
                    .takes_value(true)
                    .help("Server address of this client during after rebuild with this node as primary"),
            )
            .about("Connects to the ces cluster hosted at specified address");

        command!()
            .about("A distributed code executing system experiment")
            .subcommand(start)
            .subcommand(connect)
            .disable_help_subcommand(true)
            .subcommand_required(true)
    }

    fn from_arg_matches(matches: &ArgMatches) -> Result<Self, clap::Error> {
        match matches.subcommand() {
            Some(("start", sub_m)) => {
                let addr = sub_m.value_of_t::<HostAddr>("address")?;
                let entry_point = sub_m.value_of_t::<PathBuf>("entry_point")?;
                let start_threshold = match sub_m.value_of_t::<NonZeroU32>("start_threshold") {
                    Ok(v) => Ok(Some(v)),
                    Err(e) if e.kind() == ErrorKind::ArgumentNotFound => Ok(None),
                    Err(e) => Err(e),
                }?;
                let node_name = sub_m.value_of_t::<String>("node_name")?;
                let start = CliStart {
                    addr,
                    start_threshold,
                    entry_point,
                    node_name,
                };
                Ok(Cli {
                    command: CliCommand::Start(start),
                })
            }
            Some(("connect", sub_m)) => {
                let server_addr = sub_m.value_of_t::<HostAddr>("address")?;
                let entry_point = sub_m.value_of_t::<PathBuf>("entry_point")?;
                let node_name = sub_m.value_of_t::<String>("node_name")?;
                let client_rebuilding_addr = sub_m.value_of_t::<HostAddr>("rebuild")?;
                let connect = CliConnect {
                    server_addr,
                    entry_point,
                    node_name,
                    client_rebuilding_addr,
                };

                Ok(Cli {
                    command: CliCommand::Connect(connect),
                })
            }
            Some((_, _)) => Err(clap::Error::raw(
                ErrorKind::UnrecognizedSubcommand,
                "Valid subcommands are `connect` and `start`",
            )),
            None => Err(clap::Error::raw(
                ErrorKind::MissingSubcommand,
                "Valid subcommands are `connect` and `start`",
            )),
        }
    }
}

impl ServerStartOptions for CliStart {
    fn addr(&self) -> HostAddr {
        self.addr.clone()
    }

    fn node_name(&self) -> String {
        self.node_name.clone()
    }
}

impl ClientStartOptions for CliConnect {
    fn server_addr(&self) -> HostAddr {
        self.server_addr.clone()
    }

    fn node_name(&self) -> String {
        self.node_name.clone()
    }

    fn client_rebuilding_addr(&self) -> HostAddr {
        self.client_rebuilding_addr.clone()
    }
}
