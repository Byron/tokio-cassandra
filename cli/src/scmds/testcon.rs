use clap;
use super::super::args::ConnectionOptions;
use super::super::errors::*;

pub fn test_connection(opts: ConnectionOptions, _args: &clap::ArgMatches) -> Result<()> {
    let addr = format!("{}:{}", opts.host, opts.port);
    let (mut core, client) = opts.connect();
    core.run(client)
        .chain_err(|| format!("Failed to connect to {}", addr))
        .map(|_client| {
            println!("Connection to {} successful", addr);
            ()
        })
        .map_err(|e| e.into())
}
