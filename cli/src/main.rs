extern crate tcc;

extern crate futures;
extern crate tokio_core;
extern crate tokio_service;
extern crate tokio_cassandra;

#[macro_use]
extern crate clap;

#[macro_use]
extern crate error_chain;

extern crate env_logger;

use clap::{SubCommand, Arg};

use tcc::errors::Result;
use tcc::{CertKind, ColorMode, OutputFormat, CliProtoVersion, ConnectionOptions, THEME_NAMES};

quick_main!(run);

#[cfg(not(feature = "colors"))]
fn with_highlight_flags<'a, 'b>(sc: clap::App<'a, 'b>, default_color: &'a str) -> clap::App<'a, 'b> {
    sc
}
#[cfg(feature = "colors")]
fn with_highlight_flags<'a, 'b>(sc: clap::App<'a, 'b>, default_color: &'a str) -> clap::App<'a, 'b> {
    sc.arg(
        Arg::with_name("theme")
            .required(false)
            .takes_value(true)
            .long("theme")
            .possible_values(&THEME_NAMES)
            .default_value(&THEME_NAMES[0])
            .help("The color scheme at which the syntax is highlighted."),
    ).arg(
            Arg::with_name("color")
                .required(false)
                .takes_value(true)
                .long("color")
                .possible_values(&ColorMode::variants())
                .default_value(default_color)
                .help(
                    "Control how color is generated. 'auto' outputs it to a tty only, 'off' \
                        will never use color codes, whereas 'always' will unconditionally emit them.",
                ),
        )
}

pub fn run() -> Result<()> {
    env_logger::init().unwrap();
    let default_cert_type = format!("{}", CertKind::pkcs12);
    let default_output_format = format!("{}", OutputFormat::json);
    let default_color = format!("{}", ColorMode::auto);

    let mut app: clap::App = app_from_crate!();
    let query_sc = SubCommand::with_name("query")
        .arg(
            Arg::with_name("keyspace")
                .required(false)
                .takes_value(true)
                .long("keyspace")
                .short("k")
                .help(
                    "Uses the given keyspace before invoking any query provided later. Similar to prepending \
                       your query with 'use <keyspace>'.",
                ),
        )
        .arg(
            Arg::with_name("file")
                .required(false)
                .takes_value(true)
                .long("file")
                .short("f")
                .help(
                    "Execute the CQL statements in the given file. If the path is '-', the statements will be \
                       read from standard input. Will be executed before the --execute statement.",
                ),
        )
        .arg(
            Arg::with_name("execute")
                .required(false)
                .takes_value(true)
                .long("execute")
                .short("e")
                .help(
                    "Execute the given CQL statement. If a file is read to, the execute statement is always last.",
                ),
        )
        .arg(
            Arg::with_name("interactive")
                .required(false)
                .takes_value(false)
                .long("interactive")
                .short("i")
                .help(
                    "Drop into an interactive shell if possible, right after executing all CQL statements \
                       provided by --execute and/or --file.",
                ),
        )
        .arg(
            Arg::with_name("output-format")
                .required(false)
                .takes_value(true)
                .long("output-format")
                .short("o")
                .possible_values(&OutputFormat::variants())
                .default_value(&default_output_format)
                .help("Defines the serialization format of the query-result."),
        )
        .arg(
            Arg::with_name("dry-run")
                .required(false)
                .long("dry-run")
                .short("n")
                .help(
                    "Don't execute the generated query, but display it on standard output. Output formats are \
                       just ignored if set, as well as --interactive.",
                ),
        );
    app = app.arg(
        Arg::with_name("debug-dump-encoded-frames-into-directory")
            .required(false)
            .long("debug-dump-encoded-frames-into-directory")
            .takes_value(true)
            .help(
                "A directory into which to dump all frames in order they are sent, \
                   differentiating them by their op-code.",
            ),
    ).arg(
            Arg::with_name("debug-dump-decoded-frames-into-directory")
                .required(false)
                .long("debug-dump-decoded-frames-into-directory")
                .takes_value(true)
                .help(
                    "A directory into which to dump all frames in order they arrive, \
                   differentiating them by their op-code.",
                ),
        )
        .arg(
            Arg::with_name("protocol-version")
                .required(false)
                .takes_value(true)
                .long("protocol-version")
                .default_value(&CliProtoVersion::variants()[0])
                .possible_values(&CliProtoVersion::variants())
                .help(
                    "The protocol version to use. If not specified, the highest-supported version is used.",
                ),
        )
        .arg(
            Arg::with_name("cql-version")
                .required(false)
                .takes_value(true)
                .long("desired-cql-version")
                .help(
                    "The semantic CQL version that you require the server to support, like '3.2.1'. It defaults to \
                   the highest supported version offered by the server.",
                ),
        )
        .arg(
            Arg::with_name("host")
                .required(true)
                .takes_value(true)
                .long("host")
                .short("h")
                .help("The name or IP address of the host to connect to."),
        )
        .arg(
            Arg::with_name("port")
                .required(false)
                .long("port")
                .default_value("9042")
                .takes_value(true)
                .help("The port to connect to"),
        )
        .arg(
            Arg::with_name("user")
                .required(false)
                .short("u")
                .long("user")
                .takes_value(true)
                .help("The name of the user to login authenticate as"),
        )
        .arg(
            Arg::with_name("password")
                .required(false)
                .short("p")
                .long("password")
                .takes_value(true)
                .help(
                    "The user's password. Please note that the password might persist in your \
                   history file if provided here",
                ),
        )
        .arg(
            Arg::with_name("tls")
                .required(false)
                .takes_value(false)
                .long("tls")
                .help(
                    "Encrypt the connection via TLS. This will never connect via plain-text, \
                   even if the server supports that too.",
                ),
        )
        .arg(
            Arg::with_name("cert-type")
                .required(false)
                .takes_value(true)
                .long("cert-type")
                .possible_values(&CertKind::variants())
                .default_value(&default_cert_type)
                .help(
                    "Encrypt the connection via TLS. This will never connect via plain-text, \
                   even if the server supports that too.",
                ),
        )
        .arg(
            Arg::with_name("ca-file")
                .required(false)
                .takes_value(true)
                .long("ca-file")
                .help(
                    "A PEM file with one or more certificates to use when trusting other entities. Can be used to \
                   trust self-signed server certificates for example.",
                ),
        )
        .arg(
            Arg::with_name("cert")
                .required(false)
                .takes_value(true)
                .long("cert")
                .help(
                    "The path to the certificate file in a format defined by --cert-type. A \
                   password can be provided by separating it with a colon, such as in \
                   /path/to/cert:password.",
                ),
        )
        .subcommand(SubCommand::with_name("test-connection"))
        .subcommand(with_highlight_flags(query_sc, &default_color));
    let args: clap::ArgMatches = app.get_matches();
    let opts = ConnectionOptions::try_from(&args)?;

    match args.subcommand() {
        ("test-connection", Some(args)) => tcc::test_connection(opts, args),
        ("query", Some(args)) => tcc::query(opts, args),
        _ => {
            println!("{}", args.usage());
            ::std::process::exit(2);
        }
    }
}
