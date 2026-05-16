//! qx — QX command-line client.
//!
//! Connects to a running `qxd` and performs queue / consumer-group / workflow
//! / schema-registry operations. Optional bearer-token auth and TLS.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use qx_broker::Client;

#[derive(Parser)]
#[command(name = "qx", about = "QX command-line client")]
struct Args {
    /// QX broker address (use `https://...` when TLS is on).
    #[arg(long, default_value = "http://127.0.0.1:50051", global = true)]
    addr: String,

    /// Optional bearer token (matches the qxd `auth.token` config).
    #[arg(long, global = true)]
    token: Option<String>,

    /// PEM CA cert to trust for TLS. Must be set together with --tls-domain.
    #[arg(long, global = true)]
    ca_cert: Option<PathBuf>,

    /// Domain name to verify against the server certificate.
    #[arg(long, global = true)]
    tls_domain: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Send a record to a topic. Auto-opens the topic with --partitions if it doesn't exist.
    Send {
        #[arg(long)]
        topic: String,
        #[arg(long)]
        key: String,
        #[arg(long)]
        payload: String,
        #[arg(long, default_value = "4")]
        partitions: u32,
    },
    /// Pull a batch from one partition of a consumer group. With --auto-commit,
    /// the committed offset is advanced past the last record received.
    Stream {
        #[arg(long)]
        topic: String,
        #[arg(long)]
        group: String,
        #[arg(long)]
        partition: u32,
        #[arg(long, default_value = "10")]
        max: u32,
        #[arg(long)]
        auto_commit: bool,
    },
    /// Commit a consumer group's offset for a partition.
    CommitOffset {
        #[arg(long)]
        topic: String,
        #[arg(long)]
        group: String,
        #[arg(long)]
        partition: u32,
        #[arg(long)]
        offset: u64,
    },
    /// Join a consumer group and print the assigned partitions.
    JoinGroup {
        #[arg(long)]
        topic: String,
        #[arg(long)]
        group: String,
    },
    /// Create a root workflow task.
    WorkflowCreate {
        #[arg(long)]
        workflow: String,
        #[arg(long)]
        payload: String,
    },
    /// Show stats for a topic.
    Stats {
        #[arg(long)]
        topic: String,
    },
    /// Schema registry operations.
    Schema {
        #[command(subcommand)]
        action: SchemaAction,
    },
}

#[derive(Subcommand)]
enum SchemaAction {
    /// Register a new schema version for a subject. Supply EITHER --schema or --schema-file.
    Register {
        #[arg(long)]
        subject: String,
        #[arg(long)]
        schema: Option<String>,
        #[arg(long)]
        schema_file: Option<PathBuf>,
    },
    /// Look up a schema by its globally-unique id.
    GetId {
        #[arg(long)]
        id: u64,
    },
    /// Look up a specific version of a subject's schema.
    GetVersion {
        #[arg(long)]
        subject: String,
        #[arg(long)]
        version: u32,
    },
    /// Look up the latest version of a subject's schema.
    GetLatest {
        #[arg(long)]
        subject: String,
    },
    /// List all versions for a subject.
    ListVersions {
        #[arg(long)]
        subject: String,
    },
    /// List all known subjects.
    ListSubjects,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let mut client = connect(&args).await?;

    match args.command {
        Command::Send {
            topic,
            key,
            payload,
            partitions,
        } => {
            client.open_topic(&topic, partitions).await?;
            let (partition, record_id) = client
                .send(&topic, key.as_bytes(), payload.as_bytes())
                .await?;
            println!("partition={partition} record_id={record_id}");
        }
        Command::Stream {
            topic,
            group,
            partition,
            max,
            auto_commit,
        } => {
            let batch = client
                .stream_from_group(&topic, &group, partition, max)
                .await?;
            for rec in &batch {
                println!(
                    "[p{}/{}] {}",
                    rec.partition,
                    rec.id,
                    String::from_utf8_lossy(&rec.payload),
                );
            }
            eprintln!("({} records)", batch.len());
            if auto_commit {
                if let Some(last) = batch.last() {
                    client
                        .commit_group_offset(&topic, &group, partition, last.id + 1)
                        .await?;
                    eprintln!("committed offset {} for {}@{}", last.id + 1, group, partition);
                }
            }
        }
        Command::CommitOffset {
            topic,
            group,
            partition,
            offset,
        } => {
            client
                .commit_group_offset(&topic, &group, partition, offset)
                .await?;
            println!("committed offset {offset} for {group}@p{partition}");
        }
        Command::JoinGroup { topic, group } => {
            client.open_topic(&topic, 4).await?;
            let (member_id, generation, assignment) =
                client.join_group(&topic, &group).await?;
            println!(
                "member_id={member_id} generation={generation} partitions={assignment:?}"
            );
        }
        Command::WorkflowCreate { workflow, payload } => {
            let task_id = client
                .create_root_task(&workflow, payload.into_bytes())
                .await?;
            println!("task_id={task_id}");
        }
        Command::Stats { topic } => {
            let s = client.stats(&topic).await?;
            println!(
                "partitions={} pending={} inflight={} dead={}",
                s.partition_count, s.pending, s.inflight, s.dead
            );
        }
        Command::Schema { action } => match action {
            SchemaAction::Register {
                subject,
                schema,
                schema_file,
            } => {
                let bytes = match (schema, schema_file) {
                    (Some(s), None) => s.into_bytes(),
                    (None, Some(p)) => std::fs::read(p)?,
                    (Some(_), Some(_)) => {
                        return Err(
                            "specify either --schema or --schema-file, not both".into()
                        );
                    }
                    (None, None) => {
                        return Err("must specify --schema or --schema-file".into());
                    }
                };
                let s = client.register_schema(&subject, bytes).await?;
                println!("id={} subject={} version={}", s.id, s.subject, s.version);
            }
            SchemaAction::GetId { id } => {
                let s = client.get_schema_by_id(id).await?;
                print_schema(&s);
            }
            SchemaAction::GetVersion { subject, version } => {
                let s = client.get_schema_version(&subject, version).await?;
                print_schema(&s);
            }
            SchemaAction::GetLatest { subject } => {
                let s = client.get_latest_schema(&subject).await?;
                print_schema(&s);
            }
            SchemaAction::ListVersions { subject } => {
                let versions = client.list_schema_versions(&subject).await?;
                for v in versions {
                    println!("{v}");
                }
            }
            SchemaAction::ListSubjects => {
                let subjects = client.list_subjects().await?;
                for s in subjects {
                    println!("{s}");
                }
            }
        },
    }
    Ok(())
}

async fn connect(args: &Args) -> Result<Client, Box<dyn std::error::Error>> {
    match (
        args.ca_cert.as_deref(),
        args.tls_domain.as_deref(),
        args.token.as_deref(),
    ) {
        (Some(ca_path), Some(domain), Some(token)) => {
            let ca = std::fs::read(ca_path)?;
            Ok(Client::connect_tls_with_token(
                args.addr.clone(),
                ca,
                domain.to_string(),
                token.to_string(),
            )
            .await?)
        }
        (Some(ca_path), Some(domain), None) => {
            let ca = std::fs::read(ca_path)?;
            Ok(Client::connect_tls(args.addr.clone(), ca, domain.to_string()).await?)
        }
        (Some(_), None, _) | (None, Some(_), _) => {
            Err("--ca-cert and --tls-domain must be used together".into())
        }
        (None, None, Some(token)) => {
            Ok(Client::connect_with_token(args.addr.clone(), token.to_string()).await?)
        }
        (None, None, None) => Ok(Client::connect(args.addr.clone()).await?),
    }
}

fn print_schema(s: &qx_broker::proto::SchemaMessage) {
    println!("id={}", s.id);
    println!("subject={}", s.subject);
    println!("version={}", s.version);
    println!("bytes={}", String::from_utf8_lossy(&s.bytes));
}
