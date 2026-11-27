//! `truthdb-mount <bucket> <directory>`: mounts the bucket (or a prefix of
//! it) at the directory, read-only, until it is unmounted (`fusermount3 -u
//! <directory>`: this then ends) or this is stopped (Ctrl-C, SIGTERM: it
//! unmounts). It runs in the foreground. The endpoint and keys are found as `truthdb-cli s3` finds
//! them.

use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use truthdb_s3client::{Client, Config};

#[derive(Parser, Debug)]
#[command(
    name = "truthdb-mount",
    about = "Mount an S3 bucket as a file system (read-only)"
)]
struct Args {
    /// The bucket.
    bucket: String,
    /// The directory to mount it at.
    directory: PathBuf,
    /// Only the keys under this prefix, which ends in `/`.
    #[arg(long, default_value = "")]
    prefix: String,
    /// The service's URL.
    #[arg(
        long,
        env = "AWS_ENDPOINT_URL",
        default_value = "http://127.0.0.1:9000"
    )]
    endpoint_url: String,
    /// The region requests are signed for (else `AWS_DEFAULT_REGION`, else
    /// us-east-1).
    #[arg(long, env = "AWS_REGION")]
    region: Option<String>,
    /// The access key id (else from the credentials file).
    #[arg(long, env = "AWS_ACCESS_KEY_ID")]
    access_key: Option<String>,
    /// The secret access key (else from the credentials file).
    #[arg(long, env = "AWS_SECRET_ACCESS_KEY", hide_env_values = true)]
    secret_key: Option<String>,
    /// The profile of `~/.aws/credentials` (or `AWS_SHARED_CREDENTIALS_FILE`)
    /// to read keys from.
    #[arg(long, env = "AWS_PROFILE", default_value = "default")]
    profile: String,
    /// PEM certificates to trust for HTTPS instead of the system's.
    #[arg(long)]
    ca_file: Option<PathBuf>,
    /// The files' and directories' owner (else the mounting user).
    #[arg(long)]
    uid: Option<u32>,
    /// Their group (else the mounting user's).
    #[arg(long)]
    gid: Option<u32>,
    /// The files' mode, in octal.
    #[arg(long, default_value = "0644")]
    file_mode: String,
    /// The directories' mode, in octal.
    #[arg(long, default_value = "0755")]
    dir_mode: String,
    /// Let other users than the mounting one use the mount.
    #[arg(long)]
    allow_other: bool,
    /// Mount read-only (the mount always is, for now).
    #[arg(long)]
    read_only: bool,
}

fn mode(text: &str) -> Result<u16> {
    let mode = u16::from_str_radix(text.trim_start_matches("0o"), 8)
        .with_context(|| format!("{text} is no octal mode"))?;
    if mode > 0o7777 {
        bail!("{text} is no file mode");
    }
    Ok(mode)
}

fn main() -> Result<()> {
    let args = Args::parse();
    if !args.prefix.is_empty() && !args.prefix.ends_with('/') {
        bail!("the prefix {:?} does not end in /", args.prefix);
    }
    let (access_key, secret) = match (&args.access_key, &args.secret_key) {
        (Some(id), Some(secret)) => (id.clone(), secret.clone()),
        (Some(_), None) | (None, Some(_)) => {
            bail!("give both an access key and a secret key, or neither")
        }
        (None, None) => {
            truthdb_s3client::credentials_file_keys(&args.profile).ok_or_else(|| {
                anyhow!(
                    "no keys: give --access-key and --secret-key, set AWS_ACCESS_KEY_ID and \
                 AWS_SECRET_ACCESS_KEY, or add profile [{}] to ~/.aws/credentials",
                    args.profile
                )
            })?
        }
    };
    let region = args
        .region
        .clone()
        .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
        .unwrap_or_else(|| "us-east-1".to_string());
    let ca_pem = args
        .ca_file
        .as_ref()
        .map(|path| std::fs::read(path).with_context(|| format!("reading {}", path.display())))
        .transpose()?;
    let client = Client::new(Config {
        endpoint: args.endpoint_url.clone(),
        region,
        access_key,
        secret,
        ca_pem,
    })?;
    // SAFETY: getuid and getgid cannot fail.
    let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
    let options = truthdb_mount::Options {
        bucket: args.bucket.clone(),
        prefix: args.prefix.clone(),
        uid: args.uid.unwrap_or(uid),
        gid: args.gid.unwrap_or(gid),
        file_mode: mode(&args.file_mode)?,
        dir_mode: mode(&args.dir_mode)?,
        allow_other: args.allow_other,
    };
    // The signals are taken before the mount is made: one arriving at once
    // still unmounts it.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (mut term, mut interrupt) = {
        let _entered = runtime.enter();
        use tokio::signal::unix::{SignalKind, signal};
        (
            signal(SignalKind::terminate())?,
            signal(SignalKind::interrupt())?,
        )
    };
    let mut session = truthdb_mount::session(client, options, &args.directory)
        .with_context(|| format!("mounting {} at {}", args.bucket, args.directory.display()))?;
    let mut unmounter = session.unmount_callable();
    // Served until unmounted: from outside (`fusermount3 -u`), or here when
    // stopped.
    let (ended, end) = tokio::sync::oneshot::channel();
    let served = std::thread::spawn(move || {
        let result = session.run();
        let _ = ended.send(());
        result
    });
    let stopped = runtime.block_on(async {
        tokio::select! {
            _ = interrupt.recv() => true,
            _ = term.recv() => true,
            _ = end => false,
        }
    });
    if stopped {
        unmounter.unmount().context("unmounting")?;
    }
    served
        .join()
        .map_err(|_| anyhow!("the file system's thread panicked"))?
        .context("serving the file system")?;
    Ok(())
}
