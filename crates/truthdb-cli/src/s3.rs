//! `truthdb-cli s3`: list, copy, sync and remove objects of an S3 service
//! (this server's S3 API, or any other), in the manner of `aws s3`.
//!
//! Paths are local paths or `s3://bucket[/key]`. A sync sends a file when
//! the other side lacks it, has another size, or is older; a downloaded file
//! gets the object's modification time, so the next sync leaves it alone.
//! `--exclude` and `--include` patterns (`*` and `?`) match paths relative
//! to the source, the later ones winning, as in `aws s3`.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, CommandFactory, FromArgMatches, Parser, Subcommand};
use http_body_util::BodyExt;
use tokio::io::AsyncWriteExt;
use truthdb_s3client::{Bytes, Client, Config};

/// Files larger than this are uploaded in parts of this size.
const PART_SIZE: u64 = 8 * 1024 * 1024;

#[derive(Parser, Debug)]
#[command(
    name = "s3",
    about = "Objects of an S3 service, in the manner of `aws s3`"
)]
struct S3Cli {
    #[command(flatten)]
    connection: Connection,
    #[command(subcommand)]
    command: S3Command,
}

/// Where the service is and whose keys sign the requests.
#[derive(Args, Debug)]
struct Connection {
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
}

#[derive(Subcommand, Debug)]
enum S3Command {
    /// List buckets, or the objects and folders under a bucket or prefix.
    Ls {
        target: Option<String>,
        /// Every object below, rather than one level.
        #[arg(long)]
        recursive: bool,
    },
    /// Make a bucket.
    Mb { bucket: String },
    /// Copy a file or object; with --recursive, a folder or prefix.
    Cp {
        source: String,
        destination: String,
        #[arg(long)]
        recursive: bool,
        #[command(flatten)]
        options: Options,
    },
    /// Make a folder or prefix like another: send what is missing, of
    /// another size, or newer.
    Sync {
        source: String,
        destination: String,
        /// Also remove what the source does not have.
        #[arg(long)]
        delete: bool,
        #[command(flatten)]
        options: Options,
    },
    /// Remove an object; with --recursive, everything under a prefix.
    Rm {
        target: String,
        #[arg(long)]
        recursive: bool,
        #[command(flatten)]
        options: Options,
    },
}

#[derive(Args, Debug, Default)]
struct Options {
    /// Show what would be done, without doing it.
    #[arg(long)]
    dryrun: bool,
    /// Leave out paths matching this pattern.
    #[arg(long)]
    exclude: Vec<String>,
    /// Take paths matching this pattern back in.
    #[arg(long)]
    include: Vec<String>,
}

/// A filter rule, in the order given.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Rule {
    Exclude(String),
    Include(String),
}

/// Whether `path` passes `rules`: the last matching rule decides; with
/// none, it is included.
fn passes(rules: &[Rule], path: &str) -> bool {
    rules
        .iter()
        .rev()
        .find_map(|rule| match rule {
            Rule::Exclude(pattern) => glob(pattern, path).then_some(false),
            Rule::Include(pattern) => glob(pattern, path).then_some(true),
        })
        .unwrap_or(true)
}

/// Whether `text` matches `pattern`: `*` any run of characters (`/`
/// included), `?` any one.
fn glob(pattern: &str, text: &str) -> bool {
    let (pattern, text): (Vec<char>, Vec<char>) =
        (pattern.chars().collect(), text.chars().collect());
    let (mut p, mut t) = (0, 0);
    let mut star: Option<(usize, usize)> = None;
    while t < text.len() {
        match pattern.get(p) {
            Some('*') => {
                star = Some((p, t));
                p += 1;
            }
            Some('?') => {
                p += 1;
                t += 1;
            }
            Some(&c) if c == text[t] => {
                p += 1;
                t += 1;
            }
            _ => match star {
                Some((star_p, star_t)) => {
                    p = star_p + 1;
                    t = star_t + 1;
                    star = Some((star_p, star_t + 1));
                }
                None => return false,
            },
        }
    }
    pattern[p..].iter().all(|&c| c == '*')
}

/// A path as given: a local one, or `s3://bucket/key`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Location {
    Local(PathBuf),
    S3 { bucket: String, key: String },
}

fn location(text: &str) -> Location {
    match text.strip_prefix("s3://") {
        Some(rest) => {
            let (bucket, key) = rest.split_once('/').unwrap_or((rest, ""));
            Location::S3 {
                bucket: bucket.to_string(),
                key: key.to_string(),
            }
        }
        None => Location::Local(PathBuf::from(text)),
    }
}

/// A file or object as a sync compares it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Entry {
    size: u64,
    /// Seconds since the epoch.
    modified: u64,
}

/// What a sync does to one path.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Step {
    Send(String),
    Remove(String),
}

/// The steps that make `destination` like `source` (both keyed by relative
/// path): send what is missing, of another size, or newer; with `delete`,
/// remove what the source lacks. Paths `rules` leave out are left alone.
fn plan(
    source: &BTreeMap<String, Entry>,
    destination: &BTreeMap<String, Entry>,
    delete: bool,
    rules: &[Rule],
) -> Vec<Step> {
    let mut steps: Vec<Step> = source
        .iter()
        .filter(|(path, _)| passes(rules, path))
        .filter(|(path, entry)| {
            destination
                .get(*path)
                .is_none_or(|there| there.size != entry.size || entry.modified > there.modified)
        })
        .map(|(path, _)| Step::Send(path.clone()))
        .collect();
    if delete {
        steps.extend(
            destination
                .keys()
                .filter(|path| !source.contains_key(*path) && passes(rules, path))
                .map(|path| Step::Remove(path.clone())),
        );
    }
    steps
}

/// Seconds since the epoch of an ISO 8601 `YYYY-MM-DDTHH:MM:SS[.fff]Z`.
fn iso_seconds(text: &str) -> Option<u64> {
    let number = |range: std::ops::Range<usize>| text.get(range)?.parse::<i64>().ok();
    let (year, month, day) = (number(0..4)?, number(5..7)?, number(8..10)?);
    let (hour, minute, second) = (number(11..13)?, number(14..16)?, number(17..19)?);
    // Howard Hinnant's days-from-civil.
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    u64::try_from(days * 86_400 + hour * 3_600 + minute * 60 + second).ok()
}

/// The keys of the credentials file's `profile`.
fn file_keys(profile: &str) -> Result<Option<(String, String)>> {
    let path = match std::env::var_os("AWS_SHARED_CREDENTIALS_FILE") {
        Some(path) => PathBuf::from(path),
        None => match std::env::var_os("HOME") {
            Some(home) => Path::new(&home).join(".aws/credentials"),
            None => return Ok(None),
        },
    };
    let Ok(text) = std::fs::read_to_string(&path) else {
        return Ok(None);
    };
    Ok(profile_keys(&text, profile))
}

/// `aws_access_key_id` and `aws_secret_access_key` of `[profile]` in an
/// INI credentials file.
fn profile_keys(text: &str, profile: &str) -> Option<(String, String)> {
    let mut section = String::new();
    let (mut id, mut secret) = (None, None);
    for line in text.lines().map(str::trim) {
        if let Some(name) = line
            .strip_prefix('[')
            .and_then(|rest| rest.strip_suffix(']'))
        {
            section = name.trim().to_string();
        } else if section == profile
            && let Some((name, value)) = line.split_once('=')
        {
            match name.trim() {
                "aws_access_key_id" => id = Some(value.trim().to_string()),
                "aws_secret_access_key" => secret = Some(value.trim().to_string()),
                _ => {}
            }
        }
    }
    Some((id?, secret?))
}

fn client(connection: &Connection) -> Result<Client> {
    let (access_key, secret) = match (&connection.access_key, &connection.secret_key) {
        (Some(id), Some(secret)) => (id.clone(), secret.clone()),
        (Some(_), None) | (None, Some(_)) => {
            bail!("give both an access key and a secret key, or neither")
        }
        (None, None) => file_keys(&connection.profile)?.ok_or_else(|| {
            anyhow!(
                "no keys: give --access-key and --secret-key, set AWS_ACCESS_KEY_ID and \
                 AWS_SECRET_ACCESS_KEY, or add profile [{}] to ~/.aws/credentials",
                connection.profile
            )
        })?,
    };
    let region = connection
        .region
        .clone()
        .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
        .unwrap_or_else(|| "us-east-1".to_string());
    let ca_pem = connection
        .ca_file
        .as_ref()
        .map(|path| std::fs::read(path).with_context(|| format!("reading {}", path.display())))
        .transpose()?;
    Ok(Client::new(Config {
        endpoint: connection.endpoint_url.clone(),
        region,
        access_key,
        secret,
        ca_pem,
    })?)
}

/// The filter rules of the S3 subcommand in `matches`, in the order given.
fn rules_of(matches: &clap::ArgMatches) -> Vec<Rule> {
    let Some((_, sub)) = matches.subcommand() else {
        return Vec::new();
    };
    let mut rules: Vec<(usize, Rule)> = Vec::new();
    for (id, make) in [
        ("exclude", Rule::Exclude as fn(String) -> Rule),
        ("include", Rule::Include as fn(String) -> Rule),
    ] {
        // Not every subcommand takes filters.
        if !sub.try_contains_id(id).unwrap_or(false) {
            continue;
        }
        if let (Some(indices), Some(values)) = (sub.indices_of(id), sub.get_many::<String>(id)) {
            rules.extend(
                indices
                    .zip(values)
                    .map(|(index, value)| (index, make(value.clone()))),
            );
        }
    }
    rules.sort_by_key(|(index, _)| *index);
    rules.into_iter().map(|(_, rule)| rule).collect()
}

/// Runs `truthdb-cli s3` with `argv` (its arguments, without `s3`),
/// writing what it does to `out`.
pub async fn run(argv: &[String], out: &mut dyn Write) -> Result<()> {
    let matches = match S3Cli::command()
        .try_get_matches_from(std::iter::once("s3".to_string()).chain(argv.iter().cloned()))
    {
        Ok(matches) => matches,
        Err(err)
            if matches!(
                err.kind(),
                clap::error::ErrorKind::DisplayHelp
                    | clap::error::ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
            ) =>
        {
            write!(out, "{}", err.render())?;
            return Ok(());
        }
        Err(err) => bail!("{}", err.render()),
    };
    let rules = rules_of(&matches);
    let cli = S3Cli::from_arg_matches(&matches).map_err(|err| anyhow!("{err}"))?;
    let client = client(&cli.connection)?;
    match cli.command {
        S3Command::Ls { target, recursive } => ls(&client, target.as_deref(), recursive, out).await,
        S3Command::Mb { bucket } => {
            let bucket = match location(&bucket) {
                Location::S3 { bucket, key } if key.is_empty() => bucket,
                _ => bail!("mb takes s3://bucket"),
            };
            client.create_bucket(&bucket).await?;
            writeln!(out, "make_bucket: {bucket}")?;
            Ok(())
        }
        S3Command::Cp {
            source,
            destination,
            recursive,
            options,
        } => {
            cp(
                &client,
                location(&source),
                location(&destination),
                recursive,
                options.dryrun,
                &rules,
                out,
            )
            .await
        }
        S3Command::Sync {
            source,
            destination,
            delete,
            options,
        } => {
            sync(
                &client,
                location(&source),
                location(&destination),
                delete,
                options.dryrun,
                &rules,
                out,
            )
            .await
        }
        S3Command::Rm {
            target,
            recursive,
            options,
        } => {
            rm(
                &client,
                location(&target),
                recursive,
                options.dryrun,
                &rules,
                out,
            )
            .await
        }
    }
}

/// Every object under `prefix`, keyed by its key relative to the prefix.
async fn remote_tree(
    client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<BTreeMap<String, Entry>> {
    let mut entries = BTreeMap::new();
    let mut next: Option<String> = None;
    loop {
        let page = client
            .list_objects(bucket, prefix, None, next.as_deref())
            .await?;
        for object in page.objects {
            let modified = iso_seconds(&object.last_modified)
                .ok_or_else(|| anyhow!("{} has no readable date", object.key))?;
            let relative = object
                .key
                .strip_prefix(prefix)
                .ok_or_else(|| anyhow!("the service listed {} under {prefix}", object.key))?
                .to_string();
            // A folder marker (the console's "Create folder") is no file.
            if relative.is_empty() || relative.ends_with('/') {
                continue;
            }
            entries.insert(
                relative,
                Entry {
                    size: object.size,
                    modified,
                },
            );
        }
        match page.next {
            Some(token) => next = Some(token),
            None => return Ok(entries),
        }
    }
}

/// Every file under `root`, keyed by its `/`-separated path relative to it.
/// A folder that is not there is empty only when it is a destination
/// (`required` false): a source that is not there, or not readable, is an
/// error, or a sync with --delete would take it for empty and remove
/// everything.
fn local_tree(root: &Path, required: bool) -> Result<BTreeMap<String, Entry>> {
    let mut entries = BTreeMap::new();
    match std::fs::metadata(root) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => bail!("{} is not a folder", root.display()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound && !required => {
            return Ok(entries);
        }
        Err(err) => return Err(err).with_context(|| format!("reading {}", root.display())),
    }
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in
            std::fs::read_dir(&dir).with_context(|| format!("reading {}", dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                dirs.push(path);
            } else if metadata.is_file() {
                let relative = path
                    .strip_prefix(root)?
                    .to_str()
                    .ok_or_else(|| anyhow!("{} is not UTF-8", path.display()))?
                    .replace(std::path::MAIN_SEPARATOR, "/");
                let modified = metadata
                    .modified()?
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |since| since.as_secs());
                entries.insert(
                    relative,
                    Entry {
                        size: metadata.len(),
                        modified,
                    },
                );
            }
        }
    }
    Ok(entries)
}

/// A prefix that names a folder: empty, or ending in `/`.
fn folder(prefix: &str) -> String {
    if prefix.is_empty() || prefix.ends_with('/') {
        prefix.to_string()
    } else {
        format!("{prefix}/")
    }
}

async fn ls(
    client: &Client,
    target: Option<&str>,
    recursive: bool,
    out: &mut dyn Write,
) -> Result<()> {
    let Some(target) = target else {
        for bucket in client.list_buckets().await? {
            writeln!(out, "{bucket}")?;
        }
        return Ok(());
    };
    let Location::S3 {
        bucket,
        key: prefix,
    } = location(target)
    else {
        bail!("ls takes s3://bucket[/prefix]");
    };
    let mut next: Option<String> = None;
    loop {
        let page = client
            .list_objects(
                &bucket,
                &prefix,
                (!recursive).then_some("/"),
                next.as_deref(),
            )
            .await?;
        let shown = |key: &str| {
            if recursive {
                key.to_string()
            } else {
                key[prefix.rfind('/').map_or(0, |at| at + 1)..].to_string()
            }
        };
        for common in &page.prefixes {
            writeln!(out, "{:>30} {}", "PRE", shown(common))?;
        }
        for object in &page.objects {
            writeln!(
                out,
                "{} {:>10} {}",
                object.last_modified,
                object.size,
                shown(&object.key)
            )?;
        }
        match page.next {
            Some(token) => next = Some(token),
            None => return Ok(()),
        }
    }
}

/// Uploads the file at `path` as `key`: in one request, or in parts when
/// it is large.
async fn upload(client: &Client, path: &Path, bucket: &str, key: &str) -> Result<()> {
    let size = std::fs::metadata(path)
        .with_context(|| format!("reading {}", path.display()))?
        .len();
    if size <= PART_SIZE {
        let body = tokio::fs::read(path).await?;
        client
            .put_object(bucket, key, &[], Bytes::from(body))
            .await?;
        return Ok(());
    }
    let upload = client.create_multipart_upload(bucket, key, &[]).await?;
    let sent = async {
        use tokio::io::AsyncReadExt;
        let mut file = tokio::fs::File::open(path).await?;
        let mut parts = Vec::new();
        // At most 10,000 parts, as S3 allows.
        let part_size = PART_SIZE.max(size.div_ceil(10_000));
        for number in 1..=size.div_ceil(part_size) {
            let wanted = part_size.min(size - (number - 1) * part_size);
            let mut part = Vec::with_capacity(wanted as usize);
            (&mut file).take(part_size).read_to_end(&mut part).await?;
            if part.len() as u64 != wanted {
                bail!("{} changed while it was uploaded", path.display());
            }
            let etag = client
                .upload_part(bucket, key, &upload, number as u32, Bytes::from(part))
                .await?;
            parts.push((number as u32, etag));
        }
        client
            .complete_multipart_upload(bucket, key, &upload, &parts)
            .await?;
        anyhow::Ok(())
    }
    .await;
    if sent.is_err() {
        // Nothing half-uploaded is left to expire.
        let _ = client.abort_multipart_upload(bucket, key, &upload).await;
    }
    sent
}

/// Downloads `key` to `path` (through a file beside it, renamed when whole),
/// giving the file the object's modification time.
async fn download(
    client: &Client,
    bucket: &str,
    key: &str,
    path: &Path,
    modified: Option<u64>,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let partial = path.with_file_name(format!(
        ".{}.truthdb-part",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("download")
    ));
    let written = async {
        let mut download = client.get_object(bucket, key).await?;
        let mut file = tokio::fs::File::create(&partial).await?;
        while let Some(data) = download.next_chunk().await? {
            file.write_all(&data).await?;
        }
        file.flush().await?;
        drop(file);
        if let Some(seconds) = modified {
            let file = std::fs::File::options().write(true).open(&partial)?;
            file.set_modified(UNIX_EPOCH + Duration::from_secs(seconds))?;
        }
        tokio::fs::rename(&partial, path).await?;
        anyhow::Ok(())
    }
    .await;
    if written.is_err() {
        // No half-written file is left to be taken for a real one.
        let _ = tokio::fs::remove_file(&partial).await;
    }
    written
}

/// Objects a download leaves out because their keys are no paths below
/// the destination: each is reported, the others are still downloaded, and
/// the run then fails.
#[derive(Default)]
struct Skipped(usize);

impl Skipped {
    /// Where `relative` goes under `to`, or `None` (reported) when it
    /// cannot go anywhere there.
    fn path(
        &mut self,
        to: &Path,
        bucket: &str,
        prefix: &str,
        relative: &str,
        out: &mut dyn Write,
    ) -> Result<Option<PathBuf>> {
        match local_path(to, relative) {
            Ok(path) => Ok(Some(path)),
            Err(err) => {
                self.0 += 1;
                writeln!(out, "skipped: s3://{bucket}/{prefix}{relative}: {err}")?;
                Ok(None)
            }
        }
    }

    fn finish(self) -> Result<()> {
        if self.0 > 0 {
            bail!("{} object(s) were skipped", self.0);
        }
        Ok(())
    }
}

/// `relative` (a key below a prefix, or a file below a folder) under
/// `root`. A key whose path would leave the folder (`..`) or that names no
/// file (`.`, an empty segment) is refused: a service must not choose where
/// on this machine files are written or removed.
fn local_path(root: &Path, relative: &str) -> Result<PathBuf> {
    relative
        .split('/')
        .try_fold(root.to_path_buf(), |path, part| {
            if matches!(part, "" | "." | "..") {
                bail!("{relative:?} is not a path below {}", root.display());
            }
            Ok(path.join(part))
        })
}

async fn cp(
    client: &Client,
    source: Location,
    destination: Location,
    recursive: bool,
    dryrun: bool,
    rules: &[Rule],
    out: &mut dyn Write,
) -> Result<()> {
    let dry = if dryrun { "(dryrun) " } else { "" };
    match (source, destination) {
        (Location::Local(from), Location::S3 { bucket, key }) => {
            if recursive {
                let prefix = folder(&key);
                for relative in local_tree(&from, true)?
                    .keys()
                    .filter(|path| passes(rules, path))
                {
                    writeln!(
                        out,
                        "{dry}upload: {relative} to s3://{bucket}/{prefix}{relative}"
                    )?;
                    if !dryrun {
                        upload(
                            client,
                            &local_path(&from, relative)?,
                            &bucket,
                            &format!("{prefix}{relative}"),
                        )
                        .await?;
                    }
                }
                return Ok(());
            }
            let name = from
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| anyhow!("{} names no file", from.display()))?;
            let key = if key.is_empty() || key.ends_with('/') {
                format!("{key}{name}")
            } else {
                key
            };
            writeln!(
                out,
                "{dry}upload: {} to s3://{bucket}/{key}",
                from.display()
            )?;
            if !dryrun {
                upload(client, &from, &bucket, &key).await?;
            }
            Ok(())
        }
        (Location::S3 { bucket, key }, Location::Local(to)) => {
            if recursive {
                let prefix = folder(&key);
                let mut skipped = Skipped::default();
                for (relative, entry) in remote_tree(client, &bucket, &prefix)
                    .await?
                    .into_iter()
                    .filter(|(path, _)| passes(rules, path))
                {
                    let Some(path) = skipped.path(&to, &bucket, &prefix, &relative, out)? else {
                        continue;
                    };
                    writeln!(
                        out,
                        "{dry}download: s3://{bucket}/{prefix}{relative} to {relative}"
                    )?;
                    if !dryrun {
                        download(
                            client,
                            &bucket,
                            &format!("{prefix}{relative}"),
                            &path,
                            Some(entry.modified),
                        )
                        .await?;
                    }
                }
                return skipped.finish();
            }
            let name = key.rsplit('/').next().unwrap_or(&key);
            // A destination ending in `/` is a folder, made if need be.
            let path = if to.is_dir() || to.as_os_str().to_string_lossy().ends_with('/') {
                to.join(name)
            } else {
                to
            };
            writeln!(
                out,
                "{dry}download: s3://{bucket}/{key} to {}",
                path.display()
            )?;
            if !dryrun {
                download(client, &bucket, &key, &path, None).await?;
            }
            Ok(())
        }
        _ => bail!("cp copies between a local path and s3://bucket/key"),
    }
}

async fn sync(
    client: &Client,
    source: Location,
    destination: Location,
    delete: bool,
    dryrun: bool,
    rules: &[Rule],
    out: &mut dyn Write,
) -> Result<()> {
    let dry = if dryrun { "(dryrun) " } else { "" };
    match (source, destination) {
        (Location::Local(from), Location::S3 { bucket, key }) => {
            let prefix = folder(&key);
            let steps = plan(
                &local_tree(&from, true)?,
                &remote_tree(client, &bucket, &prefix).await?,
                delete,
                rules,
            );
            for step in steps {
                match step {
                    Step::Send(relative) => {
                        writeln!(
                            out,
                            "{dry}upload: {relative} to s3://{bucket}/{prefix}{relative}"
                        )?;
                        if !dryrun {
                            upload(
                                client,
                                &local_path(&from, &relative)?,
                                &bucket,
                                &format!("{prefix}{relative}"),
                            )
                            .await?;
                        }
                    }
                    Step::Remove(relative) => {
                        writeln!(out, "{dry}delete: s3://{bucket}/{prefix}{relative}")?;
                        if !dryrun {
                            client
                                .delete_object(&bucket, &format!("{prefix}{relative}"))
                                .await?;
                        }
                    }
                }
            }
            Ok(())
        }
        (Location::S3 { bucket, key }, Location::Local(to)) => {
            let prefix = folder(&key);
            let remote = remote_tree(client, &bucket, &prefix).await?;
            let steps = plan(&remote, &local_tree(&to, false)?, delete, rules);
            let mut skipped = Skipped::default();
            for step in steps {
                match step {
                    Step::Send(relative) => {
                        let Some(path) = skipped.path(&to, &bucket, &prefix, &relative, out)?
                        else {
                            continue;
                        };
                        writeln!(
                            out,
                            "{dry}download: s3://{bucket}/{prefix}{relative} to {relative}"
                        )?;
                        if !dryrun {
                            let modified = remote.get(&relative).map(|entry| entry.modified);
                            download(
                                client,
                                &bucket,
                                &format!("{prefix}{relative}"),
                                &path,
                                modified,
                            )
                            .await?;
                        }
                    }
                    Step::Remove(relative) => {
                        writeln!(out, "{dry}delete: {relative}")?;
                        if !dryrun {
                            std::fs::remove_file(local_path(&to, &relative)?)?;
                        }
                    }
                }
            }
            skipped.finish()
        }
        _ => bail!("sync syncs between a local folder and s3://bucket/prefix"),
    }
}

async fn rm(
    client: &Client,
    target: Location,
    recursive: bool,
    dryrun: bool,
    rules: &[Rule],
    out: &mut dyn Write,
) -> Result<()> {
    let dry = if dryrun { "(dryrun) " } else { "" };
    let Location::S3 { bucket, key } = target else {
        bail!("rm takes s3://bucket/key");
    };
    if !recursive {
        writeln!(out, "{dry}delete: s3://{bucket}/{key}")?;
        if !dryrun {
            client.delete_object(&bucket, &key).await?;
        }
        return Ok(());
    }
    let prefix = folder(&key);
    for relative in remote_tree(client, &bucket, &prefix)
        .await?
        .into_keys()
        .filter(|path| passes(rules, path))
    {
        writeln!(out, "{dry}delete: s3://{bucket}/{prefix}{relative}")?;
        if !dryrun {
            client
                .delete_object(&bucket, &format!("{prefix}{relative}"))
                .await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patterns_match_as_aws_s3_matches_them() {
        assert!(glob("*", "a/b/c.txt"));
        assert!(glob("*.txt", "a/b/c.txt"));
        assert!(!glob("*.txt", "a/b/c.txt.bak"));
        assert!(glob("a/?/c*", "a/b/cd"));
        assert!(!glob("a/?/c", "a/bb/c"));
        assert!(glob("**x", "yx"));
        assert!(glob("", ""));
        assert!(!glob("", "a"));
        let rules = [
            Rule::Exclude("*".to_string()),
            Rule::Include("*.txt".to_string()),
            Rule::Exclude("secret/*".to_string()),
        ];
        assert!(passes(&rules, "a.txt"));
        assert!(!passes(&rules, "a.bin"));
        assert!(!passes(&rules, "secret/a.txt"), "the later rule wins");
        assert!(passes(&[], "anything"));
    }

    #[test]
    fn a_sync_sends_the_missing_the_resized_and_the_newer() {
        let tree = |entries: &[(&str, u64, u64)]| -> BTreeMap<String, Entry> {
            entries
                .iter()
                .map(|(path, size, modified)| {
                    (
                        path.to_string(),
                        Entry {
                            size: *size,
                            modified: *modified,
                        },
                    )
                })
                .collect()
        };
        let source = tree(&[
            ("same", 1, 5),
            ("newer", 1, 9),
            ("older", 1, 1),
            ("resized", 2, 1),
            ("new", 1, 1),
            ("skip.log", 1, 1),
        ]);
        let destination = tree(&[
            ("same", 1, 5),
            ("newer", 1, 5),
            ("older", 1, 5),
            ("resized", 1, 9),
            ("gone", 1, 1),
            ("kept.log", 1, 1),
        ]);
        let rules = [Rule::Exclude("*.log".to_string())];
        assert_eq!(
            plan(&source, &destination, false, &rules),
            [
                Step::Send("new".to_string()),
                Step::Send("newer".to_string()),
                Step::Send("resized".to_string()),
            ]
        );
        let with_delete = plan(&source, &destination, true, &rules);
        assert!(with_delete.contains(&Step::Remove("gone".to_string())));
        assert!(
            !with_delete.contains(&Step::Remove("kept.log".to_string())),
            "filtered out"
        );
        assert!(!with_delete.contains(&Step::Send("same".to_string())));
    }

    #[test]
    fn a_key_cannot_put_a_file_outside_the_folder() {
        let root = Path::new("/data/dest");
        assert_eq!(
            local_path(root, "a/b.txt").unwrap(),
            root.join("a").join("b.txt")
        );
        for bad in ["../x", "a/../../x", "a//b", "./a", "a/", ""] {
            assert!(local_path(root, bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn locations_dates_and_credentials_read_as_given() {
        assert_eq!(
            location("s3://b/k/x"),
            Location::S3 {
                bucket: "b".to_string(),
                key: "k/x".to_string()
            }
        );
        assert_eq!(
            location("s3://b"),
            Location::S3 {
                bucket: "b".to_string(),
                key: String::new()
            }
        );
        assert_eq!(location("./d"), Location::Local(PathBuf::from("./d")));
        assert_eq!(iso_seconds("1970-01-01T00:00:00.000Z"), Some(0));
        assert_eq!(iso_seconds("2013-05-24T00:00:01Z"), Some(1_369_353_601));
        assert_eq!(iso_seconds("2000-03-01T00:00:00Z"), Some(951_868_800));
        assert_eq!(iso_seconds("nonsense"), None);
        let file = "[default]\naws_access_key_id = A\naws_secret_access_key=S\n\n[other]\n\
                    aws_access_key_id=B\naws_secret_access_key = T\n";
        assert_eq!(
            profile_keys(file, "other"),
            Some(("B".to_string(), "T".to_string()))
        );
        assert_eq!(
            profile_keys(file, "default"),
            Some(("A".to_string(), "S".to_string()))
        );
        assert_eq!(profile_keys(file, "missing"), None);
        assert_eq!(folder("a"), "a/");
        assert_eq!(folder(""), "");
    }
}
