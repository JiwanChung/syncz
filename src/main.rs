use anyhow::{anyhow, bail, Context, Result};
use clap::{ArgAction, Parser};
use dialoguer::{theme::ColorfulTheme, FuzzySelect};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use notify::{Config, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

trait CommandRunner {
    fn status(&self, program: &str, args: &[String]) -> Result<std::process::ExitStatus>;
    fn output(&self, program: &str, args: &[String]) -> Result<std::process::Output>;
}

struct RealRunner;

impl CommandRunner for RealRunner {
    fn status(&self, program: &str, args: &[String]) -> Result<std::process::ExitStatus> {
        let status = Command::new(program).args(args).status()?;
        Ok(status)
    }

    fn output(&self, program: &str, args: &[String]) -> Result<std::process::Output> {
        let output = Command::new(program).args(args).output()?;
        Ok(output)
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about = "syncz: rsync + ssh with smart pathing")]
struct Args {
    /// Local path to sync (push) or path to pull into (pull). Defaults to current directory.
    path: Option<String>,

    /// Host to sync with; if omitted, the last used host or a picker is used.
    host: Option<String>,

    /// Push local -> remote (default is bidirectional)
    #[arg(long, action = ArgAction::SetTrue, conflicts_with = "pull")]
    push: bool,

    /// Pull remote -> local (default is bidirectional)
    #[arg(long, action = ArgAction::SetTrue, conflicts_with = "push")]
    pull: bool,

    /// Watch for file changes and sync automatically (defaults to Push mode)
    #[arg(short = 'w', long, action = ArgAction::SetTrue)]
    watch: bool,

    /// Sync everything (disable default smart excludes and size limits)
    #[arg(short = 'a', long, action = ArgAction::SetTrue)]
    all: bool,

    /// Allow large files (disables the default 10MB size limit)
    #[arg(short = 'l', long, action = ArgAction::SetTrue)]
    large: bool,

    /// Use .gitignore to exclude files
    #[arg(short = 'g', long, action = ArgAction::SetTrue)]
    gitignore: bool,

    /// Override max size limit (e.g. 100M, 1G)
    #[arg(long)]
    max_size: Option<String>,

    /// Backup updated/deleted files on the destination
    #[arg(short = 'b', long, action = ArgAction::SetTrue)]
    backup: bool,

    /// Dry run: show a tree-style diff and transfer size
    #[arg(short = 'd', long, action = ArgAction::SetTrue)]
    dry_run: bool,

    /// Skip syncing permissions (useful for macOS/Linux UID/GID clashes)
    #[arg(long, action = ArgAction::SetTrue)]
    no_perms: bool,
}

impl Args {
    fn is_push(&self) -> bool {
        self.push || (!self.push && !self.pull)
    }

    fn is_pull(&self) -> bool {
        self.pull || (!self.push && !self.pull)
    }
}

fn main() -> Result<()> {
    let mut args = Args::parse();
    let runner = RealRunner;

    if args.path.is_some() && args.host.is_none() {
        let p = args.path.as_ref().unwrap();
        if !Path::new(p).exists() {
            args.host = args.path.take();
            args.path = Some(".".to_string());
        }
    }

    let path_str = args.path.as_deref().unwrap_or(".");
    let local_path = expand_path(path_str)?;
    let local_path = normalize_path(&local_path)?;
    let home = dirs::home_dir().ok_or_else(|| anyhow!("unable to resolve home dir"))?;

    let host = match &args.host {
        Some(h) => {
            save_last_host(h)?;
            h.clone()
        }
        None => match load_last_host()? {
            Some(h) => h,
            None => {
                let h = pick_host_from_ssh_config()?;
                save_last_host(&h)?;
                h
            }
        },
    };

    let remote_path = map_to_remote(&local_path, &home);

    if args.watch {
        println!("👀 Watching for changes in {}...", local_path.display());
        watch_loop(&runner, &host, &local_path, &remote_path, &args)?;
    } else {
        if args.is_push() {
            let context = if args.is_pull() { "[Upstream]" } else { "" };
            push(&runner, &host, &local_path, &remote_path, &args, context)?;
        }

        if args.is_pull() {
            let context = if args.is_push() { "[Downstream]" } else { "" };
            pull(&runner, &host, &local_path, &remote_path, &args, context)?;
        }
    }

    Ok(())
}
fn get_state_path() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| anyhow!("unable to resolve home dir"))?;
    Ok(home.join(".syncz_state"))
}

fn save_last_host(host: &str) -> Result<()> {
    let path = get_state_path()?;
    fs::write(path, host).context("failed to save last host")?;
    Ok(())
}

fn load_last_host() -> Result<Option<String>> {
    let path = get_state_path()?;
    if !path.exists() {
        return Ok(None);
    }
    let host = fs::read_to_string(path)?.trim().to_string();
    if host.is_empty() {
        Ok(None)
    } else {
        Ok(Some(host))
    }
}

fn watch_loop(
    runner: &dyn CommandRunner,
    host: &str,
    local_path: &Path,
    remote_path: &str,
    args: &Args,
) -> Result<()> {
    let (tx, rx) = std::sync::mpsc::channel();
    let mut watcher = RecommendedWatcher::new(tx, Config::default())?;

    watcher.watch(local_path, RecursiveMode::Recursive)?;

    let debounce_duration = Duration::from_millis(500);
    let mut last_event = Instant::now();

    loop {
        if let Ok(event) = rx.recv() {
            match event {
                Ok(_) => {
                    std::thread::sleep(Duration::from_millis(100));
                    while let Ok(_) = rx.try_recv() {}

                    if last_event.elapsed() > debounce_duration {
                        println!("🔄 Change detected, syncing...");
                        if let Err(e) = push(runner, host, local_path, remote_path, args, "[Watch]") {
                            eprintln!("❌ Sync failed: {}", e);
                        } else {
                            println!("✅ Synced.");
                        }
                        last_event = Instant::now();
                    }
                }
                Err(e) => eprintln!("❌ Watch error: {:?}", e),
            }
        }
    }
}

fn expand_path(raw: &str) -> Result<PathBuf> {
    if raw.starts_with('~') {
        let home = dirs::home_dir().ok_or_else(|| anyhow!("unable to resolve home dir"))?;
        let mut expanded = PathBuf::from(home);
        let rest = raw.trim_start_matches('~');
        expanded.push(rest.trim_start_matches('/'));
        Ok(expanded)
    } else {
        Ok(PathBuf::from(raw))
    }
}

fn normalize_path(path: &Path) -> Result<PathBuf> {
    let p = if path.is_relative() {
        std::env::current_dir()?.join(path)
    } else {
        path.to_path_buf()
    };
    Ok(clean_path(&p))
}

fn clean_path(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}

fn map_to_remote(local: &Path, home: &Path) -> String {
    if let Ok(rel) = local.strip_prefix(home) {
        let mut remote = PathBuf::from("~");
        remote.push(rel);
        remote.to_string_lossy().to_string()
    } else {
        local.to_string_lossy().to_string()
    }
}
fn pick_host_from_ssh_config() -> Result<String> {
    let hosts = read_ssh_hosts()?;
    if hosts.is_empty() {
        bail!("no hosts found in ~/.ssh/config and no host provided");
    }

    let selection = FuzzySelect::with_theme(&ColorfulTheme::default())
        .with_prompt("Select SSH host")
        .items(&hosts)
        .default(0)
        .interact()?;

    Ok(hosts[selection].clone())
}

fn read_ssh_hosts() -> Result<Vec<String>> {
    let home = dirs::home_dir().ok_or_else(|| anyhow!("unable to resolve home dir"))?;
    let config_path = home.join(".ssh").join("config");
    if !config_path.exists() {
        return Ok(Vec::new());
    }

    let contents = fs::read_to_string(&config_path)
        .with_context(|| format!("failed to read {}", config_path.display()))?;

    let mut hosts = Vec::new();
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.split_whitespace();
        let key = parts.next().unwrap_or_default();
        if !key.eq_ignore_ascii_case("Host") {
            continue;
        }
        for host in parts {
            if host.contains('*') || host.contains('?') {
                continue;
            }
            hosts.push(host.to_string());
        }
    }

    hosts.sort();
    hosts.dedup();
    Ok(hosts)
}

fn push(
    runner: &dyn CommandRunner,
    host: &str,
    local_path: &Path,
    remote_path: &str,
    args: &Args,
    context: &str,
) -> Result<()> {
    let is_file = local_path.is_file();
    let remote_parent = if is_file {
        parent_of_remote(remote_path)
    } else {
        parent_of_remote(remote_path)
    };

    ensure_remote_parent(runner, host, &remote_parent)?;

    if args.dry_run {
        if !context.is_empty() {
            println!("{}", context);
        }
        let summary = run_dry_run(runner, host, local_path, remote_path, is_file, args, false)?;
        println!("{}", summary.tree);
        if let Some(line) = summary.transferred_line {
            println!("{}", line);
        }
        Ok(())
    } else {
        if !context.is_empty() {
            println!("{}", context);
        }
        run_rsync(host, local_path, remote_path, is_file, args, false)
    }
}

fn pull(
    runner: &dyn CommandRunner,
    host: &str,
    local_path: &Path,
    remote_path: &str,
    args: &Args,
    context: &str,
) -> Result<()> {
    let is_file = remote_is_file(runner, host, remote_path).unwrap_or(false);
    let local_parent = if is_file {
        local_path
            .parent()
            .ok_or_else(|| anyhow!("unable to resolve local parent"))?
    } else {
        local_path
            .parent()
            .ok_or_else(|| anyhow!("unable to resolve local parent"))?
    };

    fs::create_dir_all(local_parent)
        .with_context(|| format!("failed to create {}", local_parent.display()))?;

    if args.dry_run {
        if !context.is_empty() {
            println!("{}", context);
        }
        let summary = run_dry_run(runner, host, local_path, remote_path, is_file, args, true)?;
        println!("{}", summary.tree);
        if let Some(line) = summary.transferred_line {
            println!("{}", line);
        }
        Ok(())
    } else {
        if !context.is_empty() {
            println!("{}", context);
        }
        run_rsync(host, local_path, remote_path, is_file, args, true)
    }
}
fn ssh_options() -> Vec<String> {
    vec![
        "-o".to_string(),
        "ConnectTimeout=10".to_string(),
        "-o".to_string(),
        "ServerAliveInterval=15".to_string(),
        "-o".to_string(),
        "ServerAliveCountMax=2".to_string(),
    ]
}

fn remote_is_file(runner: &dyn CommandRunner, host: &str, remote_path: &str) -> Result<bool> {
    let mut args = ssh_options();
    args.push(host.to_string());
    args.push(format!("test -f {}", remote_shell_path(remote_path)));
    let status = runner
        .status("ssh", &args)
        .with_context(|| "failed to run ssh test -f")?;
    Ok(status.success())
}

fn parent_of_remote(remote_path: &str) -> String {
    let path = Path::new(remote_path);
    if let Some(parent) = path.parent() {
        parent.to_string_lossy().to_string()
    } else {
        remote_path.to_string()
    }
}

fn ensure_remote_parent(
    runner: &dyn CommandRunner,
    host: &str,
    remote_parent: &str,
) -> Result<()> {
    let mut args = ssh_options();
    args.push(host.to_string());
    args.push(format!("mkdir -p {}", remote_shell_path(remote_parent)));
    let status = runner
        .status("ssh", &args)
        .with_context(|| "failed to run ssh mkdir -p")?;
    if !status.success() {
        bail!("failed to create remote directory {}", remote_parent);
    }
    Ok(())
}

struct DryRunSummary {
    tree: String,
    transferred_line: Option<String>,
}

fn run_dry_run(
    runner: &dyn CommandRunner,
    host: &str,
    local_path: &Path,
    remote_path: &str,
    is_file: bool,
    args: &Args,
    pulling: bool,
) -> Result<DryRunSummary> {
    let (src, dst) = sync_endpoints(host, local_path, remote_path, is_file, pulling);

    let mut cmd_args = base_rsync_args(args, true);
    cmd_args.push("--dry-run".to_string());
    cmd_args.push("--itemize-changes".to_string());
    cmd_args.push("--out-format=%i|%n|%l".to_string());
    cmd_args.push(src);
    cmd_args.push(dst);
    let output = runner
        .output("rsync", &cmd_args)
        .with_context(|| "failed to run rsync --dry-run")?;

    if !output.status.success() {
        bail!("rsync dry run failed");
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let tree = render_tree(&stdout);

    let stats = String::from_utf8_lossy(&output.stderr);
    let transferred_line = stats
        .lines()
        .find(|line| line.starts_with("Total transferred file size:"))
        .map(|line| line.trim().to_string());

    Ok(DryRunSummary {
        tree,
        transferred_line,
    })
}
fn run_rsync(
    host: &str,
    local_path: &Path,
    remote_path: &str,
    is_file: bool,
    args: &Args,
    pulling: bool,
) -> Result<()> {
    let (src, dst) = sync_endpoints(host, local_path, remote_path, is_file, pulling);

    let mut cmd = Command::new("rsync");
    let mut base_args = base_rsync_args(args, false);
    if !base_args.iter().any(|a| a == "--itemize-changes") {
        base_args.push("--itemize-changes".to_string());
    }
    cmd.args(base_args);
    cmd.arg(src);
    cmd.arg(dst);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn().with_context(|| "failed to spawn rsync")?;

    let overall = ProgressBar::new(100);
    overall.set_style(
        ProgressStyle::with_template("{msg} {wide_bar} {pos}%")
            .unwrap()
            .progress_chars("=> "),
    );
    overall.set_message("Overall");

    let current = ProgressBar::new_spinner();
    current.set_message("Waiting for files...");
    current.enable_steady_tick(Duration::from_millis(100));

    let mp = MultiProgress::new();
    let overall = mp.add(overall);
    let current = mp.add(current);

    let overall = Arc::new(overall);
    let current = Arc::new(current);
    let stats_lines = Arc::new(Mutex::new(Vec::new()));
    let itemized_lines = Arc::new(Mutex::new(Vec::new()));

    let stdout = child.stdout.take().ok_or_else(|| anyhow!("no stdout"))?;
    let stderr = child.stderr.take().ok_or_else(|| anyhow!("no stderr"))?;

    let current_clone = Arc::clone(&current);
    let itemized_clone = Arc::clone(&itemized_lines);
    let stdout_handle = std::thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines().flatten() {
            if line.trim().is_empty() {
                continue;
            }
            if line.contains('|') {
                if let Ok(mut guard) = itemized_clone.lock() {
                    guard.push(line.clone());
                }
                let parts: Vec<&str> = line.split('|').collect();
                if parts.len() >= 2 {
                    current_clone.set_message(parts[1].to_string());
                }
            } else {
                current_clone.set_message(line);
            }
        }
    });

    let overall_clone = Arc::clone(&overall);
    let stats_clone = Arc::clone(&stats_lines);
    let stderr_handle = std::thread::spawn(move || {
        let reader = BufReader::new(stderr);
        for line in reader.lines().flatten() {
            if let Some(percent) = parse_progress_percent(&line) {
                overall_clone.set_position(percent as u64);
            }
            if line.starts_with("sent ") || line.starts_with("total size is ") {
                if let Ok(mut guard) = stats_clone.lock() {
                    guard.push(line);
                }
            }
        }
    });

    let start = Instant::now();
    let status = child.wait().with_context(|| "failed to wait on rsync")?;
    let duration = start.elapsed();

    let _ = stdout_handle.join();
    let _ = stderr_handle.join();

    overall.finish_with_message("Overall");
    current.finish_with_message("Done");

    if !status.success() {
        bail!("rsync failed");
    }

    if let Ok(guard) = itemized_lines.lock() {
        if !guard.is_empty() {
            println!("Changes:");
            let itemized_blob = guard.join("\n");
            println!("{}", render_tree(&itemized_blob));
        }
    }

    let stats = stats_lines.lock().ok().map(|lines| lines.clone()).unwrap_or_default();
    print_summary(&stats, duration);

    Ok(())
}

fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn parse_bytes(s: &str) -> Option<u64> {
    s.replace(",", "").parse().ok()
}

fn print_summary(stats: &[String], duration: Duration) {
    let mut sent_bytes: Option<u64> = None;
    let mut total_bytes: Option<u64> = None;

    for line in stats {
        let line = line.trim();
        if line.starts_with("sent ") {
            if let Some(bytes_str) = line.strip_prefix("sent ") {
                if let Some(end) = bytes_str.find(" bytes") {
                    sent_bytes = parse_bytes(&bytes_str[..end]);
                }
            }
        }
        if line.starts_with("total size is ") {
            if let Some(rest) = line.strip_prefix("total size is ") {
                if let Some(end) = rest.find("  ") {
                    total_bytes = parse_bytes(&rest[..end]);
                } else {
                    total_bytes = parse_bytes(rest);
                }
            }
        }
    }

    println!("Summary:");
    if let Some(bytes) = sent_bytes {
        println!("  sent: {}", format_size(bytes));
    }
    if let Some(bytes) = total_bytes {
        println!("  total size: {}", format_size(bytes));
    }
    println!("  duration: {:.2?}", duration);
}
fn base_rsync_args(args: &Args, dry_run: bool) -> Vec<String> {
    let mut list = vec!["-avzu".to_string()];
    if !dry_run {
        list.push("-P".to_string());
        list.push("--partial".to_string());
        list.push("--inplace".to_string());
        list.push("--info=progress2".to_string());
        list.push("--out-format=%i|%n".to_string());
    }
    list.push("-e".to_string());
    list.push(format!("ssh {}", ssh_options().join(" ")));
    list.push("--stats".to_string());

    if !args.all {
        list.push("--exclude=*.o".to_string());
        list.push("--exclude=*.obj".to_string());
        list.push("--exclude=*.a".to_string());
        list.push("--exclude=*.lib".to_string());
        list.push("--exclude=*.so".to_string());
        list.push("--exclude=*.dylib".to_string());
        list.push("--exclude=*.dll".to_string());
        list.push("--exclude=*.exe".to_string());
        list.push("--exclude=__pycache__/".to_string());
        list.push("--exclude=*.pyc".to_string());
        list.push("--exclude=.git/".to_string());
        list.push("--exclude=node_modules/".to_string());
        list.push("--exclude=target/".to_string());
        list.push("--exclude=.next/".to_string());
        list.push("--exclude=dist/".to_string());
        list.push("--exclude=build/".to_string());
        list.push("--exclude=.terraform/".to_string());
        list.push("--exclude=.DS_Store".to_string());
        list.push("--exclude=Thumbs.db".to_string());
        list.push("--exclude=*.swp".to_string());
        list.push("--exclude=*~".to_string());
    }

    if args.gitignore {
        list.push("--filter=:- .gitignore".to_string());
    }

    if let Some(max_size) = &args.max_size {
        list.push(format!("--max-size={}", max_size));
    } else if !args.large && !args.all {
        list.push("--max-size=10m".to_string());
    }

    if args.backup {
        list.push("--backup".to_string());
        list.push("--backup-dir=.syncz-backups".to_string());
    }

    if args.no_perms {
        list.push("--no-perms".to_string());
    }

    list
}

fn sync_endpoints(
    host: &str,
    local_path: &Path,
    remote_path: &str,
    is_file: bool,
    pulling: bool,
) -> (String, String) {
    let (local, remote) = if is_file {
        (local_path.to_string_lossy().to_string(), remote_path.to_string())
    } else {
        (
            format!("{}/", local_path.to_string_lossy()),
            format!("{}/", remote_path),
        )
    };

    let remote = format!("{}:{}", host, remote);
    if pulling {
        (remote, local)
    } else {
        (local, remote)
    }
}

fn parse_progress_percent(line: &str) -> Option<u8> {
    if !line.contains('%') {
        return None;
    }
    let mut pct = None;
    for token in line.split_whitespace() {
        if let Some(num) = token.strip_suffix('%') {
            if let Ok(value) = num.parse::<u8>() {
                pct = Some(value);
                break;
            }
        }
    }
    pct
}

fn render_tree(output: &str) -> String {
    let mut root = TreeNode::default();

    for line in output.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('|').collect();
        if parts.len() < 2 {
            continue;
        }
        let item = parts[1].trim_start_matches("./");
        if item.is_empty() || item.starts_with('.') {
            continue;
        }
        insert_path(&mut root, item);
    }

    let mut lines = Vec::new();
    for (idx, (name, node)) in root.children.iter().enumerate() {
        let last = idx + 1 == root.children.len();
        render_node(&mut lines, name, node, "", last);
    }
    lines.join("\n")
}

#[derive(Default)]
struct TreeNode {
    children: BTreeMap<String, TreeNode>,
}

fn insert_path(root: &mut TreeNode, path: &str) {
    let mut node = root;
    for part in path.split('/') {
        if part.is_empty() {
            continue;
        }
        node = node.children.entry(part.to_string()).or_default();
    }
}

fn render_node(lines: &mut Vec<String>, name: &str, node: &TreeNode, prefix: &str, last: bool) {
    let branch = if last { "+--" } else { "|--" };
    lines.push(format!("{}{} {}", prefix, branch, name));

    let next_prefix = if last {
        format!("{}   ", prefix)
    } else {
        format!("{}|  ", prefix)
    };

    let mut iter = node.children.iter().peekable();
    while let Some((child_name, child_node)) = iter.next() {
        let is_last = iter.peek().is_none();
        render_node(lines, child_name, child_node, &next_prefix, is_last);
    }
}

fn shell_escape(value: &str) -> String {
    let mut out = String::from("'");
    for ch in value.chars() {
        if ch == '\'' {
            out.push_str("'\\''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
    out
}

fn shell_escape_double(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        match ch {
            '\\' | '"' | '$' | '`' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

fn remote_shell_path(path: &str) -> String {
    if path == "~" {
        return "\"$HOME\"".to_string();
    }
    if let Some(rest) = path.strip_prefix("~/") {
        return format!("\"$HOME/{}\"", shell_escape_double(rest));
    }
    shell_escape(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::os::unix::process::ExitStatusExt;
    use std::process::Output;

    struct ExpectedCall {
        program: String,
        args: Vec<String>,
        output: Option<Output>,
        status: Option<std::process::ExitStatus>,
    }

    struct FakeRunner {
        calls: Mutex<VecDeque<ExpectedCall>>,
    }

    impl FakeRunner {
        fn new(calls: Vec<ExpectedCall>) -> Self {
            Self {
                calls: Mutex::new(VecDeque::from(calls)),
            }
        }

        fn next_call(&self) -> ExpectedCall {
            let mut guard = self.calls.lock().expect("lock calls");
            guard.pop_front().expect("expected call")
        }
    }

    impl CommandRunner for FakeRunner {
        fn status(&self, program: &str, args: &[String]) -> Result<std::process::ExitStatus> {
            let call = self.next_call();
            assert_eq!(call.program, program);
            assert_eq!(call.args, args);
            Ok(call.status.expect("expected status"))
        }

        fn output(&self, program: &str, args: &[String]) -> Result<Output> {
            let call = self.next_call();
            assert_eq!(call.program, program);
            assert_eq!(call.args, args);
            Ok(call.output.expect("expected output"))
        }
    }

    fn ok_status() -> std::process::ExitStatus {
        std::process::ExitStatus::from_raw(0)
    }

    #[test]
    fn args_default_to_bidirectional() {
        let args = Args {
            path: None,
            host: None,
            push: false,
            pull: false,
            watch: false,
            all: false,
            large: false,
            gitignore: false,
            max_size: None,
            backup: false,
            dry_run: false,
            no_perms: false,
        };
        assert!(args.is_push());
        assert!(args.is_pull());
    }

    #[test]
    fn args_push_only() {
        let args = Args {
            path: None,
            host: None,
            push: true,
            pull: false,
            watch: false,
            all: false,
            large: false,
            gitignore: false,
            max_size: None,
            backup: false,
            dry_run: false,
            no_perms: false,
        };
        assert!(args.is_push());
        assert!(!args.is_pull());
    }

    #[test]
    fn args_pull_only() {
        let args = Args {
            path: None,
            host: None,
            push: false,
            pull: true,
            watch: false,
            all: false,
            large: false,
            gitignore: false,
            max_size: None,
            backup: false,
            dry_run: false,
            no_perms: false,
        };
        assert!(!args.is_push());
        assert!(args.is_pull());
    }

    #[test]
    fn test_base_rsync_args_logic() {
        let mut args = Args {
            path: None,
            host: None,
            push: false,
            pull: false,
            watch: false,
            all: false,
            large: false,
            gitignore: false,
            max_size: None,
            backup: false,
            dry_run: true,
            no_perms: false,
        };

        let rsync_args = base_rsync_args(&args, true);
        assert!(rsync_args.iter().any(|a| a == "--max-size=10m"));

        args.large = true;
        let rsync_args = base_rsync_args(&args, true);
        assert!(!rsync_args.iter().any(|a| a == "--max-size=10m"));

        args.all = true;
        let rsync_args = base_rsync_args(&args, true);
        assert!(!rsync_args.iter().any(|a| a == "--max-size=10m"));

        args.gitignore = true;
        let rsync_args = base_rsync_args(&args, true);
        assert!(rsync_args.iter().any(|a| a == "--filter=:- .gitignore"));

        args.backup = true;
        let rsync_args = base_rsync_args(&args, true);
        assert!(rsync_args.iter().any(|a| a == "--backup"));
        assert!(rsync_args.iter().any(|a| a == "--backup-dir=.syncz-backups"));
    }

    #[test]
    fn remote_is_file_uses_ssh() {
        let host = "example";
        let remote = "~/projects/app/file.txt";
        let mut args = ssh_options();
        args.push(host.to_string());
        args.push(format!("test -f {}", remote_shell_path(remote)));

        let runner = FakeRunner::new(vec![ExpectedCall {
            program: "ssh".to_string(),
            args,
            output: None,
            status: Some(ok_status()),
        }]);

        let is_file = remote_is_file(&runner, host, remote).expect("remote_is_file");
        assert!(is_file);
    }

    #[test]
    fn ensure_remote_parent_creates_dir() {
        let host = "example";
        let remote_parent = "~/projects/app";
        let mut args = ssh_options();
        args.push(host.to_string());
        args.push(format!("mkdir -p {}", remote_shell_path(remote_parent)));

        let runner = FakeRunner::new(vec![ExpectedCall {
            program: "ssh".to_string(),
            args,
            output: None,
            status: Some(ok_status()),
        }]);

        ensure_remote_parent(&runner, host, remote_parent).expect("ensure_remote_parent");
    }

    #[test]
    fn dry_run_parses_tree_and_stats() {
        let args = Args {
            path: None,
            host: Some("example".to_string()),
            push: false,
            pull: false,
            watch: false,
            all: false,
            large: false,
            gitignore: false,
            max_size: None,
            backup: false,
            dry_run: true,
            no_perms: false,
        };
        let local_path = Path::new("/home/user/projects/app");
        let remote_path = "~/projects/app";
        let (src, dst) = sync_endpoints("example", local_path, remote_path, false, false);

        let mut cmd_args = base_rsync_args(&args, true);
        cmd_args.push("--dry-run".to_string());
        cmd_args.push("--itemize-changes".to_string());
        cmd_args.push("--out-format=%i|%n|%l".to_string());
        cmd_args.push(src);
        cmd_args.push(dst);

        let stdout = b"f+++++++++|foo.txt|12\nd+++++++++|dir/|0\nf+++++++++|dir/bar.txt|24\n";
        let stderr = b"Total transferred file size: 36 bytes\n";
        let output = Output {
            status: ok_status(),
            stdout: stdout.to_vec(),
            stderr: stderr.to_vec(),
        };

        let runner = FakeRunner::new(vec![ExpectedCall {
            program: "rsync".to_string(),
            args: cmd_args,
            output: Some(output),
            status: None,
        }]);

        let summary =
            run_dry_run(&runner, "example", local_path, remote_path, false, &args, false).unwrap();
        assert!(summary.tree.contains("+-- foo.txt"));
        assert!(summary.tree.lines().any(|line| line.ends_with(" dir")));
        assert!(summary.tree.contains("+-- bar.txt"));
        assert_eq!(
            summary.transferred_line.as_deref(),
            Some("Total transferred file size: 36 bytes")
        );
    }
}
