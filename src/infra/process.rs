use std::collections::BTreeMap;
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

pub fn run_direct(
    command: &str,
    args: &[String],
    env: &BTreeMap<String, String>,
    cwd: &Path,
) -> Result<i32, String> {
    let status = Command::new(command)
        .args(args)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .env_clear()
        .envs(env)
        .current_dir(cwd)
        .status()
        .map_err(|error| format!("failed to run \"{command}\": {error}"))?;
    Ok(status.code().unwrap_or(1))
}

pub fn run_shell(command: &str, env: &BTreeMap<String, String>, cwd: &Path) -> Result<i32, String> {
    if cfg!(windows) {
        run_direct("cmd", &["/C".to_string(), command.to_string()], env, cwd)
    } else {
        run_direct("sh", &["-lc".to_string(), command.to_string()], env, cwd)
    }
}

/// Run a command, capture output, and kill the child if it exceeds `timeout`.
pub(crate) fn command_output(
    mut command: Command,
    timeout: Duration,
    label: &str,
) -> Result<Output, String> {
    command.stdin(Stdio::null());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }

    let deadline = Instant::now() + timeout;
    let mut child = command
        .spawn()
        .map_err(|error| format!("failed to run {label}: {error}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| format!("{label} stdout was not captured"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| format!("{label} stderr was not captured"))?;

    let (stdout_tx, stdout_rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = stdout_tx.send(read_pipe(stdout));
    });
    let (stderr_tx, stderr_rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = stderr_tx.send(read_pipe(stderr));
    });

    let status = wait_for_child(&mut child, timeout, label);
    let stdout = recv_pipe(stdout_rx, deadline, &mut child, label, "stdout");
    let stderr = recv_pipe(stderr_rx, deadline, &mut child, label, "stderr");
    let status = match status {
        Ok(status) => status,
        Err(error) => return Err(error),
    };
    Ok(Output {
        status,
        stdout: stdout?,
        stderr: stderr?,
    })
}

fn recv_pipe(
    rx: mpsc::Receiver<Vec<u8>>,
    deadline: Instant,
    child: &mut Child,
    label: &str,
    stream: &str,
) -> Result<Vec<u8>, String> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    let wait = if remaining.is_zero() {
        Duration::from_millis(1)
    } else {
        remaining
    };
    match rx.recv_timeout(wait) {
        Ok(buf) => Ok(buf),
        Err(RecvTimeoutError::Timeout) => {
            terminate_child(child);
            match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(buf) => Ok(buf),
                Err(RecvTimeoutError::Timeout) => Err(format!(
                    "{label} timed out draining {stream} after {wait:?}"
                )),
                Err(RecvTimeoutError::Disconnected) => {
                    Err(format!("{label} {stream} reader disconnected"))
                }
            }
        }
        Err(RecvTimeoutError::Disconnected) => Err(format!("{label} {stream} reader disconnected")),
    }
}

fn read_pipe(mut reader: impl std::io::Read) -> Vec<u8> {
    let mut buf = Vec::new();
    let _ = reader.read_to_end(&mut buf);
    buf
}

pub(crate) fn wait_for_child(
    child: &mut Child,
    timeout: Duration,
    label: &str,
) -> Result<ExitStatus, String> {
    let started_at = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(status),
            Ok(None) if started_at.elapsed() < timeout => {
                thread::sleep(Duration::from_millis(25));
            }
            Ok(None) => {
                terminate_child(child);
                return Err(format!("{label} timed out after {timeout:?}"));
            }
            Err(error) => {
                terminate_child(child);
                return Err(format!("failed waiting for {label}: {error}"));
            }
        }
    }
}

fn terminate_child(child: &mut Child) {
    #[cfg(unix)]
    {
        let process_group = format!("-{}", child.id());
        let _ = Command::new("kill")
            .args(["-TERM", "--", &process_group])
            .status();
        for _ in 0..20 {
            match child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => thread::sleep(Duration::from_millis(25)),
                Err(_) => break,
            }
        }
        let _ = Command::new("kill")
            .args(["-KILL", "--", &process_group])
            .status();
    }
    let _ = child.kill();
    let _ = child.wait();
}
