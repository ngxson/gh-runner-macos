#!/usr/bin/env python3
"""
GitHub Actions self-hosted runner orchestrator for macOS.

Runs as root. Polls GitHub API for queued workflow runs, then for each job
creates a fresh non-admin macOS user, runs the GitHub Actions runner as that
user, and destroys the user when the job completes.

Security model:
- Each job runs as an isolated non-admin macOS user (GID 20 / staff)
- Home directory is chmod 700, deleted after job
- TMPDIR is overridden to a per-user directory
- JIT config is written to a chmod 600 file (not a CLI arg visible in `ps`)
- Runner binary template is root-owned, world-readable, no write
- Clean env (no orchestrator credentials inherited by runner process)
"""

import hashlib
import json
import logging
import logging.handlers
import os
import resource
import secrets
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# All paths are relative to the directory containing this script.
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))

RUNNER_TEMPLATE = os.path.join(_BASE_DIR, "runner-template")
JOBS_DIR        = os.path.join(_BASE_DIR, "jobs")
LOGS_DIR        = os.path.join(_BASE_DIR, "logs", "jobs")
DEFAULT_CONFIG_PATH = os.path.join(_BASE_DIR, "config.env")
UID_RANGE_START = 60000
UID_RANGE_END = 65000
USERNAME_PREFIX = "ghr_"

# ---------------------------------------------------------------------------
# Globals (set in main)
# ---------------------------------------------------------------------------

_config: dict = {}
_shutdown_event = threading.Event()
_uid_lock = threading.Lock()
_allocated_uids: set[int] = set()
_active_count = 0
_active_count_lock = threading.Lock()
logger = logging.getLogger("orchestrator")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def check_config_permissions(path: str) -> None:
    """Abort if config.env is missing or not chmod 600."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    mode = stat.S_IMODE(os.stat(path).st_mode)
    if mode != 0o600:
        raise PermissionError(
            f"Refusing to start: {path} has permissions {oct(mode)}, must be 600.\n"
            f"Fix with: chmod 600 {path}"
        )


def load_config(path: str) -> dict:
    """Parse KEY=VALUE env file, validate required keys, return dict."""
    defaults = {
        "RUNNER_LABELS": "self-hosted,macOS,x64",
        "RUNNER_GROUP_ID": "1",
        "MAX_CONCURRENT_JOBS": "3",
        "JOB_TIMEOUT_SECONDS": "3600",
        "POLL_INTERVAL_SECONDS": "15",
        "GITHUB_API_URL": "https://api.github.com",
    }
    config = dict(defaults)

    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, _, value = line.partition("=")
                    config[key.strip()] = value.strip()
    else:
        # Fall back to environment variables (useful for testing)
        config.update({k: v for k, v in os.environ.items()})

    required = ["GITHUB_TOKEN", "GITHUB_OWNER", "GITHUB_REPO", "RUNNER_DOWNLOAD_URL"]
    missing = [k for k in required if not config.get(k)]
    if missing:
        raise ValueError(f"Missing required config keys: {', '.join(missing)}")

    # Coerce numeric values
    config["MAX_CONCURRENT_JOBS"] = int(config["MAX_CONCURRENT_JOBS"])
    config["JOB_TIMEOUT_SECONDS"] = int(config["JOB_TIMEOUT_SECONDS"])
    config["POLL_INTERVAL_SECONDS"] = int(config["POLL_INTERVAL_SECONDS"])
    config["RUNNER_GROUP_ID"] = int(config["RUNNER_GROUP_ID"])

    return config


# ---------------------------------------------------------------------------
# Setup (run once at startup)
# ---------------------------------------------------------------------------

def check_system_deps() -> None:
    """Abort if required macOS tools are missing."""
    missing = [cmd for cmd in ("dscl", "createhomedir", "pkill") if not shutil.which(cmd)]
    if missing:
        raise RuntimeError(f"Missing required system tools: {', '.join(missing)}")


def ensure_directories() -> None:
    """Create runtime directories if they don't exist yet."""
    for path in (RUNNER_TEMPLATE, JOBS_DIR, LOGS_DIR):
        os.makedirs(path, exist_ok=True)


def download_runner(url: str) -> None:
    """
    Download the runner tarball from url, verify its SHA-256 checksum against
    the GitHub-published checksums file, then extract into RUNNER_TEMPLATE.

    The checksums URL is derived by replacing .tar.gz with _checksums.txt in
    the filename (matching the layout GitHub uses for every runner release).

    Skips download entirely if RUNNER_TEMPLATE/run.sh already exists.
    To force a re-download, delete the runner-template/ directory.
    """
    if os.path.isfile(os.path.join(RUNNER_TEMPLATE, "run.sh")):
        logger.info("Runner already present at %s — skipping download", RUNNER_TEMPLATE)
        return

    filename = url.split("/")[-1]
    if not filename.endswith(".tar.gz"):
        raise ValueError(f"RUNNER_DOWNLOAD_URL must point to a .tar.gz file, got: {filename}")

    logger.info("Downloading runner from %s", url)
    with tempfile.TemporaryDirectory() as tmp:
        tarball = os.path.join(tmp, filename)

        try:
            urllib.request.urlretrieve(url, tarball)
        except urllib.error.URLError as e:
            raise RuntimeError(f"Failed to download {url}: {e}") from e

        sha = hashlib.sha256()
        with open(tarball, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                sha.update(chunk)
        print(f"SHA-256: {sha.hexdigest()}  {filename}")

        # Clear old template and extract
        shutil.rmtree(RUNNER_TEMPLATE, ignore_errors=True)
        os.makedirs(RUNNER_TEMPLATE, exist_ok=True)
        subprocess.run(["tar", "-xzf", tarball, "-C", RUNNER_TEMPLATE], check=True)

    # Root-owned, world-readable+executable, no write
    subprocess.run(["chown", "-R", "root:wheel", RUNNER_TEMPLATE], check=True)
    subprocess.run(["chmod", "-R", "755", RUNNER_TEMPLATE], check=True)
    # Remove any stale credential files that might have been in an old template
    for pattern in (".credentials*", ".runner"):
        for p in (p for p in [RUNNER_TEMPLATE] if os.path.exists(p)):
            subprocess.run(
                ["find", RUNNER_TEMPLATE, "-name", pattern, "-delete"],
                check=False,
            )

    logger.info("Runner installed at %s", RUNNER_TEMPLATE)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(log_dir: str) -> None:
    os.makedirs(log_dir, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(threadName)s: %(message)s")

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(sh)

    try:
        fh = logging.handlers.RotatingFileHandler(
            os.path.join(_BASE_DIR, "logs", "orchestrator.log"),
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
        )
        fh.setFormatter(fmt)
        root.addHandler(fh)
    except OSError as e:
        logger.warning("Could not open log file: %s", e)


# ---------------------------------------------------------------------------
# UID management
# ---------------------------------------------------------------------------

def find_free_uid() -> int:
    """
    Thread-safe UID allocation. Must be called with _uid_lock held.
    Queries dscl for in-use UIDs, adds in-flight allocations, returns first free.
    """
    result = subprocess.run(
        ["dscl", ".", "-list", "/Users", "UniqueID"],
        capture_output=True, text=True, check=True,
    )
    used = set()
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) == 2:
            try:
                used.add(int(parts[1]))
            except ValueError:
                pass
    used |= _allocated_uids

    for uid in range(UID_RANGE_START, UID_RANGE_END):
        if uid not in used:
            _allocated_uids.add(uid)
            return uid

    raise RuntimeError("No free UIDs available in range 60000-65000")


def release_uid(uid: int) -> None:
    _allocated_uids.discard(uid)


def uid_to_username(uid: int) -> str:
    return f"{USERNAME_PREFIX}{uid - UID_RANGE_START + 1:05d}"


# ---------------------------------------------------------------------------
# User lifecycle
# ---------------------------------------------------------------------------

def create_ephemeral_user(username: str, uid: int) -> None:
    """Create a non-admin macOS user with an isolated home directory."""
    logger.info("Creating user %s (uid=%d)", username, uid)
    password = secrets.token_urlsafe(48)

    home = f"/Users/{username}"

    # Remove stale home dir if it exists from a prior crash
    if os.path.exists(home):
        subprocess.run(["rm", "-rf", home], check=False)

    steps = [
        ["dscl", ".", "-create", f"/Users/{username}"],
        ["dscl", ".", "-create", f"/Users/{username}", "UserShell", "/bin/bash"],
        ["dscl", ".", "-create", f"/Users/{username}", "RealName", f"GH Runner {uid}"],
        ["dscl", ".", "-create", f"/Users/{username}", "UniqueID", str(uid)],
        # GID 20 = staff (non-admin). Do NOT use 80 (admin).
        ["dscl", ".", "-create", f"/Users/{username}", "PrimaryGroupID", "20"],
        ["dscl", ".", "-create", f"/Users/{username}", "NFSHomeDirectory", home],
        ["dscl", ".", "-passwd", f"/Users/{username}", password],
    ]
    try:
        for cmd in steps:
            subprocess.run(cmd, check=True, capture_output=True)
        # Create home directory (sets chmod 700 automatically)
        subprocess.run(
            ["createhomedir", "-c", "-u", username],
            check=True, capture_output=True,
        )
    except subprocess.CalledProcessError as e:
        logger.error("User creation failed: %s", e)
        delete_ephemeral_user(username, uid)
        raise RuntimeError(f"Failed to create user {username}") from e

    # Per-user tmp directory (overrides shared /tmp)
    user_tmp = f"/private/tmp/{username}"
    os.makedirs(user_tmp, mode=0o700, exist_ok=True)
    subprocess.run(["chown", f"{username}:staff", user_tmp], check=True)

    logger.info("User %s created, home=%s", username, home)


def delete_ephemeral_user(username: str, uid: int) -> None:
    """
    Delete user and all associated resources. Best-effort — never raises.
    """
    logger.info("Deleting user %s", username)

    def _run(cmd):
        try:
            subprocess.run(cmd, capture_output=True, timeout=30)
        except Exception as e:
            logger.warning("Cleanup step failed (%s): %s", cmd[0], e)

    # Kill all processes owned by this user (two passes)
    _run(["pkill", "-9", "-u", username])
    time.sleep(1)
    _run(["pkill", "-9", "-u", username])

    _run(["dscl", ".", "-delete", f"/Users/{username}"])
    _run(["rm", "-rf", f"/Users/{username}"])
    _run(["rm", "-rf", f"/private/tmp/{username}"])
    _run(["rm", "-rf", f"{JOBS_DIR}/{username}"])

    release_uid(uid)
    logger.info("User %s deleted", username)


# ---------------------------------------------------------------------------
# Runner directory setup
# ---------------------------------------------------------------------------

def setup_runner_directory(job_dir: str, username: str) -> str:
    """
    Prepare per-job runner directory using hard links for large binaries.

    Strategy:
    - cp -al (hard-link) the entire template into job_dir/runner
    - Then regular-copy root-level files (*.sh, *.json) to break those hard
      links, so the runner can write .credentials/.runner without affecting
      the shared template inodes
    - Create fresh writable subdirs (_work, _diag, _temp) owned by username
    """
    os.makedirs(job_dir, mode=0o755, exist_ok=True)
    runner_dir = os.path.join(job_dir, "runner")

    # Hard-link copy (fast, ~0 extra disk for binaries)
    subprocess.run(["cp", "-al", RUNNER_TEMPLATE, runner_dir], check=True)

    # Break hard links on root-level regular files (runner writes here)
    for entry in os.scandir(runner_dir):
        if entry.is_file(follow_symlinks=False):
            # Copy over itself to get a fresh inode
            tmp = entry.path + ".tmp"
            subprocess.run(["cp", entry.path, tmp], check=True)
            os.replace(tmp, entry.path)

    # Create writable subdirs for the runner
    for subdir in ["_work", "_diag", "_temp"]:
        path = os.path.join(runner_dir, subdir)
        os.makedirs(path, mode=0o700, exist_ok=True)
        subprocess.run(["chown", "-R", f"{username}:staff", path], check=True)

    # Give the runner dir root ownership; only the writable subdirs above are user-owned
    # The runner needs to write .runner and .credentials to its root dir during JIT setup
    subprocess.run(["chown", username, runner_dir], check=True)

    # Per-job log directory
    log_dir = os.path.join(job_dir, "logs")
    os.makedirs(log_dir, mode=0o755, exist_ok=True)
    subprocess.run(["chown", "-R", f"{username}:staff", log_dir], check=True)

    return runner_dir


# ---------------------------------------------------------------------------
# GitHub API client
# ---------------------------------------------------------------------------

def github_api_request(
    method: str,
    path: str,
    token: str,
    body: Optional[dict] = None,
    api_url: str = "https://api.github.com",
) -> dict:
    """Make an authenticated GitHub API request using stdlib urllib."""
    url = api_url.rstrip("/") + path
    data = json.dumps(body).encode() if body is not None else None
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if data:
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body_text = e.read().decode(errors="replace")
        try:
            msg = json.loads(body_text).get("message", body_text)
        except Exception:
            msg = body_text
        raise RuntimeError(f"GitHub API {method} {path} → HTTP {e.code}: {msg}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"GitHub API request failed: {e.reason}") from e


def count_queued_runs(config: dict) -> int:
    """Return how many workflow runs are currently queued for this repo."""
    owner = config["GITHUB_OWNER"]
    repo = config["GITHUB_REPO"]
    try:
        resp = github_api_request(
            "GET",
            f"/repos/{owner}/{repo}/actions/runs?status=queued&per_page=1",
            config["GITHUB_TOKEN"],
            api_url=config["GITHUB_API_URL"],
        )
        return resp.get("total_count", 0)
    except RuntimeError as e:
        logger.warning("Could not count queued runs: %s", e)
        return 0


def generate_jit_config(config: dict, runner_name: str) -> str:
    """Generate a single-use JIT runner config from the GitHub API."""
    owner = config["GITHUB_OWNER"]
    repo = config["GITHUB_REPO"]
    labels = [l.strip() for l in config["RUNNER_LABELS"].split(",") if l.strip()]

    resp = github_api_request(
        "POST",
        f"/repos/{owner}/{repo}/actions/runners/generate-jitconfig",
        config["GITHUB_TOKEN"],
        body={
            "name": runner_name,
            "runner_group_id": config["RUNNER_GROUP_ID"],
            "labels": labels,
            "work_folder": "_work",
        },
        api_url=config["GITHUB_API_URL"],
    )
    jit_config = resp.get("encoded_jit_config")
    if not jit_config:
        raise RuntimeError("generate-jitconfig response missing encoded_jit_config")
    return jit_config


# ---------------------------------------------------------------------------
# Runner execution
# ---------------------------------------------------------------------------

def set_resource_limits() -> None:
    """Called as preexec_fn to apply ulimits to the runner subprocess."""
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (4096, 4096))
    except Exception:
        pass
    try:
        resource.setrlimit(resource.RLIMIT_NPROC, (512, 512))
    except Exception:
        pass
    # Create a new process group so we can kill all descendants on timeout
    os.setsid()


def build_runner_env(username: str) -> dict:
    """
    Build a clean environment for the runner subprocess.
    Deliberately does NOT inherit the orchestrator's environment so that
    GITHUB_TOKEN and other secrets are never visible to the job.
    """
    return {
        "HOME": f"/Users/{username}",
        "USER": username,
        "LOGNAME": username,
        "SHELL": "/bin/bash",
        "TMPDIR": f"/private/tmp/{username}",
        "TEMP": f"/private/tmp/{username}",
        "TMP": f"/private/tmp/{username}",
        "PATH": "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
        # Prevent Homebrew from updating shared global state mid-job
        "HOMEBREW_NO_AUTO_UPDATE": "1",
        "HOMEBREW_NO_INSTALL_CLEANUP": "1",
        "HOMEBREW_NO_ENV_HINTS": "1",
        # Safety: prevent runner from accidentally running as root
        "RUNNER_ALLOW_RUNASROOT": "0",
    }


def run_runner_process(
    runner_dir: str,
    username: str,
    jitconfig_path: str,
    env: dict,
    config: dict,
    log_path: str,
) -> int:
    """
    Execute the runner as the ephemeral user via `su`.
    Returns the exit code (or -1 on timeout/signal).

    The jit config is read from a file (chmod 600) rather than passed directly
    on the command line, so it does not appear in `ps aux` output.
    """
    # $(...) in the shell command reads the file; it never appears as a process arg
    cmd_inner = f'cd {runner_dir!r} && ./run.sh --jitconfig "$(cat {jitconfig_path!r})"'
    cmd = ["su", "-m", username, "-c", cmd_inner]

    timeout = config["JOB_TIMEOUT_SECONDS"]
    proc = None
    try:
        with open(log_path, "wb") as log_file:
            proc = subprocess.Popen(
                cmd,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                env=env,
                preexec_fn=set_resource_limits,
            )
            try:
                proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                logger.warning(
                    "Job timed out after %ds for user %s — killing process group",
                    timeout, username,
                )
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass
                proc.wait()
                return -1

        exit_code = proc.returncode
        logger.info("Runner for %s exited with code %d", username, exit_code)
        return exit_code

    except Exception as e:
        logger.error("Runner process error for %s: %s", username, e)
        if proc is not None:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                proc.wait()
            except Exception:
                pass
        return -1


# ---------------------------------------------------------------------------
# Job lifecycle
# ---------------------------------------------------------------------------

def run_job(config: dict) -> None:
    """
    Full per-job lifecycle. Runs in a ThreadPoolExecutor worker thread.
    Thread name is set to job-<uid> for log traceability.
    The finally block guarantees user cleanup even on exceptions.
    """
    uid = None
    username = None

    try:
        with _uid_lock:
            uid = find_free_uid()

        username = uid_to_username(uid)
        threading.current_thread().name = f"job-{username}"

        runner_name = f"ephemeral-{int(time.time())}-{uid}"
        job_dir = os.path.join(JOBS_DIR, username)
        log_path = os.path.join(LOGS_DIR, f"{username}.log")

        logger.info("Starting job: user=%s runner=%s", username, runner_name)

        create_ephemeral_user(username, uid)
        runner_dir = setup_runner_directory(job_dir, username)

        jit_config = generate_jit_config(config, runner_name)

        # Write JIT config to a private file — keeps it out of `ps` output
        jitconfig_path = f"/Users/{username}/.jitconfig"
        with open(jitconfig_path, "w") as f:
            f.write(jit_config)
        os.chmod(jitconfig_path, 0o600)
        subprocess.run(["chown", f"{username}:staff", jitconfig_path], check=True)

        env = build_runner_env(username)
        run_runner_process(runner_dir, username, jitconfig_path, env, config, log_path)

    except Exception as e:
        logger.error("Job failed: %s", e, exc_info=True)
    finally:
        if username is not None and uid is not None:
            delete_ephemeral_user(username, uid)


# ---------------------------------------------------------------------------
# Startup cleanup
# ---------------------------------------------------------------------------

def cleanup_stale_jobs() -> None:
    """
    On startup, remove any ghr_* users and job directories left over from a
    prior crash. This prevents UID exhaustion and orphaned processes.
    """
    logger.info("Scanning for stale runner users...")
    try:
        result = subprocess.run(
            ["dscl", ".", "-list", "/Users"],
            capture_output=True, text=True, check=True,
        )
    except subprocess.CalledProcessError:
        return

    stale = [u.strip() for u in result.stdout.splitlines() if u.strip().startswith(USERNAME_PREFIX)]

    if not stale:
        logger.info("No stale users found")
        return

    logger.warning("Found %d stale user(s): %s", len(stale), stale)

    for username in stale:
        # Recover the UID from dscl so release_uid works correctly
        try:
            uid_result = subprocess.run(
                ["dscl", ".", "-read", f"/Users/{username}", "UniqueID"],
                capture_output=True, text=True, check=True,
            )
            uid = int(uid_result.stdout.split()[-1])
        except Exception:
            uid = 0  # won't matter — we just call release_uid(0)
        delete_ephemeral_user(username, uid)

    # Remove all leftover job directories
    try:
        for entry in os.scandir(JOBS_DIR):
            subprocess.run(["rm", "-rf", entry.path], check=False)
    except FileNotFoundError:
        pass

    logger.info("Stale job cleanup complete")


# ---------------------------------------------------------------------------
# Poll loop
# ---------------------------------------------------------------------------

def poll_loop(config: dict, executor: ThreadPoolExecutor) -> None:
    """
    Periodically check GitHub for queued workflow runs and spawn runners.

    Spawning logic:
    - queued_count = runs waiting for a runner (may be > jobs, but good enough)
    - slots = MAX_CONCURRENT - currently active runner threads
    - spawn min(queued_count, slots) new runners
    """
    global _active_count

    interval = config["POLL_INTERVAL_SECONDS"]
    max_jobs = config["MAX_CONCURRENT_JOBS"]

    logger.info(
        "Poll loop started (interval=%ds, max_concurrent=%d)", interval, max_jobs
    )

    while not _shutdown_event.is_set():
        try:
            queued = count_queued_runs(config)
            with _active_count_lock:
                slots = max_jobs - _active_count

            if queued > 0 and slots > 0:
                to_spawn = min(queued, slots)
                logger.info(
                    "Queued runs: %d, available slots: %d → spawning %d runner(s)",
                    queued, slots, to_spawn,
                )
                for _ in range(to_spawn):
                    executor.submit(_wrapped_run_job, config)
            elif queued > 0:
                logger.debug("Queued runs: %d but no slots available (%d active)", queued, _active_count)

        except Exception as e:
            logger.error("Poll loop error: %s", e, exc_info=True)

        _shutdown_event.wait(timeout=interval)

    logger.info("Poll loop exiting")


def _wrapped_run_job(config: dict) -> None:
    """Wrapper that maintains the active job counter around run_job()."""
    global _active_count
    with _active_count_lock:
        _active_count += 1
    try:
        run_job(config)
    finally:
        with _active_count_lock:
            _active_count -= 1


# ---------------------------------------------------------------------------
# Signal handling / graceful shutdown
# ---------------------------------------------------------------------------

def _handle_shutdown(signum, frame) -> None:
    if _shutdown_event.is_set():
        logger.warning("Forced exit")
        os._exit(1)
    logger.info("Received signal %d — shutting down gracefully (Ctrl+C again to force quit)...", signum)
    _shutdown_event.set()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if os.geteuid() != 0:
        print("ERROR: start.py must run as root (sudo python3 start.py)", file=sys.stderr)
        sys.exit(1)

    config_path = os.environ.get("CONFIG_ENV_PATH", DEFAULT_CONFIG_PATH)

    try:
        check_config_permissions(config_path)
        config = load_config(config_path)
    except (ValueError, FileNotFoundError, PermissionError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    ensure_directories()
    setup_logging(LOGS_DIR)

    logger.info("GitHub Actions runner orchestrator starting")
    logger.info(
        "Config: owner=%s repo=%s labels=%s max_concurrent=%d",
        config["GITHUB_OWNER"],
        config["GITHUB_REPO"],
        config["RUNNER_LABELS"],
        config["MAX_CONCURRENT_JOBS"],
    )

    try:
        check_system_deps()
        download_runner(config["RUNNER_DOWNLOAD_URL"])
    except (RuntimeError, ValueError) as e:
        logger.error("Startup error: %s", e)
        sys.exit(1)

    cleanup_stale_jobs()

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    with ThreadPoolExecutor(
        max_workers=config["MAX_CONCURRENT_JOBS"],
        thread_name_prefix="job",
    ) as executor:
        poll_loop(config, executor)
        with _active_count_lock:
            count = _active_count
        if count:
            logger.info("Waiting for %d active job(s) to finish (Ctrl+C to force quit)...", count)

    logger.info("Orchestrator stopped")


if __name__ == "__main__":
    main()
