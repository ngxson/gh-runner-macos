# gh-runner-macos

A GitHub Actions self-hosted runner for macOS that creates a fresh non-admin OS user for each job and destroys it when the job finishes. Jobs are isolated from each other via separate home directories, keychains, and temp directories.

## How it works

A Python orchestrator runs as root. It polls the GitHub API for queued workflow runs, and for each job it:

1. Creates a temporary macOS user (`ghr_NNNNN`, non-admin, no sudo)
2. Hard-links the runner binary into that user's job directory
3. Fetches a single-use JIT registration token from GitHub
4. Runs the GitHub Actions runner as that user
5. Deletes the user and all their files when the job exits

## Requirements

- macOS 13+ (Ventura or later)
- Admin account (to run with sudo)
- Python 3.9+
- A GitHub fine-grained Personal Access Token with two repository permissions:
  - Actions: Read-only (poll for queued runs)
  - Self-hosted runners: Read and write (generate JIT tokens)

Note: classic PATs cannot express these permissions this narrowly. Use a fine-grained token.

For org repos, self-hosted runners may appear under Organization permissions rather than Repository permissions. The org must also allow fine-grained PATs under Settings > Actions > Runner groups.

## Setup

```
git clone <this repo>
cd gh-runner-macos
cp config.env.example config.env
chmod 600 config.env
nano config.env
```

Fill in at minimum:

```
GITHUB_TOKEN=github_pat_...
GITHUB_OWNER=your-username-or-org
GITHUB_REPO=your-repository
RUNNER_DOWNLOAD_URL=https://github.com/actions/runner/releases/download/vX.Y.Z/actions-runner-osx-arm64-X.Y.Z.tar.gz
```

Get the download URL from https://github.com/actions/runner/releases. Use `arm64` for Apple Silicon, `x64` for Intel. See `config.env.example` for all options.

## Running

From your admin account:

```
sudo python3 start.py
```

On first run, the runner binary is downloaded from `RUNNER_DOWNLOAD_URL`, its SHA-256 is printed to stdout, and it is extracted into `runner-template/`. Subsequent starts skip the download.

Press Ctrl-C to stop. The orchestrator waits for active jobs to finish. Press Ctrl-C again to force quit immediately.

Logs are written to `logs/orchestrator.log`. Per-job logs go to `logs/jobs/`.

## Upgrading the runner binary

Update `RUNNER_DOWNLOAD_URL` in `config.env`, delete `runner-template/`, then restart.

## Security properties

- Each job runs as a separate non-admin macOS user with an isolated home directory (`chmod 700`)
- `TMPDIR` is overridden per user so jobs cannot read each other's temp files
- The JIT registration token is written to a `chmod 600` file rather than passed as a CLI argument, so it does not appear in `ps` output
- The runner subprocess receives a clean environment — no credentials from the orchestrator are inherited
- Each user has their own keychain, so stored credentials are not shared between jobs
- User home directories (including `~/Library/LaunchAgents`) are deleted after each job, preventing persistent backdoors
- `config.env` must be `chmod 600` or the orchestrator refuses to start
- Stale users from a prior crash are cleaned up automatically on startup

## Known limitations

- Homebrew is shared across jobs. A job can install packages that persist after it finishes.
- macOS has no `hidepid` equivalent, so all users can see each other's process names via `ps aux`. Secrets are kept out of CLI arguments for this reason.
- There is no network isolation between jobs.
