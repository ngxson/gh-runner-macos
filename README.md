# gh-runner-macos

A GitHub Actions self-hosted runner for macOS that creates a fresh non-admin OS user for each job and destroys it when the job finishes. Jobs are isolated from each other via separate home directories, keychains, and temp directories.

## How it works

A Python orchestrator runs as root. It polls the GitHub API for queued workflow runs, and for each job it:

1. Creates a temporary macOS user (`ghr_NNNNN`, non-admin, no sudo)
2. Hard-links the runner binary into that user's home directory
3. Fetches a single-use JIT registration token from GitHub
4. Runs the GitHub Actions runner as that user
5. Deletes the user and all their files when the job exits

## Requirements

- macOS 13+ (Ventura or later)
- Admin account (to run with sudo)
- Python 3.9+ (`brew install python3` if the system stub is not enough)
- Xcode Command Line Tools (`xcode-select --install`)
- A GitHub fine-grained Personal Access Token with two repository permissions:
  - Actions: Read-only (poll for queued runs)
  - Self-hosted runners: Read and write (generate JIT tokens)
  - Classic PATs cannot express these permissions this narrowly, so use a fine-grained token

## Setup

Clone the repo, then run setup once as root. All files stay inside the repo directory.

```
git clone https://github.com/(this_repo)
cd gh-runner-macos
sudo bash setup.sh
```

This creates `runner-template/`, `jobs/`, `logs/`, and `config.env` inside the repo directory.

Edit the config:

```
sudo nano config.env
```

Minimum required values:

```
GITHUB_TOKEN=github_pat_...
GITHUB_OWNER=your-username
GITHUB_REPO=your-repository
```

See `config.env.example` for all options. `config.env` is automatically added to `.gitignore` by setup.

## Starting the runner

Run this from your admin account on each boot (or after login):

```
sudo python3 orchestrator.py
```

Press Ctrl-C or send SIGTERM to stop. The orchestrator will wait for any active jobs to finish before exiting.

Logs are written to `logs/orchestrator.log` and per-job logs go to `logs/jobs/`.

## Updating

To update the runner binary version, change `RUNNER_VERSION` at the top of `setup.sh` and re-run it. It will skip the download if the version is already current.

## Security properties

- Each job runs as a separate non-admin macOS user with an isolated home directory (`chmod 700`)
- `TMPDIR` is overridden per user so jobs cannot read each other's temp files
- The JIT registration token is written to a `chmod 600` file rather than passed as a CLI argument, so it does not appear in `ps` output
- The runner subprocess receives a clean environment — no credentials from the orchestrator are inherited
- Each user has their own keychain (macOS default), so code signing certificates and stored credentials are not shared between jobs
- User home directories (including `~/Library/LaunchAgents`) are deleted after each job, preventing persistent backdoors
- Stale users from a prior crash are cleaned up automatically on startup

## Known limitations

- Homebrew is shared across jobs. A job can install packages that persist after it finishes. Set `HOMEBREW_NO_AUTO_UPDATE=1` (already set by the orchestrator) to at least prevent auto-updates during jobs.
- macOS has no `hidepid` equivalent, so all users can see each other's process names via `ps aux`. The orchestrator avoids passing secrets as arguments for this reason.
- There is no network isolation between jobs.
