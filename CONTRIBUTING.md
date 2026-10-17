# Contributing to Runspoke

Use an unprivileged Linux account with stable Rust, a C compiler, GNU coreutils and Python 3. SQLite is bundled. Start with the [architecture](docs/ARCHITECTURE.md), [recovery model](docs/FAILURE-MODEL.md), [security boundary](docs/SECURITY.md) and [scheduling](docs/SCHEDULING.md).

Run the repository checks with:

```bash
scripts/check.sh
```

Keep scheduling policies deterministic and durable state transitions in the store. Never release a reservation or retry work that may still be running without proof of termination or valid fencing. Treat connection profiles, invitations, logs, snapshots and database backups as sensitive user data.

Bug reports are most useful with `runspk --version`, Linux distribution and architecture, a redacted `runspk doctor --json` report, job and attempt states, and a minimal reproducer. Remove tokens, private paths and workload data before sharing logs or files.
