# Golden worker AMI

Elastic-Agent can use one encrypted, credential-free worker image to avoid
reinstalling common system, browser, agent, Docker, and Python dependencies for
every ephemeral EC2. The current provider has one global `ami_id`, so the image
is a union of `ubuntu-agent-v1` and `ubuntu-agent-docker-v1`; Docker is installed
but disabled until a Docker Job enables it.

## Build

Pin a Canonical Ubuntu x86_64 AMI rather than resolving `latest` during a build.
The builder must be tagged as an Elastic-Agent image builder, use an encrypted
root volume, require IMDSv2, and be reachable with the supplied SSH key.

```bash
scripts/build_golden_ami.sh \
  --builder-id i-0123456789abcdef0 \
  --base-ami ami-0123456789abcdef0 \
  --region ap-northeast-1 \
  --ssh-key /secure/path/worker-key.pem \
  --source-commit "$(git rev-parse HEAD)"
```

For a new builder, omit `--builder-id` and also pass `--subnet`,
`--security-group`, and `--key-name`. `--associate-public-ip` gives the builder
a public egress address while SSH still uses its private VPC address; only the
separate `--use-public-ssh` override moves control traffic to the public address.
Run `--help` for all options. The script:

1. validates the pinned source and builder ownership before any cleanup;
2. installs version-pinned dependencies and reboots once;
3. validates every command, import, CLI version, and claude-pty commit;
4. removes account/browser/cloud credentials, runtime tokens, logs, host keys,
   machine identity, and caches;
5. stops the builder, creates an AMI from its encrypted volume, tags both the
   AMI and snapshot, verifies the launch invariants, and terminates the builder.

The last stdout line is JSON containing the AMI and snapshot IDs. Build progress
goes to stderr. The script never copies an AWS key, API key, OAuth credential,
account file, Manager URL, or worker token into the image.

## Runtime contract

The build writes `/etc/elastic-agent/image-manifest.json` (schema 1) and installs
`/usr/local/bin/elastic-agent-image-verify`. Bootstrap takes a fast path only
when the requested component is present in the manifest and the live machine
still exactly matches it. Checks include dpkg versions and commands, agent CLI
versions, Python distribution versions and imports, Chrome, Docker/buildx, and
the VCS commit in claude-pty's `direct_url.json`. A missing verifier, corrupt or
stale manifest, package drift, failed command/import, or unpinned PTY URL runs
the complete existing apt/npm/pip fallback.

Do not bake account state, `auth.json`, browser profiles, Job code/data, the
Manager URL/token, or a running Elastic-Agent service. Current framework source
and the per-worker runtime unit are still delivered at provision time.

## Promotion and rollback

Before promotion, verify that the AMI is owned by this account, available,
x86_64/HVM/ENA, IMDSv2-only, has an encrypted EBS snapshot, and carries
`ManagedBy=elastic-agent` plus `Role=worker-golden`. Then run canaries for the
standard profile, Docker profile, S3 collection, Claude login, Codex login, and
one EIP-bound Job. Confirm each EC2/root EBS is destroyed and its EIP retained.

Select a fixed AMI ID through the Manager deployment configuration; never use a
moving alias. Rollback selects the previous fixed ID and restarts the Manager;
already-running workers are unaffected. Retain at least two known-good image
versions for 7–14 days before deregistering an old AMI and deleting its snapshot.

Rebuild whenever the base OS security image, pinned Claude/Codex/claude-pty,
Chrome, Docker, Node, or Python dependency set changes. The manifest and image
tags retain the source AMI, source commit, build timestamp, and build-tree hash.
