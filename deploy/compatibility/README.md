# AMI-023 compatibility binding (read-only evidence)

This directory contains the authoritative external WorkerProfileInput and
read-only EC2 evidence used to generate the PR#8-compatible release when Task
Platform Manager is configured to launch `ami-023251121ceb0d6f3`. It does not
change IAM, Terraform, or any production resource.

The network and IAM identity fields are bound to the Manager's authenticated
systemd environment observed over SSM: dedicated instance profile/role
`task-platform-pilot-executor-worker`, subnet `subnet-000a8edefd5306091`, and
security group `sg-0a72ebfc1a59587c5`. The earlier build-only profile and
`subnet-0c1db80817d054277` are not interchangeable with this Manager runtime.

The compatibility claim is deliberately narrow: EC2 `DescribeImages` showed
that AMI-023 is a KMS-refreshed copy whose `SourceImageId` is AMI-03c4 and whose
`ManifestDigest`, `ConstraintsDigest`, `RunnerImage`, `PlatformRevision`,
`UpstreamRevision`, architecture, IMDS mode, and instance type contract match
the PR#8 release provenance. This permits a profile/manifest binding update,
not an assumption that an arbitrary old AMI is equivalent.

`scripts/generate_release_evidence.py` binds these values into the canonical
`deploy/release-manifest.json`; the profile and provenance remain separate
digest domains. Before deployment, the Manager release installer must still
perform its normal fail-closed health and worker preflight checks. No Campaign
may be created until the active binding set and online template/profile IDs are
issued by the platform.
