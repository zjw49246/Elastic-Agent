# AWS 800-Worker production gate

The production branch may be configured for 800 one-Worker Jobs, but the code
limit is not evidence that the AWS account or the upstream model provider can
serve that fleet. Do not start the full queue until every gate below is proven.

## Required production settings

```text
ELASTIC_AGENT_AWS_MAX_INSTANCES=800
ELASTIC_AGENT_JOB_BATCH_MAX_ACTIVE_JOBS=800
ELASTIC_AGENT_WORKER_BRINGUP_CONCURRENCY=32
ELASTIC_AGENT_PERIODIC_COLLECT_CONCURRENCY=32
ELASTIC_AGENT_PERIODIC_COLLECT_JITTER_RATIO=0.25
ELASTIC_AGENT_RESULTS_S3_PERIODIC_ENABLED=false
```

The per-manifest policy remains capped at 10 active Jobs. At least 80 queued
manifests are therefore required to fill 800 one-Worker slots. Increasing
`ELASTIC_AGENT_MAX_JOB_BATCH_ITEMS` does not increase fleet concurrency.

## External capacity gates

For 800 `t3.large` Workers in `ap-northeast-1`, prove all of the following:

- `Running On-Demand Standard vCPUs` quota covers existing non-EA usage plus
  1,600 vCPUs. At the 2026-08-17 observation point, 59 running instances used
  about 640 vCPUs, so the applied quota needs to be at least 2,240 vCPUs;
  2,400 or more is the recommended operating floor.
- available gp3 quota covers an additional 64,000 GiB for 800 80-GiB roots;
- the selected subnet has at least 800 free addresses plus operating headroom;
- the selected AZ can supply 800 `t3.large` On-Demand instances, or a Capacity
  Reservation / multi-AZ plan exists;
- the Agent API provider confirms capacity near 800 concurrent Claude calls.

The Manager role must have read-only permission to verify the applied Service
Quotas and EBS usage. A denied quota read is an unknown result, not approval.

## Deployment and rollback

`systemctl restart` is not a zero-downtime operation: graceful Manager shutdown
cancels every active Job, performs bounded final collection, and terminates its
instances. Freeze scheduling first, record the exact active Job set, stop the
Manager, and create new-idempotency-key retry manifests for every interrupted
logical task before starting the new release.

The production settings deliberately disable only the Manager's redundant
whole-tree S3 background scan. Worker-direct periodic collections and awaited
per-Job/final S3 uploads remain enabled. Roll back before admitting new Jobs if
startup recovery is not clean, the applied limits differ from the expected
values, or infrastructure/provider failures appear during the bounded ramp.

EC2 launches are process-wide rate-limited to 2 requests per second with a
burst of 5. Explicit AWS throttling is retried with bounded full jitter and a
stable `ClientToken`; quota and AZ-capacity errors fail closed for operator
action rather than being replayed blindly.
