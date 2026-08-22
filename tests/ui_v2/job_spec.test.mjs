// Unit tests for the pure JobSpec builder/validator.
// Run: node --test tests/ui_v2/

import { test } from 'node:test';
import assert from 'node:assert/strict';

import {
  JOB_FORM_DEFAULTS, buildJobSpec, validateJobForm, deriveFormState,
  parseKeyValueLines, parseS3DatasetLine, parseSetupSteps, createSubmissionIntent,
} from '../../src/elastic_agent/api/ui_v2/js/core/job-spec.js';

function values(overrides = {}) {
  return { ...JOB_FORM_DEFAULTS, jRun: 'echo hi', ...overrides };
}

test('default form with a command validates and builds a full spec', () => {
  const v = values();
  assert.deepEqual(validateJobForm(v), []);
  const spec = buildJobSpec(v);
  assert.equal(spec.name, 'job');
  assert.equal(spec.environment.profile, 'ubuntu-agent-v1');
  assert.equal(spec.run.command, 'echo hi');
  assert.equal(spec.run.timeout, 86400);
  assert.equal(spec.ttl_seconds, 172800);
  assert.equal(spec.account.mode, 'worker_local_login');
  assert.equal(spec.account.binding, 'none');
  assert.equal(spec.rotation.strategy, 'none');
  assert.equal(spec.fanout.workers, 1);
  assert.deepEqual(spec.collect.paths, ['results']);
  assert.equal(spec.recovery.policy, 'none');
  assert.ok(!('harness_ref' in spec));
});

test('missing run command is rejected', () => {
  const errors = validateJobForm(values({ jRun: '   ' }));
  assert.ok(errors.some((e) => e.field === 'jRun'));
});

test('ttl shorter than run timeout is rejected', () => {
  const errors = validateJobForm(values({ jRunTimeout: '7200', jTtl: '3600' }));
  assert.ok(errors.some((e) => e.field === 'jTtl'));
});

test('ref and resolved_commit only included when repo present', () => {
  const without = buildJobSpec(values());
  assert.ok(!('ref' in without.setup));
  const withRepo = buildJobSpec(values({
    jRepo: 'https://github.com/org/repo.git',
    jRepoRef: 'main',
    jResolvedCommit: 'a'.repeat(40),
  }));
  assert.equal(withRepo.setup.ref, 'main');
  assert.equal(withRepo.setup.resolved_commit, 'a'.repeat(40));
});

test('account mode none strips account/rotation extras', () => {
  const spec = buildJobSpec(values({
    jAcctMode: 'none', jAgentModel: 'x', jAcctGroup: 'g',
    jRot: 'on_exhaust_restart_resume', jResume: '--resume',
  }));
  assert.equal(spec.account.mode, 'none');
  assert.equal(spec.account.model, '');
  assert.equal(spec.account.group, 'standard');
  assert.equal(spec.account.binding, 'none');
  assert.equal(spec.rotation.strategy, 'none');
  assert.equal(spec.rotation.resume_args, '');
});

test('EIP binding forces per_worker 1 and forbids rotation', () => {
  const errors = validateJobForm(values({
    jAcctBinding: 'eip', jPerWorker: '2', jRot: 'on_exhaust_restart_resume',
  }));
  assert.ok(errors.some((e) => e.field === 'jPerWorker'));
  assert.ok(errors.some((e) => e.field === 'jRot'));
});

test('EIP account count must equal workers', () => {
  const errors = validateJobForm(values({
    jAcctBinding: 'eip', jWorkers: '3', jAcctIds: ['a', 'b'],
  }));
  assert.ok(errors.some((e) => e.field === 'jAcctIds'));
  const ok = validateJobForm(values({
    jAcctBinding: 'eip', jWorkers: '2', jAcctIds: ['a', 'b'],
  }));
  assert.ok(!ok.some((e) => e.field === 'jAcctIds'));
});

test('codex multi-account requires explicit config_dir', () => {
  const errors = validateJobForm(values({ jAgentType: 'codex', jPerWorker: '2' }));
  assert.ok(errors.some((e) => e.field === 'jConfigDir'));
  const ok = validateJobForm(values({
    jAgentType: 'codex', jPerWorker: '2', jConfigDir: '/home/user/.codex',
  }));
  assert.ok(!ok.some((e) => e.field === 'jConfigDir'));
});

test('checkpoint requires collect paths and shard_index', () => {
  const errors = validateJobForm(values({
    jCollectCheckpoint: 'true', jCollect: '', jShard: 'hostname',
  }));
  assert.ok(errors.some((e) => e.field === 'jCollect'));
  assert.ok(errors.some((e) => e.field === 'jShard'));
});

test('recovery requires source job and paths', () => {
  const errors = validateJobForm(values({ jRecoveryPolicy: 'checkpoint' }));
  assert.ok(errors.some((e) => e.field === 'jRecoveryJob'));
  assert.ok(errors.some((e) => e.field === 'jRecoveryPaths'));
});

test('legacy recovery cannot pin a generation', () => {
  const errors = validateJobForm(values({
    jRecoveryPolicy: 'legacy_final_collection',
    jRecoveryJob: 'job-1', jRecoveryPaths: 'results',
    jRecoveryGeneration: 'gen-9',
  }));
  assert.ok(errors.some((e) => e.field === 'jRecoveryGeneration'));
});

test('secret env values must be aws references', () => {
  const errors = validateJobForm(values({ jSecretEnv: 'KEY=plaintext' }));
  assert.ok(errors.some((e) => e.field === 'jSecretEnv'));
  const ok = validateJobForm(values({
    jSecretEnv: 'KEY=aws-secretsmanager://prod/openai#api_key',
  }));
  assert.ok(!ok.some((e) => e.field === 'jSecretEnv'));
});

test('env parser enforces KEY=VALUE and name charset', () => {
  assert.deepEqual(parseKeyValueLines('A=1\nB_x=two=2', 'env'), { A: '1', B_x: 'two=2' });
  assert.throws(() => parseKeyValueLines('1BAD=x', 'env'));
  assert.throws(() => parseKeyValueLines('NOVALUE', 'env'));
});

test('s3 dataset line parses templates with inner whitespace', () => {
  const parsed = parseS3DatasetLine('s3://b/shard-{{ shard_id }} /data/in');
  assert.equal(parsed.dest, '/data/in');
  assert.throws(() => parseS3DatasetLine('not-s3 /x'));
  assert.throws(() => parseS3DatasetLine('s3://b/only-uri'));
});

test('setup steps must be a JSON array', () => {
  assert.deepEqual(parseSetupSteps(''), []);
  assert.deepEqual(parseSetupSteps('[{"name":"a","command":"b"}]'), [{ name: 'a', command: 'b' }]);
  assert.throws(() => parseSetupSteps('{"name":"a"}'));
  assert.throws(() => parseSetupSteps('not json'));
});

test('idempotency key stable for identical spec, new for edits', () => {
  const intent = createSubmissionIntent();
  const specA = buildJobSpec(values());
  const key1 = intent.keyFor(specA);
  const key2 = intent.keyFor(buildJobSpec(values()));
  assert.equal(key1, key2);
  const key3 = intent.keyFor(buildJobSpec(values({ jName: 'other' })));
  assert.notEqual(key1, key3);
});

test('deriveFormState reflects eip/rotation/codex flags', () => {
  const st = deriveFormState(values({ jAcctBinding: 'eip', jAgentType: 'codex' }));
  assert.equal(st.eip, true);
  assert.equal(st.codex, true);
  assert.equal(st.rotationStrategy, 'none');
});
