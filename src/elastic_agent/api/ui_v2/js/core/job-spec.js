/**
 * Pure JobSpec builder + client-side validator.
 *
 * Mirrors ``elastic_agent.core.job_spec.JobSpec`` (current main, including
 * resume_command / collect.checkpoint / recovery).  Takes a plain values
 * object so it is unit-testable without DOM.
 *
 * Field keys deliberately reuse the legacy control IDs (jName, jRun, …) so
 * the /batch semantics tests map 1:1 onto this module.
 */

export const JOB_FORM_DEFAULTS = Object.freeze({
  jName: '',
  jProfile: 'ubuntu-agent-v1',
  jNamePrefix: '',
  jRegion: '',
  jWorkers: '1',
  jInstanceType: '',
  jDiskGb: '0',
  jSpot: 'false',
  jNeedsDocker: 'false',
  jRepo: '',
  jDeliver: 'manager_rsync',
  jTargetDir: '/opt/elastic-agent/harness',
  jSetup: '',
  jRepoRef: 'main',
  jResolvedCommit: '',
  jSetupSteps: '',
  jS3: '',
  jAgentType: 'claude',
  jAcctMode: 'worker_local_login',
  jAcctGroup: 'standard',
  jAgentModel: '',
  jAcctBinding: 'none',
  jAcctIds: [],
  jConfigDir: '',
  jPerWorker: '1',
  jLoginTimeout: '900',
  jRun: '',
  jRunResumeCommand: '',
  jCwd: '.',
  jShard: 'hostname',
  jShell: 'true',
  jRunTimeout: '86400',
  jTtl: '172800',
  jEnv: '',
  jSecretEnv: '',
  jCollect: 'results',
  jCollectInterval: '0',
  jCollectCheckpoint: 'false',
  jCollectExclude: '',
  jCheckpointRetention: '3',
  jRecoveryPolicy: 'none',
  jRecoveryJob: '',
  jRecoveryPaths: '',
  jRecoveryGeneration: '',
  jRot: 'none',
  jResume: '',
  jMaxRotations: '20',
  jHarnessRef: '',
});

export function lines(raw) {
  return String(raw || '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
}

export function parseKeyValueLines(raw, label) {
  const env = {};
  for (const line of lines(raw)) {
    const eq = line.indexOf('=');
    if (eq <= 0) throw new Error(`${label}每行必须是 KEY=VALUE。`);
    const key = line.slice(0, eq).trim();
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key)) {
      throw new Error(`${label}变量名不合法：${key}`);
    }
    env[key] = line.slice(eq + 1);
  }
  return env;
}

export function parseSetupSteps(raw) {
  const trimmed = String(raw || '').trim();
  if (!trimmed) return [];
  let value;
  try {
    value = JSON.parse(trimmed);
  } catch (_) {
    throw new Error('结构化初始化步骤必须是有效的 JSON array。');
  }
  if (!Array.isArray(value)) throw new Error('结构化初始化步骤必须是 JSON array。');
  return value;
}

/**
 * ``s3://uri dest`` per line; whitespace inside ``{{template}}`` spans is not a
 * separator (matches the legacy parser).
 */
export function parseS3DatasetLine(line) {
  let inTemplate = false;
  let separator = -1;
  for (let index = 0; index < line.length; index += 1) {
    if (!inTemplate && line.startsWith('{{', index)) { inTemplate = true; index += 1; continue; }
    if (inTemplate && line.startsWith('}}', index)) { inTemplate = false; index += 1; continue; }
    if (!inTemplate && /\s/.test(line[index])) { separator = index; break; }
  }
  if (inTemplate || separator < 0) {
    throw new Error('S3 数据集每行必须包含完整 URI 和目标路径。');
  }
  const uri = line.slice(0, separator).trim();
  let index = separator;
  while (index < line.length && /\s/.test(line[index])) index += 1;
  const dest = line.slice(index).trim();
  if (!uri.startsWith('s3://') || !dest) {
    throw new Error('S3 数据集每行必须是“s3://桶/路径 目标路径”。');
  }
  return { uri, dest };
}

export function parseS3Datasets(raw) {
  return lines(raw).map(parseS3DatasetLine);
}

const int = (value, fallback) => {
  const parsed = parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
};

/** Derived flags used by both the builder and the dynamic form UI. */
export function deriveFormState(values) {
  const accountMode = values.jAcctMode;
  const accountEnabled = accountMode !== 'none';
  const workerLocal = accountMode === 'worker_local_login';
  const binding = workerLocal ? values.jAcctBinding : 'none';
  const eip = binding === 'eip';
  const rotationStrategy = accountEnabled && !eip ? values.jRot : 'none';
  const checkpoint = values.jCollectCheckpoint === 'true';
  const recoveryPolicy = values.jRecoveryPolicy || 'none';
  return {
    accountMode,
    accountEnabled,
    workerLocal,
    binding,
    eip,
    rotationStrategy,
    rotationEnabled: rotationStrategy === 'on_exhaust_restart_resume',
    checkpoint,
    recoveryPolicy,
    recoveryEnabled: recoveryPolicy !== 'none',
    codex: values.jAgentType === 'codex',
    hasRepo: Boolean(String(values.jRepo || '').trim()),
  };
}

/**
 * Client-side validation.  Returns ``[{field, message}]``; empty means the
 * spec may be built and sent to ``/api/jobs/plan``.
 */
export function validateJobForm(values) {
  const errors = [];
  const st = deriveFormState(values);
  const push = (field, message) => errors.push({ field, message });

  if (!String(values.jRun || '').trim()) push('jRun', '请填写运行命令。');

  try { parseSetupSteps(values.jSetupSteps); } catch (error) { push('jSetupSteps', error.message); }
  try { parseS3Datasets(values.jS3); } catch (error) { push('jS3', error.message); }
  try { parseKeyValueLines(values.jEnv, '普通环境变量'); } catch (error) { push('jEnv', error.message); }
  try {
    const secret = parseKeyValueLines(values.jSecretEnv, '秘密环境变量');
    for (const value of Object.values(secret)) {
      if (!/^aws-(secretsmanager|ssm):\/\//.test(value.trim())) {
        push('jSecretEnv', '秘密环境变量的值必须是 aws-secretsmanager:// 或 aws-ssm:// 引用。');
        break;
      }
    }
  } catch (error) { push('jSecretEnv', error.message); }

  const runTimeout = int(values.jRunTimeout, 86400);
  const ttl = int(values.jTtl, 172800);
  if (runTimeout < 60 || runTimeout > 2592000) push('jRunTimeout', '运行超时须在 60 秒到 30 天之间。');
  if (ttl < 300 || ttl > 2592000) push('jTtl', 'Job 总生命周期须在 300 秒到 30 天之间。');
  if (ttl < runTimeout) push('jTtl', 'Job 总生命周期不能短于运行超时。');

  const workers = int(values.jWorkers, 1);
  if (workers < 1 || workers > 100) push('jWorkers', 'Worker 数量须在 1–100 之间。');
  const diskGb = int(values.jDiskGb, 0);
  if (diskGb < 0 || diskGb > 2048) push('jDiskGb', '根盘大小须在 0–2048 GiB 之间。');

  if (st.accountEnabled) {
    const perWorker = int(values.jPerWorker, 1);
    if (perWorker < 1 || perWorker > 32) push('jPerWorker', '每台 Worker 账号数须在 1–32 之间。');
    const loginTimeout = int(values.jLoginTimeout, 900);
    if (loginTimeout < 60 || loginTimeout > 1200) push('jLoginTimeout', '登录超时须在 60–1200 秒之间。');
    const configDir = String(values.jConfigDir || '').trim();
    if (configDir && !configDir.startsWith('/')) push('jConfigDir', '凭据目录必须是绝对路径。');
    if (st.codex && !configDir && (perWorker > 1 || st.rotationEnabled)) {
      push('jConfigDir', 'Codex 多账号或换号必须显式填写可写的绝对凭据目录。');
    }
  }

  const ids = Array.isArray(values.jAcctIds) ? values.jAcctIds.filter(Boolean) : [];
  if (st.eip) {
    if (ids.length && ids.length !== workers) {
      push('jAcctIds', `EIP 绑定模式下，指定账号数必须等于 Worker 数（当前 ${ids.length}/${workers}）。`);
    }
    if (int(values.jPerWorker, 1) !== 1) push('jPerWorker', '固定 EIP 模式下每台 Worker 只能使用 1 个账号。');
    if (values.jRot === 'on_exhaust_restart_resume') push('jRot', '固定 EIP 模式不支持原机换号。');
  } else if (st.accountEnabled && ids.length) {
    const expected = workers * int(values.jPerWorker, 1);
    if (ids.length !== expected) {
      push('jAcctIds', `指定账号数必须等于 Workers × 每台账号数（当前 ${ids.length}/${expected}）。`);
    }
  }

  if (st.checkpoint) {
    if (!lines(values.jCollect).length) push('jCollect', '开启原子检查点时至少填写一个结果目录。');
    if (values.jShard !== 'shard_index') push('jShard', '原子检查点要求 Worker 区分方式为 shard_index。');
    const retention = int(values.jCheckpointRetention, 3);
    if (retention < 1 || retention > 100) push('jCheckpointRetention', '检查点保留代数须在 1–100 之间。');
  }
  const interval = int(values.jCollectInterval, 0);
  if (interval < 0 || interval > 86400) push('jCollectInterval', '收集间隔须在 0–86400 秒之间。');

  if (st.recoveryEnabled) {
    if (!String(values.jRecoveryJob || '').trim()) push('jRecoveryJob', '启用恢复时必须填写来源 Job ID。');
    if (!lines(values.jRecoveryPaths).length) push('jRecoveryPaths', '启用恢复时至少填写一个恢复目录。');
    if (st.recoveryPolicy !== 'checkpoint' && String(values.jRecoveryGeneration || '').trim()) {
      push('jRecoveryGeneration', '仅原子检查点恢复可以指定 generation。');
    }
  }

  if (st.rotationEnabled) {
    const maxRotations = int(values.jMaxRotations, 20);
    if (maxRotations < 0 || maxRotations > 100) push('jMaxRotations', '换号次数须在 0–100 之间。');
  }

  return errors;
}

/** Build the exact JobSpec JSON; assumes validateJobForm returned []. */
export function buildJobSpec(values) {
  const st = deriveFormState(values);
  const workers = int(values.jWorkers, 1);
  const perWorker = st.accountEnabled ? int(values.jPerWorker, 1) : 1;
  const repo = String(values.jRepo || '').trim() || null;
  const ids = Array.isArray(values.jAcctIds) ? values.jAcctIds.filter(Boolean) : [];

  const setup = {
    repo,
    target_dir: String(values.jTargetDir || '').trim(),
    commands: lines(values.jSetup),
    steps: parseSetupSteps(values.jSetupSteps),
    deliver: values.jDeliver,
    needs_docker: values.jNeedsDocker === 'true',
    s3_datasets: parseS3Datasets(values.jS3),
  };
  if (repo) {
    setup.ref = String(values.jRepoRef || '').trim();
    setup.resolved_commit = String(values.jResolvedCommit || '').trim();
  }

  const spec = {
    name: String(values.jName || '').trim() || 'job',
    environment: { profile: values.jProfile },
    setup,
    run: {
      command: String(values.jRun || '').trim(),
      resume_command: String(values.jRunResumeCommand || '').trim(),
      cwd: String(values.jCwd || '').trim() || '.',
      env: parseKeyValueLines(values.jEnv, '普通环境变量'),
      secret_env: parseKeyValueLines(values.jSecretEnv, '秘密环境变量'),
      timeout: int(values.jRunTimeout, 86400),
      shell: values.jShell === 'true',
    },
    ttl_seconds: int(values.jTtl, 172800),
    account: {
      mode: st.accountMode,
      agent_type: values.jAgentType,
      model: st.accountEnabled ? String(values.jAgentModel || '').trim() : '',
      group: st.accountEnabled ? String(values.jAcctGroup || '').trim() || 'standard' : 'standard',
      per_worker: perWorker,
      config_dir: st.accountEnabled ? String(values.jConfigDir || '').trim() : '',
      login_timeout_seconds: st.accountEnabled ? int(values.jLoginTimeout, 900) : 900,
      binding: st.binding,
      ids,
    },
    rotation: {
      strategy: st.rotationStrategy,
      resume_args: st.rotationEnabled ? String(values.jResume || '').trim() : '',
      max_rotations: st.rotationEnabled ? int(values.jMaxRotations, 20) : 0,
    },
    fanout: {
      workers,
      shard_by: values.jShard,
      name_prefix: String(values.jNamePrefix || '').trim(),
      instance_type: String(values.jInstanceType || '').trim(),
      region: String(values.jRegion || '').trim(),
      disk_gb: int(values.jDiskGb, 0),
      spot: values.jSpot === 'true',
    },
    collect: {
      paths: lines(values.jCollect),
      exclude: lines(values.jCollectExclude),
      checkpoint: st.checkpoint,
      checkpoint_keep_generations: int(values.jCheckpointRetention, 3),
      interval_seconds: int(values.jCollectInterval, 0),
    },
    recovery: {
      policy: st.recoveryPolicy,
      source_job_id: st.recoveryEnabled ? String(values.jRecoveryJob || '').trim() : '',
      paths: st.recoveryEnabled ? lines(values.jRecoveryPaths) : [],
      generation: st.recoveryPolicy === 'checkpoint' ? String(values.jRecoveryGeneration || '').trim() : '',
    },
  };
  const harnessRef = String(values.jHarnessRef || '').trim();
  if (harnessRef) spec.harness_ref = harnessRef;
  return spec;
}

/**
 * Per-intent idempotency key state machine: the key is reused only while the
 * serialized spec is byte-identical; any edit mints a new key.
 */
export function createSubmissionIntent() {
  let key = null;
  let fingerprint = null;
  return {
    keyFor(spec) {
      const serialized = JSON.stringify(spec);
      if (serialized !== fingerprint || !key) {
        fingerprint = serialized;
        key = (crypto.randomUUID && crypto.randomUUID())
          || `${Date.now()}-${Math.random().toString(36).slice(2)}`;
      }
      return key;
    },
    clear() {
      key = null;
      fingerprint = null;
    },
  };
}
