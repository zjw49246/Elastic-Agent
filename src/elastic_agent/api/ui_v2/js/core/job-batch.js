/** Pure helpers for strict JSON JobBatch manifests. */

export const JOB_BATCH_MAX_BYTES = 2 * 1024 * 1024;
export const JOB_BATCH_SCHEMA_MAX_ITEMS = 100;
export const JOB_BATCH_DEFAULT_MAX_ITEMS = 20;

const PUBLIC_ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

export function isPlainObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

export function utf8ByteLength(value) {
  return new TextEncoder().encode(String(value)).byteLength;
}

/** Reject duplicate object keys, including escaped spellings such as a/\u0061. */
export function assertNoDuplicateJsonKeys(source) {
  let position = 0;
  const skipWhitespace = () => {
    while (position < source.length && /\s/.test(source[position])) position += 1;
  };
  const scanString = () => {
    const start = position;
    position += 1;
    while (position < source.length) {
      if (source[position] === '\\') {
        position += 2;
      } else if (source[position] === '"') {
        position += 1;
        return JSON.parse(source.slice(start, position));
      } else {
        position += 1;
      }
    }
    throw new Error('JSON 字符串未闭合。');
  };
  const scanValue = (depth) => {
    if (depth > 128) throw new Error('JSON 嵌套层级超过 128。');
    skipWhitespace();
    const token = source[position];
    if (token === '"') {
      scanString();
      return;
    }
    if (token === '{') {
      position += 1;
      skipWhitespace();
      const keys = new Set();
      if (source[position] === '}') { position += 1; return; }
      while (position < source.length) {
        skipWhitespace();
        if (source[position] !== '"') throw new Error('JSON object key 必须是字符串。');
        const key = scanString();
        if (keys.has(key)) throw new Error(`JSON 中存在重复 object key：${key}`);
        keys.add(key);
        skipWhitespace();
        if (source[position] !== ':') throw new Error('JSON object 缺少冒号。');
        position += 1;
        scanValue(depth + 1);
        skipWhitespace();
        if (source[position] === '}') { position += 1; return; }
        if (source[position] !== ',') throw new Error('JSON object 分隔符无效。');
        position += 1;
      }
      throw new Error('JSON object 未闭合。');
    }
    if (token === '[') {
      position += 1;
      skipWhitespace();
      if (source[position] === ']') { position += 1; return; }
      while (position < source.length) {
        scanValue(depth + 1);
        skipWhitespace();
        if (source[position] === ']') { position += 1; return; }
        if (source[position] !== ',') throw new Error('JSON array 分隔符无效。');
        position += 1;
      }
      throw new Error('JSON array 未闭合。');
    }
    const start = position;
    while (position < source.length) {
      const character = source[position];
      if (/\s/.test(character) || [',', '}', ']'].includes(character)) break;
      position += 1;
    }
    if (position === start) throw new Error('JSON value 无效。');
  };

  scanValue(0);
  skipWhitespace();
  if (position !== source.length) throw new Error('JSON 根节点后存在多余内容。');
}

export function validateBatchManifest(manifest) {
  const errors = [];
  if (!isPlainObject(manifest)) return ['JSON 顶层必须是 object。'];

  for (const key of Object.keys(manifest)) {
    if (!['schema_version', 'batch_id', 'policy', 'jobs'].includes(key)) {
      errors.push(`manifest 包含不支持的字段：${key}`);
    }
  }
  if (manifest.schema_version !== 1) errors.push('schema_version 必须严格等于 1。');
  if (typeof manifest.batch_id !== 'string' || !PUBLIC_ID.test(manifest.batch_id)) {
    errors.push('batch_id 必须为 1–128 位字母、数字、点、下划线或连字符。');
  }

  if (manifest.policy !== undefined) {
    if (!isPlainObject(manifest.policy)) {
      errors.push('policy 必须是 object。');
    } else {
      for (const key of Object.keys(manifest.policy)) {
        if (!['max_active_jobs', 'on_job_failure'].includes(key)) {
          errors.push(`policy 包含不支持的字段：${key}`);
        }
      }
      const active = manifest.policy.max_active_jobs;
      if (active !== undefined && (!Number.isInteger(active) || active < 1 || active > 10)) {
        errors.push('policy.max_active_jobs 必须是 1–10 的整数。');
      }
      const failure = manifest.policy.on_job_failure;
      if (failure !== undefined && failure !== 'continue') {
        errors.push('schema v1 的 policy.on_job_failure 只支持 "continue"。');
      }
    }
  }

  if (!Array.isArray(manifest.jobs) || manifest.jobs.length === 0) {
    errors.push('jobs 必须是非空 array。');
    return errors;
  }
  if (manifest.jobs.length > JOB_BATCH_SCHEMA_MAX_ITEMS) {
    errors.push(`schema 最多允许 ${JOB_BATCH_SCHEMA_MAX_ITEMS} 个 Job。`);
  }
  const clientIds = new Set();
  for (const [index, item] of manifest.jobs.slice(0, JOB_BATCH_SCHEMA_MAX_ITEMS).entries()) {
    if (!isPlainObject(item)) {
      errors.push(`jobs.${index} 必须是 object。`);
      continue;
    }
    for (const key of Object.keys(item)) {
      if (!['client_id', 'spec'].includes(key)) errors.push(`jobs.${index} 包含不支持的字段：${key}`);
    }
    if (typeof item.client_id !== 'string' || !PUBLIC_ID.test(item.client_id)) {
      errors.push(`jobs.${index}.client_id 格式不合法。`);
    } else if (clientIds.has(item.client_id)) {
      errors.push(`client_id 在同一批次中必须唯一：${item.client_id}`);
    } else {
      clientIds.add(item.client_id);
    }
    if (!isPlainObject(item.spec)) errors.push(`jobs.${index}.spec 必须是完整 JobSpec object。`);
  }
  return errors;
}

export function parseBatchSource(source) {
  const raw = String(source);
  if (!raw.trim()) throw new Error('请粘贴或上传 JSON manifest。');
  if (utf8ByteLength(raw) > JOB_BATCH_MAX_BYTES) throw new Error('JSON 内容不能超过 2 MiB。');
  let manifest;
  try {
    manifest = JSON.parse(raw);
  } catch (error) {
    throw new Error(`JSON 解析失败：${error.message}`);
  }
  assertNoDuplicateJsonKeys(raw);
  const errors = validateBatchManifest(manifest);
  if (errors.length) throw new Error(errors.join('\n'));
  return manifest;
}

export async function batchIdempotencyKey(batchId) {
  if (!globalThis.crypto || !globalThis.crypto.subtle) {
    throw new Error('当前环境不支持安全 SHA-256，无法生成稳定幂等键。');
  }
  const bytes = new TextEncoder().encode(`batch-json-v1\n${String(batchId)}`);
  const digest = await globalThis.crypto.subtle.digest('SHA-256', bytes);
  const hex = Array.from(new Uint8Array(digest))
    .map((byte) => byte.toString(16).padStart(2, '0')).join('');
  return `batch-json-v1-${hex}`;
}

export function batchReceiptIsTerminal(receipt) {
  return String(receipt && receipt.state || '').toLowerCase() === 'terminal';
}
