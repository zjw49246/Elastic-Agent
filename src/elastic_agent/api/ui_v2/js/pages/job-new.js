/**
 * Submit Job — four-step wizard over the eight JobSpec sections.
 *
 * Field IDs keep the legacy names (jName, jRun, …) so semantics tests map
 * across. Draft lives in application memory only (never session/local
 * storage): run.env / secret refs / harness code must not be persisted.
 */

import { el, clear } from '../core/dom.js';
import { get, post } from '../core/api.js';
import { describeError } from '../core/errors.js';
import { getJobDraft, setJobDraft, clearJobDraft } from '../core/store.js';
import {
  JOB_FORM_DEFAULTS, validateJobForm, buildJobSpec, deriveFormState,
  createSubmissionIntent,
} from '../core/job-spec.js';
import { toastError, toastSuccess } from '../components/toast.js';

const STEPS = [
  { id: 'step-basics', label: '1. 基本信息与计算资源' },
  { id: 'step-source', label: '2. 代码、环境与数据' },
  { id: 'step-run', label: '3. Agent、账号与命令' },
  { id: 'step-results', label: '4. 结果、续跑与高级设置' },
];

// field id -> step index, used to jump to the first invalid control.
const FIELD_STEP = {
  jName: 0, jProfile: 0, jNamePrefix: 0, jRegion: 0, jWorkers: 0, jInstanceType: 0,
  jDiskGb: 0, jSpot: 0, jNeedsDocker: 0,
  jRepo: 1, jDeliver: 1, jTargetDir: 1, jSetup: 1, jRepoRef: 1, jResolvedCommit: 1,
  jSetupSteps: 1, jS3: 1,
  jAgentType: 2, jAcctMode: 2, jAcctGroup: 2, jAgentModel: 2, jAcctBinding: 2,
  jAcctIds: 2, jConfigDir: 2, jPerWorker: 2, jLoginTimeout: 2, jRun: 2,
  jRunResumeCommand: 2, jCwd: 2, jShard: 2, jShell: 2, jRunTimeout: 2, jTtl: 2,
  jEnv: 2, jSecretEnv: 2,
  jCollect: 3, jCollectInterval: 3, jCollectCheckpoint: 3, jCollectExclude: 3,
  jCheckpointRetention: 3, jRecoveryPolicy: 3, jRecoveryJob: 3, jRecoveryPaths: 3,
  jRecoveryGeneration: 3, jRot: 3, jResume: 3, jMaxRotations: 3, jHarnessRef: 3,
};

export function createPage({ router, container }) {
  const controls = new Map();
  const nodes = {};
  const intent = createSubmissionIntent();
  let activeStep = 0;
  let providerType = '';
  let accounts = [];
  let submitting = false;

  function mount() {
    container.appendChild(buildLayout());
    restoreDraft();
    updateDynamicState();
    void loadContext();
  }

  function dispose() {
    setJobDraft(collectValues());
  }

  // ---------------------------------------------------------------- values

  function collectValues() {
    const values = { ...JOB_FORM_DEFAULTS };
    for (const [id, control] of controls) {
      if (control.multiple) {
        values[id] = Array.from(control.selectedOptions).map((o) => o.value);
      } else {
        values[id] = control.value;
      }
    }
    return values;
  }

  function restoreDraft() {
    const draft = getJobDraft();
    if (!draft) return;
    for (const [id, control] of controls) {
      if (!(id in draft)) continue;
      if (control.multiple) {
        const wanted = new Set(draft[id] || []);
        for (const option of control.options) option.selected = wanted.has(option.value);
      } else {
        control.value = draft[id];
      }
    }
  }

  // ------------------------------------------------------------- context

  async function loadContext() {
    try {
      const health = await get('/health');
      providerType = health.provider || '';
      if (providerType === 'aws' && !getJobDraft()) {
        // AWS Managers default to the persistent-EIP flow.
        controls.get('jAcctBinding').value = 'eip';
        updateDynamicState();
      }
    } catch (_) { /* defaults stay */ }
    try {
      const data = await get('/accounts');
      accounts = data.accounts || [];
      renderAccountOptions();
    } catch (error) {
      nodes.acctIdsHelp.textContent = `账号列表加载失败：${describeError(error)}`;
    }
  }

  function renderAccountOptions() {
    const picker = controls.get('jAcctIds');
    const selected = new Set(Array.from(picker.selectedOptions).map((o) => o.value));
    clear(picker);
    for (const account of accounts) {
      const agents = account.supported_agent_types && account.supported_agent_types.length
        ? account.supported_agent_types
        : (account.agent_type ? [account.agent_type] : []);
      const option = el('option', {
        value: account.id,
        text: `${account.id}${account.email ? ` (${account.email})` : ''}${account.auth_kind === 'agent_api' ? ' [API]' : ''}`,
        dataset: { enabled: String(Boolean(account.enabled)), agentTypes: agents.join(',') },
      });
      if (selected.has(account.id)) option.selected = true;
      picker.appendChild(option);
    }
    filterAccountOptions();
  }

  function filterAccountOptions() {
    const agentType = controls.get('jAgentType').value;
    const picker = controls.get('jAcctIds');
    for (const option of picker.options) {
      const supported = (option.dataset.agentTypes || '').split(',').includes(agentType);
      option.disabled = option.dataset.enabled !== 'true' || !supported;
      if (option.disabled) option.selected = false;
    }
  }

  // ----------------------------------------------------------- dynamics

  function updateDynamicState() {
    const values = collectValues();
    const st = deriveFormState(values);

    setDisabled('jAcctGroup', !st.accountEnabled, '账号模式为“不配置账号”时不适用。');
    setDisabled('jAgentModel', !st.accountEnabled, '账号模式为“不配置账号”时不适用。');
    setDisabled('jConfigDir', !st.accountEnabled, '账号模式为“不配置账号”时不适用。');
    setDisabled('jLoginTimeout', !st.accountEnabled, '账号模式为“不配置账号”时不适用。');
    setDisabled('jAcctBinding', !st.workerLocal, '固定 EIP 只适用于 Worker 本地登录。');
    setDisabled('jAcctIds', !st.accountEnabled, '账号模式为“不配置账号”时不适用。');
    setDisabled('jPerWorker', !st.accountEnabled || st.eip,
      st.eip ? '固定 EIP 模式强制每台 1 个账号。' : '账号模式为“不配置账号”时不适用。');
    if (st.eip) controls.get('jPerWorker').value = '1';

    const rotationBlocked = !st.accountEnabled || st.eip;
    setDisabled('jRot', rotationBlocked,
      st.eip ? '固定 EIP 模式不支持原机换号。' : '未配置账号时不适用。');
    if (rotationBlocked) controls.get('jRot').value = 'none';
    const rotationOn = controls.get('jRot').value === 'on_exhaust_restart_resume';
    setDisabled('jResume', rotationBlocked || !rotationOn);
    setDisabled('jMaxRotations', rotationBlocked || !rotationOn);

    setDisabled('jRepoRef', !st.hasRepo, '未填写 Repo 时不生效。');
    setDisabled('jResolvedCommit', !st.hasRepo, '未填写 Repo 时不生效。');

    setDisabled('jCollectExclude', false);
    setDisabled('jCheckpointRetention', !st.checkpoint, '仅原子检查点模式可用。');
    setDisabled('jRecoveryJob', !st.recoveryEnabled, '恢复策略为“不恢复”时不适用。');
    setDisabled('jRecoveryPaths', !st.recoveryEnabled, '恢复策略为“不恢复”时不适用。');
    setDisabled('jRecoveryGeneration', st.recoveryPolicy !== 'checkpoint', '仅原子检查点恢复可用。');

    controls.get('jConfigDir').placeholder = st.codex
      ? '/home/user/.codex（示例；必须是绝对路径）'
      : '/home/user/.claude（示例；必须是绝对路径）';

    nodes.eipHint.textContent = st.eip
      ? '固定 EIP 已启用：每台临时 EC2 只使用一个账号；指定账号数必须等于 Worker 数。Job 结束销毁 EC2，但保留并继续计费 EIP。'
      : (st.workerLocal && providerType === 'aws'
        ? 'AWS Manager 默认建议固定 EIP；当前选择普通临时公网出口，plan 会给出警告。'
        : '当前配置使用普通临时公网出口。');

    filterAccountOptions();
    renderSummary(values, st);
  }

  function setDisabled(id, disabled, reason = '') {
    const control = controls.get(id);
    if (!control) return;
    control.disabled = disabled;
    if (reason) control.title = disabled ? reason : '';
  }

  function renderSummary(values, st) {
    clear(nodes.summary);
    const items = [
      ['名称', values.jName || 'job'],
      ['Workers', values.jWorkers],
      ['Agent', values.jAgentType],
      ['账号模式', st.accountEnabled ? (st.eip ? '本地登录 + EIP' : '本地登录') : '不配置账号'],
      ['命令', (values.jRun || '').split('\n')[0].slice(0, 60) || '—'],
      ['收集', (values.jCollect || '').split('\n').filter(Boolean).join(', ') || '(不收集)'],
      ['运行超时', `${values.jRunTimeout}s`],
      ['TTL', `${values.jTtl}s`],
    ];
    for (const [label, value] of items) {
      nodes.summary.appendChild(el('li', {}, [
        el('span', { text: label }),
        el('span', { class: 'sv', text: String(value) }),
      ]));
    }
  }

  // --------------------------------------------------------------- steps

  function showStep(index) {
    activeStep = index;
    nodes.stepTabs.forEach((tab, i) => tab.setAttribute('aria-selected', String(i === index)));
    nodes.stepPanels.forEach((panel, i) => { panel.hidden = i !== index; });
  }

  function applyErrors(errors) {
    for (const control of controls.values()) control.removeAttribute('aria-invalid');
    for (const errNode of nodes.fieldErrors.values()) errNode.textContent = '';
    if (!errors.length) return;
    for (const { field, message } of errors) {
      const control = controls.get(field);
      if (control) control.setAttribute('aria-invalid', 'true');
      const errNode = nodes.fieldErrors.get(field);
      if (errNode && !errNode.textContent) errNode.textContent = message;
    }
    const first = errors[0];
    const step = FIELD_STEP[first.field] ?? activeStep;
    showStep(step);
    const control = controls.get(first.field);
    if (control) control.focus();
    toastError(first.message);
  }

  // -------------------------------------------------------- plan / submit

  function validateAndBuild() {
    const values = collectValues();
    const errors = validateJobForm(values);
    applyErrors(errors);
    if (errors.length) return null;
    return buildJobSpec(values);
  }

  async function planOnly() {
    const spec = validateAndBuild();
    if (!spec) return;
    nodes.planBtn.disabled = true;
    try {
      const plan = await post('/jobs/plan', spec);
      nodes.planOutput.hidden = false;
      nodes.planOutput.textContent = JSON.stringify(plan, null, 2);
      toastSuccess('Job 计划校验通过。');
    } catch (error) {
      nodes.planOutput.hidden = false;
      nodes.planOutput.textContent = describeError(error);
      toastError(describeError(error));
    } finally {
      nodes.planBtn.disabled = false;
    }
  }

  async function submit() {
    if (submitting) return;
    const spec = validateAndBuild();
    if (!spec) return;
    submitting = true;
    nodes.submitBtn.disabled = true;
    nodes.submitBtn.textContent = '启动中…';
    try {
      const plan = await post('/jobs/plan', spec);
      nodes.planOutput.hidden = false;
      nodes.planOutput.textContent = JSON.stringify(plan, null, 2);
      // One intent = one Idempotency-Key; retries with an unchanged spec
      // reuse it, so a flaky network cannot double-create the Job.
      const key = intent.keyFor(spec);
      const job = await post('/jobs', spec, { headers: { 'Idempotency-Key': key } });
      intent.clear();
      clearJobDraft();
      toastSuccess(`Job 已创建：${job.job_id}`);
      nodes.successNote.hidden = false;
      nodes.successNote.textContent = `Job 已创建：${job.job_id}（正在跳转详情…）`;
      router.navigate(`/jobs/${encodeURIComponent(job.job_id)}`);
    } catch (error) {
      toastError(describeError(error));
      nodes.planOutput.hidden = false;
      nodes.planOutput.textContent = describeError(error);
    } finally {
      submitting = false;
      nodes.submitBtn.disabled = false;
      nodes.submitBtn.textContent = '校验并启动 Job';
    }
  }

  // -------------------------------------------------------------- layout

  function field(id, label, control, help = '') {
    controls.set(id, control);
    control.id = id;
    control.addEventListener('change', updateDynamicState);
    control.addEventListener('input', updateDynamicState);
    const err = el('span', { class: 'err', role: 'alert' });
    nodes.fieldErrors.set(id, err);
    return el('div', { class: 'field' }, [
      el('label', { for: id, text: label }),
      control,
      help ? el('span', { class: 'help', text: help }) : null,
      err,
    ]);
  }

  const input = (attrs = {}) => el('input', { type: 'text', autocomplete: 'off', ...attrs });
  const number = (value, min, max) => el('input', { type: 'number', value, min, max });
  const select = (options, value) => {
    const node = el('select');
    for (const [v, label] of options) node.appendChild(el('option', { value: v, text: label, selected: v === value }));
    return node;
  };
  const textarea = (attrs = {}) => el('textarea', attrs);

  function buildLayout() {
    nodes.fieldErrors = new Map();
    const root = document.createDocumentFragment();
    root.appendChild(el('div', { class: 'page-head' }, [
      el('div', {}, [
        el('h1', { text: '提交 Job' }),
        el('p', { class: 'page-sub', text: '先“仅校验并查看计划”，再启动；同一提交意图复用同一 Idempotency-Key。' }),
      ]),
    ]));

    const layout = el('div', { class: 'job-layout' });
    const main = el('div', {});

    // step tabs
    const tabs = el('div', { class: 'steps', role: 'tablist' });
    nodes.stepTabs = STEPS.map((step, index) => {
      const tab = el('button', {
        type: 'button', class: 'step-tab', role: 'tab',
        'aria-selected': String(index === 0), text: step.label,
      });
      tab.addEventListener('click', () => showStep(index));
      tabs.appendChild(tab);
      return tab;
    });
    main.appendChild(tabs);

    // ---- step 1: basics + compute
    const step1 = el('div', { role: 'tabpanel' }, [
      el('fieldset', {}, [
        el('legend', { text: '基本信息' }),
        el('div', { class: 'form-grid' }, [
          field('jName', 'Job 名称', input({ placeholder: 'my-batch-job' })),
          field('jProfile', 'Worker 基础环境', select([
            ['ubuntu-agent-v1', 'ubuntu-agent-v1（标准）'],
            ['ubuntu-agent-docker-v1', 'ubuntu-agent-docker-v1（含 Docker）'],
          ], 'ubuntu-agent-v1'), '版本化、不可变的通用环境；Job 专属依赖在“代码与初始化”中安装。'),
          field('jNamePrefix', '机器名称前缀', input({ placeholder: '留空则使用 Job 名称' })),
          field('jRegion', '运行 Region', input({ placeholder: '留空则使用当前 Manager Region' }), '当前必须与 Manager Region 一致。'),
        ]),
      ]),
      el('fieldset', {}, [
        el('legend', { text: '计算资源' }),
        el('div', { class: 'form-grid' }, [
          field('jWorkers', 'Worker 数量', number('1', '1', '100'), '每台 Worker 各运行一次命令；上限 100。'),
          field('jInstanceType', '实例类型', input({ placeholder: '留空使用 Manager 默认' }), '须在部署的实例类型白名单内。'),
          field('jDiskGb', '根盘（GiB）', number('0', '0', '2048'), '0 使用默认值；Worker 销毁时根盘一并删除。'),
          field('jSpot', '购买方式', select([['false', '按需实例（推荐）'], ['true', 'Spot 实例']], 'false')),
          field('jNeedsDocker', '运行时需要 Docker', select([['false', '不需要'], ['true', '需要']], 'false'), '如 --sandbox os 类命令需开启。'),
        ]),
      ]),
    ]);

    // ---- step 2: source
    const step2 = el('div', { role: 'tabpanel', hidden: true }, [
      el('fieldset', {}, [
        el('legend', { text: '代码与初始化' }),
        el('div', { class: 'form-grid' }, [
          field('jRepo', '代码仓库 URL', input({ placeholder: 'https://github.com/org/repo.git' }), '可留空直接执行命令。'),
          field('jDeliver', '代码分发方式', select([
            ['manager_rsync', 'Manager rsync（私库推荐，token 不上机）'],
            ['worker_clone', 'Worker 自行 clone（仅公开仓库）'],
          ], 'manager_rsync')),
          field('jTargetDir', 'Worker 代码目录', input({ value: '/opt/elastic-agent/harness' })),
          field('jRepoRef', '分支或标签', input({ value: 'main' })),
          field('jResolvedCommit', '锁定 Commit SHA', input({ placeholder: '完整 40 位 SHA（推荐）' })),
        ]),
        field('jSetup', '初始化命令（每行一条）', textarea({ placeholder: 'uv sync' })),
        field('jSetupSteps', '结构化初始化步骤（JSON）', textarea({
          placeholder: '[{"name":"install","command":"uv sync","timeout":1200,"retries":1}]',
        }), '可选；固定以 Job 用户执行。'),
        field('jS3', 'S3 数据集（每行 “s3://桶/路径 目标目录”）', textarea({
          placeholder: 's3://bucket/shard-{{shard_id}} /data/input',
        }), 'Worker 用实例角色直连 S3 拉取；支持 {{shard_id}} 等模板。'),
      ]),
    ]);

    // ---- step 3: account + run
    nodes.eipHint = el('p', { class: 'help', role: 'status' });
    const step3 = el('div', { role: 'tabpanel', hidden: true }, [
      el('fieldset', {}, [
        el('legend', { text: 'Agent 与账号' }),
        el('div', { class: 'form-grid' }, [
          field('jAgentType', 'Agent', select([['claude', 'Claude'], ['codex', 'Codex']], 'claude')),
          field('jAcctMode', '账号使用方式', select([
            ['worker_local_login', 'Worker 本地登录'],
            ['none', '不配置账号（命令自带凭据）'],
          ], 'worker_local_login')),
          field('jAcctGroup', '账号组', input({ value: 'standard' })),
          field('jAgentModel', 'Agent 模型（可选）', input({ placeholder: '如 claude-opus-4-8' }), '填写后在计划、认领与配置三处校验。'),
          field('jAcctBinding', '固定公网出口', select([['none', '普通临时出口'], ['eip', '固定 EIP（AWS）']], 'none')),
          field('jPerWorker', '每台 Worker 预登录账号数', number('1', '1', '32')),
          field('jConfigDir', '凭据目录', input({ placeholder: '留空则使用 Agent 默认目录' })),
          field('jLoginTimeout', '自动登录页面超时（秒）', number('900', '60', '1200')),
        ]),
        field('jAcctIds', '指定账号（可选，多选）', el('select', { multiple: true, size: '5' }),
          'EIP 模式下数量须等于 Worker 数；留空按账号组自动分配。'),
        nodes.eipHint,
      ]),
      el('fieldset', {}, [
        el('legend', { text: '运行命令' }),
        field('jRun', '运行命令', textarea({ required: true, placeholder: 'uv run my-bench run --shard {{shard_index}}/{{num_shards}}' }),
          '支持 {{shard_id}}/{{shard_index}}/{{num_shards}} 模板。'),
        field('jRunResumeCommand', '恢复命令（可选）', textarea({ placeholder: '留空则复用运行命令' }), '用于检查点恢复后续跑。'),
        el('div', { class: 'form-grid' }, [
          field('jCwd', '命令工作目录', input({ value: '.' })),
          field('jShard', 'Worker 区分方式', select([
            ['hostname', 'hostname'], ['shard_index', 'shard_index'], ['none', '不区分'],
          ], 'hostname')),
          field('jShell', '命令解析方式', select([['true', 'Shell（bash -lc，推荐）'], ['false', '直接 argv']], 'true')),
          field('jRunTimeout', '运行超时（秒）', number('86400', '60', '2592000')),
          field('jTtl', 'Job 总生命周期（秒）', number('172800', '300', '2592000'), '不得短于运行超时。'),
        ]),
        field('jEnv', '普通环境变量（每行 KEY=VALUE）', textarea({})),
        field('jSecretEnv', '秘密环境变量引用（每行 KEY=aws-…）', textarea({
          placeholder: 'OPENAI_API_KEY=aws-secretsmanager://prod/openai#api_key',
        }), '只接受 aws-secretsmanager:// 或 aws-ssm:// 引用；实际值在下发前即时解析、API 不回显。'),
      ]),
    ]);

    // ---- step 4: results + rotation + advanced
    const step4 = el('div', { role: 'tabpanel', hidden: true }, [
      el('fieldset', {}, [
        el('legend', { text: '结果收集' }),
        el('div', { class: 'form-grid' }, [
          field('jCollect', '结果目录（每行一个，相对代码目录）', textarea({ placeholder: 'results' }), '为空表示不收集任何结果。'),
          field('jCollectInterval', '运行中收集间隔（秒）', number('0', '0', '86400'), '0 表示只做终态收集。'),
          field('jCollectCheckpoint', '原子检查点', select([['false', '关闭'], ['true', '开启']], 'false'),
            '开启后每次成功收集写入不可变、带校验的 S3 generation；要求 shard_index 分片。'),
          field('jCheckpointRetention', '检查点保留代数', number('3', '1', '100')),
        ]),
        field('jCollectExclude', '排除模式（每行一个相对 glob）', textarea({ placeholder: '.venv/**\n**/__pycache__/**' })),
      ]),
      el('fieldset', {}, [
        el('legend', { text: '从先前 Job 恢复' }),
        el('div', { class: 'form-grid' }, [
          field('jRecoveryPolicy', '恢复策略', select([
            ['none', '不恢复'], ['checkpoint', '原子检查点'], ['legacy_final_collection', '最终收集结果'],
          ], 'none')),
          field('jRecoveryJob', '来源 Job ID', input({ placeholder: 'job-…' })),
          field('jRecoveryGeneration', '指定 generation（可选）', input({})),
        ]),
        field('jRecoveryPaths', '恢复目录（每行一个）', textarea({})),
      ]),
      el('fieldset', {}, [
        el('legend', { text: '额度耗尽换号' }),
        el('div', { class: 'form-grid' }, [
          field('jRot', '额度耗尽后的处理', select([
            ['none', '结束任务'], ['on_exhaust_restart_resume', '换号并重启续跑'],
          ], 'none'), '仅普通（非 EIP）模式可用。'),
          field('jResume', '换号重启追加参数', input({ placeholder: '--resume' })),
          field('jMaxRotations', '最多自动换号次数', number('20', '0', '100')),
        ]),
      ]),
      el('fieldset', {}, [
        el('legend', { text: '高级：Harness 引用' }),
        field('jHarnessRef', '已上传 Harness 引用', input({ placeholder: '留空则使用声明式配置' }),
          'Harness 上传默认关闭，仅可信部署可用。'),
      ]),
    ]);

    nodes.stepPanels = [step1, step2, step3, step4];
    for (const panel of nodes.stepPanels) main.appendChild(panel);

    // actions
    nodes.planBtn = el('button', { type: 'button', class: 'btn', text: '仅校验并查看计划' });
    nodes.planBtn.addEventListener('click', planOnly);
    nodes.submitBtn = el('button', { type: 'button', class: 'btn btn-primary', text: '校验并启动 Job' });
    nodes.submitBtn.addEventListener('click', submit);
    main.appendChild(el('div', { class: 'row', style: { 'margin-top': '10px' } }, [nodes.planBtn, nodes.submitBtn]));

    nodes.successNote = el('p', { class: 'small', role: 'status', hidden: true });
    main.appendChild(nodes.successNote);
    nodes.planOutput = el('pre', { class: 'code', hidden: true, role: 'status' });
    main.appendChild(nodes.planOutput);

    // sticky summary
    const summaryCard = el('aside', { class: 'card job-summary', 'aria-label': '配置摘要' }, [
      el('h2', { text: '配置摘要' }),
    ]);
    nodes.summary = el('ul', { class: 'summary-list' });
    summaryCard.appendChild(nodes.summary);

    layout.appendChild(el('div', { class: 'card' }, [main]));
    layout.appendChild(summaryCard);
    root.appendChild(layout);
    return root;
  }

  return { mount, dispose };
}
