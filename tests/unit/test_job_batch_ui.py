"""UI contract tests for JSON Job batch upload and confirmation."""

from __future__ import annotations

import json
import shutil
import subprocess

import pytest

from elastic_agent.api.routes.ui import _BATCH_HTML


def _between(source: str, start: str, end: str) -> str:
    start_at = source.index(start)
    return source[start_at : source.index(end, start_at)]


def _run_node_json(source: str) -> object:
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is required for the inline JavaScript behavior test")
    completed = subprocess.run(
        [node, "-e", source],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return json.loads(completed.stdout)


def test_batch_console_has_json_file_picker_and_explicit_confirmation():
    assert 'id="batchJsonSubmissionTab"' in _BATCH_HTML
    assert 'id="batchJsonFile" type="file"' in _BATCH_HTML
    assert 'accept=".json,application/json"' in _BATCH_HTML
    assert 'id="batchJsonPlanBtn"' in _BATCH_HTML
    assert 'onclick="planBatchJson()" disabled' in _BATCH_HTML
    assert 'id="batchJsonSubmitBtn"' in _BATCH_HTML
    assert 'onclick="submitBatchJson()" disabled' in _BATCH_HTML


def test_batch_manifest_is_memory_only_and_plan_precedes_submit():
    batch_script = _between(
        _BATCH_HTML,
        "// ---- Batch JSON submission ----",
        "function lines(id)",
    )
    plan_function = _between(
        batch_script,
        "async function planBatchJson()",
        "function batchReceiptStateClass",
    )
    submit_function = _between(
        batch_script,
        "async function submitBatchJson()",
        "async function refreshActiveJobBatch()",
    )

    assert "localStorage" not in batch_script
    assert "sessionStorage.getItem(BATCH_PENDING_INTENT_STORAGE_KEY)" in batch_script
    assert "sessionStorage.setItem(BATCH_PENDING_INTENT_STORAGE_KEY" in batch_script
    stored_intent = _between(
        batch_script,
        "sessionStorage.setItem(BATCH_PENDING_INTENT_STORAGE_KEY, JSON.stringify({",
        "}));",
    )
    assert "file_hash: fileHash" in stored_intent
    assert "idempotency_key: key" in stored_intent
    assert "rawSource" not in stored_intent
    assert "manifest" not in stored_intent
    assert "FileReader" not in batch_script
    assert "await file.arrayBuffer()" in plan_function
    assert "JSON.parse(source)" in plan_function
    assert "'/job-batches/plan'" in plan_function
    assert "batchJsonState.planValid" in submit_function
    assert "必须先重新解析并通过全部预检" in submit_function
    assert "window.confirm(" in submit_function
    assert "fileInput.disabled = true" in submit_function
    assert "generation !== batchJsonState.generation" in submit_function
    assert "'/job-batches'" in submit_function
    assert batch_script.index("'/job-batches/plan'") < batch_script.index("'/job-batches'")


def test_batch_ui_does_not_render_environment_values():
    batch_script = _between(
        _BATCH_HTML,
        "// ---- Batch JSON submission ----",
        "function lines(id)",
    )

    assert "batchHiddenEnvironmentValues" in batch_script
    assert "safeBatchServerText" in batch_script
    assert "Object.values(values)" in batch_script
    assert "[已隐藏环境变量值]" in batch_script
    assert "run.env 中疑似秘密字段" in batch_script
    assert "字段值未显示" in batch_script


def test_batch_ui_local_validation_rejects_placeholders_and_strictness():
    validation_source = _between(
        _BATCH_HTML,
        "const BATCH_JSON_MAX_BYTES",
        "async function sha256Hex",
    )
    result = _run_node_json(
        validation_source
        + """
const manifest = {
  schema_version: 1,
  batch_id: 'local-contract',
  unexpected: true,
  jobs: [
    {
      client_id: 'same-client',
      spec: {
        name: 'first',
        run: {
          command: 'true',
          env: {TOKENROUTER_API_KEY: 'private-value-must-not-render'},
          secret_env: {AWS_TOKEN: '[SECRET_REFERENCE]'},
        },
      },
    },
    {client_id: 'same-client', spec: {name: 'second', run: {command: 'true'}}},
  ],
};
const result = validateBatchManifest(manifest);
process.stdout.write(JSON.stringify(result));
"""
    )

    assert any("unexpected" in error for error in result["errors"])
    assert result["items"][0]["valid"] is False
    assert any("SECRET_REFERENCE" in error for error in result["items"][0]["errors"])
    assert result["items"][0]["warnings"]
    assert result["items"][1]["valid"] is False
    assert "private-value-must-not-render" not in json.dumps(result)


def test_batch_ui_rejects_semantically_duplicate_json_keys():
    validation_source = _between(
        _BATCH_HTML,
        "const BATCH_JSON_MAX_BYTES",
        "async function sha256Hex",
    )
    result = _run_node_json(
        validation_source
        + r"""
let duplicateRejected = false;
try {
  assertNoDuplicateJsonKeys('{"a":1,"\\u0061":2}');
} catch (_) {
  duplicateRejected = true;
}
assertNoDuplicateJsonKeys('{"a":1,"nested":{"a":2},"items":[{"a":3}]}');
process.stdout.write(JSON.stringify({duplicateRejected}));
"""
    )

    assert result == {"duplicateRejected": True}


def test_batch_ui_understands_backend_queue_terminal_states():
    batch_script = _between(
        _BATCH_HTML,
        "// ---- Batch JSON submission ----",
        "function lines(id)",
    )
    state_class = _between(
        batch_script,
        "function batchReceiptStateClass",
        "function batchReceiptIsTerminal",
    )
    terminal_check = _between(
        batch_script,
        "function batchReceiptIsTerminal",
        "function renderBatchReceipt",
    )
    receipt_render = _between(
        batch_script,
        "function renderBatchReceipt",
        "async function submitBatchJson",
    )

    assert "terminal" in state_class
    assert "error" in state_class
    assert "terminal" in terminal_check
    assert "receipt?.summary?.error" in receipt_render
    assert "item?.job_state || '').toLowerCase() === 'failed'" in receipt_render
    assert "terminalBatch && batchErrorCount > 0" in receipt_render
    assert "itemState === 'terminal' && item?.job_state" in receipt_render
    assert "itemState + ' · ' + terminalJobState" in receipt_render


def test_batch_status_poll_ignores_stale_file_generation():
    batch_script = _between(
        _BATCH_HTML,
        "// ---- Batch JSON submission ----",
        "function lines(id)",
    )
    refresh_function = batch_script[
        batch_script.index("async function refreshActiveJobBatch()") :
    ]

    assert "const requestedId = batchJsonState.jobBatchId" in refresh_function
    assert "const requestedGeneration = batchJsonState.generation" in refresh_function
    assert "batchJsonState.jobBatchId !== requestedId" in refresh_function
    assert "batchJsonState.generation !== requestedGeneration" in refresh_function
    assert "batchJsonState.jobBatchId === requestedId" in refresh_function
