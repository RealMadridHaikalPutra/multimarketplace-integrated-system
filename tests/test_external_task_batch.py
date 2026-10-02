"""Regression: mode BATCH vs SATU PER SATU untuk external service task.

Dikontrol per-topic di ``infrastructure/camunda/config.json``:

    {"topic": "serviceWebhookTest", "batch": true|false, ...}

- ``batch: true``  -> SEMUA task topik tsb yang terkumpul dalam SATU poll
  di-merge jadi SATU aksi handler (satu POST /run-part berisi part_id ARRAY
  gabungan, dedup), lalu setiap task di-complete dengan hasil yang sama.
- ``batch: false`` (default / dihapus) -> dikerjakan SATU PER SATU per task.
- ``batch: true`` TAPI handler tidak ``batch_capable`` -> otomatis fallback
  per-task (aman).
"""

import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from infrastructure.camunda.external_task import (
    ExternalTask,
    ExternalTaskTopic,
    ExternalTaskWorker,
    parse_topics,
)


def _task(task_id: str, part_id) -> ExternalTask:
    return ExternalTask(
        id=task_id,
        topic_name="serviceWebhookTest",
        variables={"partId": part_id},
    )


def _worker(batch: bool, handler) -> ExternalTaskWorker:
    topic = ExternalTaskTopic(
        name="serviceWebhookTest", handler="handler", batch=batch
    )
    worker = ExternalTaskWorker(topics=[topic])
    worker.handlers["serviceWebhookTest"] = handler
    return worker


async def _patch_io(worker, tasks):
    worker.fetch_and_lock = AsyncMock(return_value=(tasks, None))
    worker.complete = AsyncMock(return_value=(True, None))
    worker.handle_failure = AsyncMock(return_value=(True, None))
    worker.handle_bpmn_error = AsyncMock(return_value=(True, None))


def _make_handler(calls, batch_capable=True):
    async def handler(task):
        calls.append(list(task.variables.get("partId") or []))
        return {
            "partId": task.variables.get("partId"),
            "pipelineStatus": "success",
        }

    if batch_capable:
        handler.batch_capable = True
    return handler


# ---------------------------------------------------------------------------
# Parse config.json
# ---------------------------------------------------------------------------


def test_topic_batch_flag_parsed_from_config():
    topic = ExternalTaskTopic.from_dict(
        {"topic": "serviceWebhookTest", "handler": "x", "batch": True}
    )
    assert topic.batch is True
    assert topic.to_dict()["batch"] is True

    default = ExternalTaskTopic.from_dict({"topic": "x", "handler": "y"})
    assert default.batch is False


def test_real_config_service_webhook_test_is_batch():
    """config.json produksi memuat batch untuk topic serviceWebhookTest."""
    config_path = (
        Path(__file__).resolve().parent.parent
        / "infrastructure"
        / "camunda"
        / "config.json"
    )
    data = json.loads(config_path.read_text(encoding="utf-8"))
    topics = parse_topics(data.get("external_tasks") or {})
    topic = next((t for t in topics if t.name == "serviceWebhookTest"), None)
    assert topic is not None, "topic serviceWebhookTest hilang dari config.json"
    assert topic.batch is True
    assert json.loads(config_path.read_text(encoding="utf-8"))["external_tasks"][
        "idle_backoff_seconds"
    ] == 60


def test_batch_merge_preserves_all_webhook_log_ids():
    tasks = [
        ExternalTask(
            id=f"task-{index}",
            topic_name="serviceWebhookTest",
            variables={"partId": [2000000 + index], "webhookLogId": f"log-{index}"},
        )
        for index in range(5)
    ]

    merged = ExternalTaskWorker._merge_batch_tasks(tasks)

    assert merged.variables["partId"] == [2000000, 2000001, 2000002, 2000003, 2000004]
    assert merged.variables["webhookLogId"] == [
        "log-0",
        "log-1",
        "log-2",
        "log-3",
        "log-4",
    ]


# ---------------------------------------------------------------------------
# Mode BATCH
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_true_merges_part_ids_into_single_handler_call():
    """2 task dengan partId tumpang-tindih -> handler dipanggil 1x, dedup."""
    calls = []
    handler = _make_handler(calls, batch_capable=True)
    worker = _worker(batch=True, handler=handler)
    await _patch_io(worker, [_task("t1", [1, 2]), _task("t2", [2, 3])])

    outcomes = await worker.poll_once()

    # SATU aksi untuk semua task -> part_id gabungan + dedup.
    assert len(calls) == 1
    assert calls[0] == [1, 2, 3]
    # Tapi SEMUA task tetap di-lapor balik.
    assert len(outcomes) == 2
    assert all(o.action == "completed" for o in outcomes)
    assert worker.complete.await_count == 2


@pytest.mark.asyncio
async def test_batch_true_merges_json_string_and_scalar_part_ids():
    """partId berbentuk teks JSON array dan skalar tetap ter-merge."""
    calls = []
    handler = _make_handler(calls, batch_capable=True)
    worker = _worker(batch=True, handler=handler)
    await _patch_io(
        worker,
        [_task("t1", "[7, 9]"), _task("t2", [9]), _task("t3", 42)],
    )

    await worker.poll_once()

    assert calls == [[7, 9, 42]]


@pytest.mark.asyncio
async def test_batch_false_runs_one_task_at_a_time():
    calls = []
    handler = _make_handler(calls, batch_capable=True)
    worker = _worker(batch=False, handler=handler)
    await _patch_io(worker, [_task("t1", [1, 2]), _task("t2", [2, 3])])

    outcomes = await worker.poll_once()

    # Per-task (perilaku lama): handler 2x, tanpa merge.
    assert calls == [[1, 2], [2, 3]]
    assert len(outcomes) == 2
    assert all(o.action == "completed" for o in outcomes)


@pytest.mark.asyncio
async def test_batch_true_without_batch_capable_handler_falls_back():
    """Handler tanpa batch_capable -> fallback SATU PER SATU (tidak error)."""
    calls = []
    handler = _make_handler(calls, batch_capable=False)
    worker = _worker(batch=True, handler=handler)
    await _patch_io(worker, [_task("t1", [1]), _task("t2", [2])])

    outcomes = await worker.poll_once()

    assert calls == [[1], [2]]
    assert len(outcomes) == 2


@pytest.mark.asyncio
async def test_batch_failure_applies_to_every_task():
    """Handler gagal saat mode batch -> SEMUA task dalam grup dilaporkan gagal."""
    async def handler(task):
        raise RuntimeError("boom")

    handler.batch_capable = True
    worker = _worker(batch=True, handler=handler)
    await _patch_io(worker, [_task("t1", [1]), _task("t2", [2])])

    outcomes = await worker.poll_once()

    assert len(outcomes) == 2
    assert all(o.action == "failed" for o in outcomes)
    assert worker.handle_failure.await_count == 2
    assert worker.complete.await_count == 0
