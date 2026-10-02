"""
Paket integrasi Camunda — WEBHOOK marketplace (dari APISIX) → START EVENT,
HIT SIGNAL, dan EXTERNAL SERVICE TASK.

Struktur:
    models.py        DTO hasil trigger (CamundaTriggerResult) & rencana request
    rules.py         loader + matcher + mapper aturan dari camunda/config.json
    signals.py       peta STATUS ORDER → NAMA SIGNAL (bagian `signal` config.json)
    client.py        HTTP client Camunda 7 (engine-rest) & Camunda 8 (Zeebe REST)
    service.py       orkestrasi webhook → Camunda (start/message/signal) + log JSONL
    external_task.py worker service task bertipe External (fetchAndLock → handler)
    task_handlers.py handler bawaan topic external task

Pemakaian dari router webhook:
    from infrastructure.camunda.service import camunda_webhook_trigger
    await camunda_webhook_trigger.trigger(
        marketplace="shopee", signal_code=3, body=body, headers=headers
    )
    await camunda_webhook_trigger.send_signal(
        marketplace="tiktok", order_status="ON_HOLD", body=body
    )
"""

from infrastructure.camunda.client import CamundaClient, CamundaConfigError, normalize_version
from infrastructure.camunda.external_task import (
    ExternalTask,
    ExternalTaskOutcome,
    ExternalTaskResult,
    ExternalTaskTopic,
    ExternalTaskWorker,
    get_worker,
    start_worker,
    stop_worker,
)
from infrastructure.camunda.models import CamundaPlan, CamundaTriggerResult
from infrastructure.camunda.rules import load_rules, render_template, resolve_variables, select_rule
from infrastructure.camunda.service import CamundaWebhookTrigger, camunda_webhook_trigger
from infrastructure.camunda.signals import (
    describe_signal_config,
    render_signal_name,
    resolve_signal_name,
    signal_config,
)

__all__ = [
    "CamundaClient",
    "CamundaConfigError",
    "CamundaPlan",
    "CamundaTriggerResult",
    "CamundaWebhookTrigger",
    "ExternalTask",
    "ExternalTaskOutcome",
    "ExternalTaskResult",
    "ExternalTaskTopic",
    "ExternalTaskWorker",
    "camunda_webhook_trigger",
    "describe_signal_config",
    "get_worker",
    "load_rules",
    "normalize_version",
    "render_signal_name",
    "render_template",
    "resolve_signal_name",
    "resolve_variables",
    "select_rule",
    "signal_config",
    "start_worker",
    "stop_worker",
]
