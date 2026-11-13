"""Bounded operational logs and temporary A2A results, not a message archive."""
import asyncio
from datetime import datetime, timedelta, timezone
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import time

from a2a.server.tasks import DatabaseTaskStore
from a2a.types import Task, TaskState
from sqlalchemy import delete, or_, select

from a2a_state import Limits

TERMINAL = {TaskState.TASK_STATE_COMPLETED, TaskState.TASK_STATE_FAILED,
            TaskState.TASK_STATE_CANCELED, TaskState.TASK_STATE_REJECTED}
TERMINAL_NAMES = [TaskState.Name(v) for v in TERMINAL] + ["completed", "failed", "canceled", "rejected"]
CACHED_STATES = TERMINAL_NAMES + [TaskState.Name(TaskState.TASK_STATE_INPUT_REQUIRED),
                                  TaskState.Name(TaskState.TASK_STATE_AUTH_REQUIRED), "input_required", "auth_required"]
log = logging.getLogger("ai_collaboration")


class MetadataFormatter(logging.Formatter):
    def format(self, record):
        # Third-party exceptions can contain request bodies or HTTP credentials.
        # Keep event names, IDs and exception classes, never arbitrary log text.
        stamp = datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="seconds")
        fields = [stamp, record.levelname, record.name]
        fields.append(str(getattr(record, "event", "library_event")))
        for key in ("task_id", "error_type", "removed"):
            if hasattr(record, key):
                fields.append(key + "=" + str(getattr(record, key)))
        if record.exc_info:
            fields.append("exception=" + record.exc_info[0].__name__)
        return " ".join(fields)


def configure_logging(root):
    path = Path(root) / "runtime/a2a.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(path, maxBytes=1024*1024, backupCount=2, encoding="utf8")
    handler.setFormatter(MetadataFormatter())
    logger = logging.getLogger()
    for old in list(logger.handlers):
        logger.removeHandler(old)
        old.close()
    logger.setLevel(logging.WARNING)
    logger.addHandler(handler)
    log.setLevel(logging.INFO)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        child = logging.getLogger(name)
        child.handlers.clear()
        child.propagate = True
    return handler


class TemporaryTaskStore(DatabaseTaskStore):
    def __init__(self, *args, mailbox, **kwargs):
        super().__init__(*args, **kwargs)
        self.mailbox = mailbox

    async def save(self, task, context):
        temporary = Task()
        temporary.CopyFrom(task)
        temporary.ClearField("history")
        temporary.ClearField("metadata")
        record = self.mailbox.row(task.id)
        if record and record.get("mode") == "message":
            receipt = self.mailbox.receipt_for(record["owner"], record["message_id"])
            temporary.metadata.update({"collaboration_mode": "message", "collaboration_receipt": receipt})
        if not temporary.status.HasField("timestamp"):
            temporary.status.timestamp.FromDatetime(datetime.now(timezone.utc))
        await super().save(temporary, context)
        if task.status.state in TERMINAL:
            self.mailbox.seal(task.id, "done" if task.status.state == TaskState.TASK_STATE_COMPLETED else "failed")


async def cleanup_once(engine, mailbox, store, now=None):
    now = time.time() if now is None else now
    limits = mailbox.limits
    model = store.task_model
    cutoff = datetime.fromtimestamp(now, timezone.utc).replace(tzinfo=None) - timedelta(seconds=limits.result_ttl_seconds)
    terminal = model.status["state"].as_string().in_(CACHED_STATES)
    async with engine.begin() as connection:
        expired = list((await connection.execute(select(model.id).where(
            terminal, or_(model.last_updated < cutoff, model.last_updated.is_(None)),
        ))).scalars())
        excess = list((await connection.execute(select(model.id).where(terminal)
            .order_by(model.last_updated.desc(), model.id.desc())
            .offset(int(limits.max_completed_tasks)))).scalars())
        removed = set(expired) | set(excess)
        if removed:
            await connection.execute(delete(model).where(model.id.in_(removed)))
    await asyncio.to_thread(mailbox.cleanup, now, removed)
    if removed:
        log.info("cleanup", extra={"event": "results_removed", "removed": len(removed)})
    return len(removed)


async def cleanup_loop(cleaner, interval):
    while True:
        await asyncio.sleep(interval)
        try:
            await cleaner()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            log.warning("cleanup", extra={"event": "cleanup_failed", "error_type": type(error).__name__})
