"""Simple on-prem Loader agent simulator for tests.

Simulates polling SQS, per-message processing, ACK(DeleteMessage) and
defer/retry via ChangeMessageVisibility.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import boto3


ProcessFn = Callable[[Dict[str, Any]], str]


@dataclass
class LoaderConfig:
    queue_url: str
    wait_time_seconds: int = 1  # kept small for tests
    max_messages: int = 10
    visibility_timeout: int = 30
    backoff_seconds: List[int] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:  # type: ignore[override]
        if self.backoff_seconds is None:
            # Spec examples: 2s, 4s, 8s (kept small for tests)
            object.__setattr__(self, "backoff_seconds", [1, 2, 4])


class FakeLoaderAgent:
    """A minimal loader that exercises SQS semantics for tests."""

    def __init__(self, config: LoaderConfig, *, region: Optional[str] = None) -> None:
        self.config = config
        self.client = boto3.client("sqs", region_name=region or os.environ.get("AWS_REGION", "us-east-1"))

    def run_once(self, process: ProcessFn) -> Dict[str, Any]:
        """Poll once and process up to `max_messages` with individual ACKs.

        The `process` callback returns one of: "SUCCESS", "RETRY", "FAIL".
        """
        resp = self.client.receive_message(
            QueueUrl=self.config.queue_url,
            MaxNumberOfMessages=self.config.max_messages,
            WaitTimeSeconds=self.config.wait_time_seconds,
            AttributeNames=["ApproximateReceiveCount"],
        )
        messages: List[Dict[str, Any]] = resp.get("Messages", [])
        results: List[Dict[str, Any]] = []

        for m in messages:
            body_raw = m.get("Body", "{}")
            body: Dict[str, Any] = json.loads(body_raw) if isinstance(body_raw, str) else body_raw  # type: ignore[assignment]
            action = process(body)
            if action == "SUCCESS":
                self.client.delete_message(QueueUrl=self.config.queue_url, ReceiptHandle=m["ReceiptHandle"])  # type: ignore[index]
                results.append({"action": action, "message": body})
            elif action == "RETRY":
                # Reduce visibility to 0 to speed re-delivery in tests
                self.client.change_message_visibility(
                    QueueUrl=self.config.queue_url,
                    ReceiptHandle=m["ReceiptHandle"],  # type: ignore[index]
                    VisibilityTimeout=0,
                )
                results.append({"action": action, "message": body})
            else:  # FAIL (no ACK)
                results.append({"action": action, "message": body})

        return {"count": len(results), "results": results}
