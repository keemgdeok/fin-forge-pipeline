"""Data ingestion Lambda function handler - delegate to shared service."""

from typing import Any, Dict
from shared.ingestion.service import process_event


def main(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    return process_event(event, context)
