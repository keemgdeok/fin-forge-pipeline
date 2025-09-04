"""Environment settings helpers provided via Common Layer."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class EnvSettings:
    environment: Optional[str]
    raw_bucket: Optional[str]
    curated_bucket: Optional[str]
    error_topic_arn: Optional[str]
    notification_topic_arn: Optional[str]
    source_email: Optional[str]
    admin_emails: List[str]

    @staticmethod
    def load() -> "EnvSettings":
        admin_emails_raw = os.environ.get("ADMIN_EMAILS", "")
        admin_emails = [e.strip() for e in admin_emails_raw.split(";") if e.strip()] if admin_emails_raw else []
        if not admin_emails and "," in admin_emails_raw:
            admin_emails = [e.strip() for e in admin_emails_raw.split(",") if e.strip()]

        return EnvSettings(
            environment=os.environ.get("ENVIRONMENT"),
            raw_bucket=os.environ.get("RAW_BUCKET"),
            curated_bucket=os.environ.get("CURATED_BUCKET"),
            error_topic_arn=os.environ.get("ERROR_TOPIC_ARN"),
            notification_topic_arn=os.environ.get("NOTIFICATION_TOPIC_ARN"),
            source_email=os.environ.get("SOURCE_EMAIL"),
            admin_emails=admin_emails,
        )

