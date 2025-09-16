"""Contracts and helpers for Load pipeline (Pull model).

Implements message schema validation, S3 key parsing, and EventBridge → SQS
transform utilities according to docs/specs/load specifications.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import re
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple
from uuid import UUID, uuid4


class ValidationError(ValueError):
    """Raised when payload validation fails."""


BUCKET_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
DOMAIN_PATTERN = re.compile(r"^[a-z0-9-]{1,50}$")
TABLE_PATTERN = re.compile(r"^[a-z0-9_]{1,50}$")
PARTITION_PATTERN = re.compile(r"^ds=\d{4}-\d{2}-\d{2}$")
QUEUE_URL_PATTERN = re.compile(r"^https://sqs\.[a-z0-9-]+\.amazonaws\.com/\d{12}/[a-z0-9-]+$")

_PRIORITY_BY_DOMAIN = {
    "market": "1",  # High priority
    "customer": "2",  # Medium priority
    "product": "3",  # Low priority
    "analytics": "3",  # Low priority
}


class LoadErrorCodes:
    PARSE_ERROR = "PARSE_ERROR"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    FILE_NOT_FOUND = "FILE_NOT_FOUND"
    CONNECTION_ERROR = "CONNECTION_ERROR"
    TIMEOUT_ERROR = "TIMEOUT_ERROR"
    MEMORY_ERROR = "MEMORY_ERROR"
    PERMISSION_ERROR = "PERMISSION_ERROR"
    SECRETS_ERROR = "SECRETS_ERROR"


@dataclass(frozen=True)
class LoadMessage:
    bucket: str
    key: str
    domain: str
    table_name: str
    partition: str
    correlation_id: str
    file_size: Optional[int] = None
    presigned_url: Optional[str] = None

    def __post_init__(self) -> None:  # type: ignore[override]
        self._validate_bucket()
        parsed_domain, parsed_table, parsed_partition = parse_curated_key(self.key)
        if self.domain != parsed_domain:
            raise ValidationError("Domain does not match S3 key")
        if self.table_name != parsed_table:
            raise ValidationError("Table name does not match S3 key")
        if self.partition != parsed_partition:
            raise ValidationError("Partition does not match S3 key")

        if not DOMAIN_PATTERN.fullmatch(self.domain):
            raise ValidationError("Invalid domain format")
        if not TABLE_PATTERN.fullmatch(self.table_name):
            raise ValidationError("Invalid table_name format")
        if not PARTITION_PATTERN.fullmatch(self.partition):
            raise ValidationError("Invalid partition format")

        self._validate_correlation()
        self._validate_file_size()
        self._validate_presigned_url()

    def _validate_bucket(self) -> None:
        if not BUCKET_PATTERN.fullmatch(self.bucket):
            raise ValidationError("Invalid S3 bucket format")

    def _validate_correlation(self) -> None:
        try:
            uuid = UUID(self.correlation_id, version=4)
        except (ValueError, AttributeError) as exc:
            raise ValidationError("Invalid correlation_id") from exc
        if uuid.version != 4:
            raise ValidationError("correlation_id must be UUID v4")

    def _validate_file_size(self) -> None:
        if self.file_size is not None:
            if not isinstance(self.file_size, int):
                raise ValidationError("file_size must be an integer")
            if self.file_size <= 0:
                raise ValidationError("file_size must be positive")

    def _validate_presigned_url(self) -> None:
        if self.presigned_url is None:
            return
        if not isinstance(self.presigned_url, str) or not self.presigned_url.startswith("https://"):
            raise ValidationError("presigned_url must be https")

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        return {k: v for k, v in payload.items() if v is not None}


def parse_curated_key(key: str) -> Tuple[str, str, str]:
    if not isinstance(key, str) or not key:
        raise ValidationError("key must be a non-empty string")

    parts = key.split("/")
    if len(parts) < 4:
        raise ValidationError("Invalid curated key format")

    domain, table, partition = parts[0], parts[1], parts[2]

    if not DOMAIN_PATTERN.fullmatch(domain):
        raise ValidationError("Invalid domain in key")
    if not TABLE_PATTERN.fullmatch(table):
        raise ValidationError("Invalid table in key")
    if not PARTITION_PATTERN.fullmatch(partition):
        raise ValidationError("Invalid partition in key")

    if not parts[-1].endswith(".parquet"):
        raise ValidationError("Load objects must be Parquet files")

    return domain, table, partition


def transform_s3_event_to_message(event: Mapping[str, Any]) -> Dict[str, Any]:
    try:
        source = event["source"]
        detail_type = event["detail-type"]
        detail = event["detail"]
        bucket = detail["bucket"]["name"]
        obj = detail["object"]
        key = obj["key"]
        size = obj.get("size")
    except (KeyError, TypeError) as exc:
        raise ValidationError("Invalid S3 event payload") from exc

    if source != "aws.s3" or detail_type != "Object Created":
        raise ValidationError("Unsupported event type")

    if not isinstance(size, int) or size < 1024:
        raise ValidationError("Object size below minimum threshold")

    domain, table, partition = parse_curated_key(key)

    message = LoadMessage(
        bucket=bucket,
        key=key,
        domain=domain,
        table_name=table,
        partition=partition,
        correlation_id=str(uuid4()),
        file_size=size,
    )

    return message.to_dict()


@dataclass(frozen=True)
class LoaderConfig:
    queue_url: str
    wait_time_seconds: int = 20
    max_messages: int = 10
    visibility_timeout: int = 1800
    query_timeout: int = 300
    backoff_seconds: Iterable[int] | None = None

    def __post_init__(self) -> None:  # type: ignore[override]
        if not isinstance(self.queue_url, str) or not self.queue_url:
            raise ValidationError("queue_url is required")
        if not QUEUE_URL_PATTERN.match(self.queue_url):
            raise ValidationError("queue_url must be an SQS HTTPS endpoint")

        if self.wait_time_seconds <= 0 or self.wait_time_seconds > 20:
            raise ValidationError("wait_time_seconds must be between 1 and 20")

        if self.max_messages < 1 or self.max_messages > 10:
            raise ValidationError("max_messages must be 1-10")

        if self.query_timeout <= 0:
            raise ValidationError("query_timeout must be positive")

        if self.visibility_timeout < self.query_timeout * 6:
            raise ValidationError("visibility_timeout must be at least 6x query_timeout")

        backoff = list(self.backoff_seconds) if self.backoff_seconds is not None else [2, 4, 8]
        if not backoff:
            raise ValidationError("backoff_seconds must not be empty")
        if not all(isinstance(v, int) and v > 0 for v in backoff):
            raise ValidationError("backoff_seconds must be positive integers")

        object.__setattr__(self, "backoff_seconds", tuple(backoff))


def build_message_attributes(message: LoadMessage) -> Dict[str, str]:
    priority = _PRIORITY_BY_DOMAIN.get(message.domain, "3")
    return {
        "ContentType": "application/json",
        "Domain": message.domain,
        "TableName": message.table_name,
        "Priority": priority,
    }
