"""Contracts and helpers for Load pipeline (Pull model).

Implements message schema validation, S3 key parsing, and EventBridge → SQS
transform utilities according to docs/specs/load specifications.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import re
from typing import Any, Dict, Iterable, Mapping, Optional
from uuid import UUID, uuid4


class ValidationError(ValueError):
    """Raised when payload validation fails."""


BUCKET_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
DOMAIN_PATTERN = re.compile(r"^[a-z0-9-]{1,50}$")
TABLE_PATTERN = re.compile(r"^[a-z0-9_]{1,50}$")
INTERVAL_VALUE_PATTERN = re.compile(r"^[a-z0-9_-]{1,50}$")
DATA_SOURCE_VALUE_PATTERN = re.compile(r"^[a-z0-9_-]{1,100}$")
LAYER_VALUE_PATTERN = re.compile(r"^[a-z0-9_-]{1,50}$")
YEAR_VALUE_PATTERN = re.compile(r"^\d{4}$")
MONTH_VALUE_PATTERN = re.compile(r"^(0[1-9]|1[0-2])$")
DAY_VALUE_PATTERN = re.compile(r"^(0[1-9]|[12]\d|3[01])$")
DS_VALUE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

INTERVAL_SEGMENT_PATTERN = re.compile(r"^interval=([a-z0-9_-]{1,50})$")
DATA_SOURCE_SEGMENT_PATTERN = re.compile(r"^data_source=([a-z0-9_-]{1,100})$")
YEAR_SEGMENT_PATTERN = re.compile(r"^year=(\d{4})$")
MONTH_SEGMENT_PATTERN = re.compile(r"^month=(0[1-9]|1[0-2])$")
DAY_SEGMENT_PATTERN = re.compile(r"^day=(0[1-9]|[12]\d|3[01])$")
LAYER_SEGMENT_PATTERN = re.compile(r"^layer=([a-z0-9_-]{1,50})$")

QUEUE_URL_PATTERN = re.compile(r"^https://sqs\.[a-z0-9-]+\.amazonaws\.com/\d{12}/[a-z0-9-]+$")

_PRIORITY_BY_DOMAIN = {
    "market": "1",  # High priority
    "daily-prices-data": "2",  # Medium priority
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
class ParsedCuratedKey:
    domain: str
    table_name: str
    interval: str
    data_source: Optional[str]
    year: str
    month: str
    day: str
    layer: str

    @property
    def ds(self) -> str:
        return f"{self.year}-{self.month}-{self.day}"


@dataclass(frozen=True)
class LoadMessage:
    bucket: str
    key: str
    domain: str
    table_name: str
    interval: str
    layer: str
    year: str
    month: str
    day: str
    ds: str
    correlation_id: str
    data_source: Optional[str] = None
    file_size: Optional[int] = None
    presigned_url: Optional[str] = None

    def __post_init__(self) -> None:  # type: ignore[override]
        self._validate_bucket()
        parsed = parse_curated_key(self.key)
        if self.domain != parsed.domain:
            raise ValidationError("Domain does not match S3 key")
        if self.table_name != parsed.table_name:
            raise ValidationError("Table name does not match S3 key")
        if self.interval != parsed.interval:
            raise ValidationError("Interval does not match S3 key")
        if self.layer != parsed.layer:
            raise ValidationError("Layer does not match S3 key")
        if self.year != parsed.year:
            raise ValidationError("Year does not match S3 key")
        if self.month != parsed.month:
            raise ValidationError("Month does not match S3 key")
        if self.day != parsed.day:
            raise ValidationError("Day does not match S3 key")
        if self.data_source != parsed.data_source:
            raise ValidationError("Data source does not match S3 key")
        if self.ds != parsed.ds:
            raise ValidationError("ds does not match S3 key")

        self._validate_fields()
        if not DOMAIN_PATTERN.fullmatch(self.domain):
            raise ValidationError("Invalid domain format")
        if not TABLE_PATTERN.fullmatch(self.table_name):
            raise ValidationError("Invalid table_name format")

        self._validate_correlation()
        self._validate_file_size()
        self._validate_presigned_url()

    def _validate_bucket(self) -> None:
        if not BUCKET_PATTERN.fullmatch(self.bucket):
            raise ValidationError("Invalid S3 bucket format")

    def _validate_fields(self) -> None:
        if not isinstance(self.interval, str):
            raise ValidationError("interval must be a string")
        if not INTERVAL_VALUE_PATTERN.fullmatch(self.interval):
            raise ValidationError("Invalid interval format")
        if self.data_source is not None:
            if not isinstance(self.data_source, str):
                raise ValidationError("data_source must be a string")
            if not DATA_SOURCE_VALUE_PATTERN.fullmatch(self.data_source):
                raise ValidationError("Invalid data_source format")
        if not isinstance(self.year, str):
            raise ValidationError("year must be a string")
        if not YEAR_VALUE_PATTERN.fullmatch(self.year):
            raise ValidationError("Invalid year format")
        if not isinstance(self.month, str):
            raise ValidationError("month must be a string")
        if not MONTH_VALUE_PATTERN.fullmatch(self.month):
            raise ValidationError("Invalid month format")
        if not isinstance(self.day, str):
            raise ValidationError("day must be a string")
        if not DAY_VALUE_PATTERN.fullmatch(self.day):
            raise ValidationError("Invalid day format")
        if not isinstance(self.layer, str):
            raise ValidationError("layer must be a string")
        if not LAYER_VALUE_PATTERN.fullmatch(self.layer):
            raise ValidationError("Invalid layer format")
        if not isinstance(self.ds, str):
            raise ValidationError("ds must be a string")
        if not DS_VALUE_PATTERN.fullmatch(self.ds):
            raise ValidationError("Invalid ds format")

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


def _extract_value(segment: str, pattern: re.Pattern[str], error: str) -> str:
    match = pattern.match(segment)
    if not match:
        raise ValidationError(error)
    return match.group(1)


def parse_curated_key(key: str) -> ParsedCuratedKey:
    if not isinstance(key, str) or not key:
        raise ValidationError("key must be a non-empty string")

    parts = key.split("/")
    if len(parts) < 8:
        raise ValidationError("Invalid curated key format")

    domain, table = parts[0], parts[1]

    if not DOMAIN_PATTERN.fullmatch(domain):
        raise ValidationError("Invalid domain in key")
    if not TABLE_PATTERN.fullmatch(table):
        raise ValidationError("Invalid table in key")

    interval = _extract_value(parts[2], INTERVAL_SEGMENT_PATTERN, "Invalid interval segment in key")

    index = 3
    data_source: Optional[str] = None
    if index < len(parts) - 4 and parts[index].startswith("data_source="):
        data_source = _extract_value(parts[index], DATA_SOURCE_SEGMENT_PATTERN, "Invalid data_source segment in key")
        index += 1

    try:
        year_segment = parts[index]
        month_segment = parts[index + 1]
        day_segment = parts[index + 2]
        layer_segment = parts[index + 3]
    except IndexError as exc:
        raise ValidationError("Incomplete curated key partitions") from exc

    year = _extract_value(year_segment, YEAR_SEGMENT_PATTERN, "Invalid year segment in key")
    month = _extract_value(month_segment, MONTH_SEGMENT_PATTERN, "Invalid month segment in key")
    day = _extract_value(day_segment, DAY_SEGMENT_PATTERN, "Invalid day segment in key")
    layer = _extract_value(layer_segment, LAYER_SEGMENT_PATTERN, "Invalid layer segment in key")

    if not parts[-1].endswith(".parquet"):
        raise ValidationError("Load objects must be Parquet files")

    return ParsedCuratedKey(
        domain=domain,
        table_name=table,
        interval=interval,
        data_source=data_source,
        year=year,
        month=month,
        day=day,
        layer=layer,
    )


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

    parsed = parse_curated_key(key)

    message = LoadMessage(
        bucket=bucket,
        key=key,
        domain=parsed.domain,
        table_name=parsed.table_name,
        interval=parsed.interval,
        layer=parsed.layer,
        year=parsed.year,
        month=parsed.month,
        day=parsed.day,
        ds=parsed.ds,
        correlation_id=str(uuid4()),
        data_source=parsed.data_source,
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
