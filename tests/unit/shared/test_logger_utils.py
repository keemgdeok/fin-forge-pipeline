from shared.utils.logger import extract_correlation_id, get_logger


def test_extract_correlation_id_from_headers():
    event = {"headers": {"x-correlation-id": "cid-123"}}
    assert extract_correlation_id(event) == "cid-123"


def test_extract_correlation_id_from_fields():
    assert extract_correlation_id({"correlation_id": "cid-1"}) == "cid-1"
    assert extract_correlation_id({"CorrelationId": "cid-2"}) == "cid-2"
    assert extract_correlation_id({"request_id": "cid-3"}) == "cid-3"


def test_get_logger_adapter_has_extras():
    log = get_logger(__name__, correlation_id="abc")
    # Ensure the adapter processes extra without raising
    log.info("hello", extra={"foo": "bar"})
