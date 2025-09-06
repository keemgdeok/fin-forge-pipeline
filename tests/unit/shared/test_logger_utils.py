from shared.utils.logger import extract_correlation_id, get_logger


def test_extract_correlation_id_from_headers():
    """
    Given: x-correlation-id 헤더가 포함된 이벤트
    When: 상관관계 ID를 추출하면
    Then: 헤더 값이 반환되어야 함
    """
    event = {"headers": {"x-correlation-id": "cid-123"}}
    assert extract_correlation_id(event) == "cid-123"


def test_extract_correlation_id_from_fields():
    """
    Given: 서로 다른 필드명에 상관관계 ID가 존재
    When: 상관관계 ID를 추출하면
    Then: 각 필드의 값이 우선순위 없이 반환될 수 있어야 함
    """
    assert extract_correlation_id({"correlation_id": "cid-1"}) == "cid-1"
    assert extract_correlation_id({"CorrelationId": "cid-2"}) == "cid-2"
    assert extract_correlation_id({"request_id": "cid-3"}) == "cid-3"


def test_get_logger_adapter_has_extras():
    """
    Given: 상관관계 ID가 설정된 로거
    When: extra 필드를 포함해 로그를 기록하면
    Then: 예외 없이 처리되어야 함
    """
    log = get_logger(__name__, correlation_id="abc")
    # Ensure the adapter processes extra without raising
    log.info("hello", extra={"foo": "bar"})
