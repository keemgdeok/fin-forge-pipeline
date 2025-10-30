from shared.utils.logger import extract_correlation_id, get_logger


def test_extract_correlation_id_from_headers() -> None:
    """
    Given: x-correlation-id 헤더가 포함된 이벤트
    When: 상관관계 ID 추출
    Then: 헤더 값 반환
    """
    event = {"headers": {"x-correlation-id": "cid-123"}}
    assert extract_correlation_id(event) == "cid-123"


def test_extract_correlation_id_from_fields() -> None:
    """
    Given: 서로 다른 필드명에 상관관계 ID가 존재
    When: 상관관계 ID 추출
    Then: 각 필드 값 반환
    """
    assert extract_correlation_id({"correlation_id": "cid-1"}) == "cid-1"
    assert extract_correlation_id({"CorrelationId": "cid-2"}) == "cid-2"
    assert extract_correlation_id({"request_id": "cid-3"}) == "cid-3"


def test_get_logger_adapter_has_extras() -> None:
    """
    Given: 상관관계 ID가 설정된 로거
    When: extra 포함 로그 기록
    Then: 예외 없이 처리
    """
    log = get_logger(__name__, correlation_id="abc")
    # Ensure the adapter processes extra without raising
    log.info("hello", extra={"foo": "bar"})
