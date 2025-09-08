import os
import runpy
import pytest

# Test imports follow TDD methodology


def _load_module():
    return runpy.run_path("src/lambda/functions/build_dates/handler.py")


def test_build_dates_happy_path(monkeypatch) -> None:
    mod = _load_module()
    os.environ["MAX_BACKFILL_DAYS"] = "5"
    resp = mod["lambda_handler"]({"date_range": {"start": "2025-09-01", "end": "2025-09-03"}}, None)
    assert resp["dates"] == ["2025-09-01", "2025-09-02", "2025-09-03"]


def test_build_dates_invalid_format(monkeypatch) -> None:
    mod = _load_module()
    resp = mod["lambda_handler"]({"date_range": {"start": "2025/09/01", "end": "2025-09-03"}}, None)
    assert resp["error"]["code"] == "PRE_VALIDATION_FAILED"


def test_build_dates_range_too_long(monkeypatch) -> None:
    mod = _load_module()
    os.environ["MAX_BACKFILL_DAYS"] = "3"
    resp = mod["lambda_handler"]({"date_range": {"start": "2025-09-01", "end": "2025-09-05"}}, None)
    assert resp["error"]["code"] == "PRE_VALIDATION_FAILED"


def test_build_dates_single_day_range() -> None:
    """
    Given: 시작과 종료 날짜가 동일함
    When: build_dates를 실행하면
    Then: 하나의 날짜만 반환해야 함
    """
    mod = _load_module()
    os.environ["MAX_BACKFILL_DAYS"] = "31"

    resp = mod["lambda_handler"]({"date_range": {"start": "2025-09-07", "end": "2025-09-07"}}, None)
    assert resp["dates"] == ["2025-09-07"]
    assert len(resp["dates"]) == 1


def test_build_dates_leap_year_february() -> None:
    """
    Given: 윤년 2월의 날짜 범위
    When: build_dates를 실행하면
    Then: 2월 29일을 포함해야 함
    """
    mod = _load_module()
    os.environ["MAX_BACKFILL_DAYS"] = "31"

    resp = mod["lambda_handler"]({"date_range": {"start": "2024-02-28", "end": "2024-03-01"}}, None)
    expected = ["2024-02-28", "2024-02-29", "2024-03-01"]
    assert resp["dates"] == expected


def test_build_dates_non_leap_year_february() -> None:
    """
    Given: 평년 2월의 날짜 범위
    When: build_dates를 실행하면
    Then: 2월 28일까지만 포함해야 함
    """
    mod = _load_module()
    os.environ["MAX_BACKFILL_DAYS"] = "31"

    resp = mod["lambda_handler"]({"date_range": {"start": "2025-02-28", "end": "2025-03-01"}}, None)
    expected = ["2025-02-28", "2025-03-01"]  # No Feb 29 in 2025
    assert resp["dates"] == expected


def test_build_dates_month_boundary() -> None:
    """
    Given: 월 경계를 넘나드는 날짜 범위
    When: build_dates를 실행하면
    Then: 올바른 날짜 시퀀스를 반환해야 함
    """
    mod = _load_module()
    os.environ["MAX_BACKFILL_DAYS"] = "5"

    resp = mod["lambda_handler"]({"date_range": {"start": "2025-01-30", "end": "2025-02-02"}}, None)
    expected = ["2025-01-30", "2025-01-31", "2025-02-01", "2025-02-02"]
    assert resp["dates"] == expected


def test_build_dates_year_boundary() -> None:
    """
    Given: 연도 경계를 넘나드는 날짜 범위
    When: build_dates를 실행하면
    Then: 올바른 날짜 시퀀스를 반환해야 함
    """
    mod = _load_module()
    os.environ["MAX_BACKFILL_DAYS"] = "5"

    resp = mod["lambda_handler"]({"date_range": {"start": "2024-12-30", "end": "2025-01-02"}}, None)
    expected = ["2024-12-30", "2024-12-31", "2025-01-01", "2025-01-02"]
    assert resp["dates"] == expected


def test_build_dates_missing_start_date() -> None:
    """
    Given: date_range.start가 누락됨
    When: build_dates를 실행하면
    Then: PRE_VALIDATION_FAILED 에러를 반환해야 함
    """
    mod = _load_module()

    resp = mod["lambda_handler"]({"date_range": {"end": "2025-09-07"}}, None)
    assert "error" in resp
    assert resp["error"]["code"] == "PRE_VALIDATION_FAILED"
    assert "date_range.start/end required" in resp["error"]["message"]


def test_build_dates_missing_end_date() -> None:
    """
    Given: date_range.end가 누락됨
    When: build_dates를 실행하면
    Then: PRE_VALIDATION_FAILED 에러를 반환해야 함
    """
    mod = _load_module()

    resp = mod["lambda_handler"]({"date_range": {"start": "2025-09-07"}}, None)
    assert "error" in resp
    assert resp["error"]["code"] == "PRE_VALIDATION_FAILED"
    assert "date_range.start/end required" in resp["error"]["message"]


def test_build_dates_missing_date_range() -> None:
    """
    Given: date_range 자체가 누락됨
    When: build_dates를 실행하면
    Then: PRE_VALIDATION_FAILED 에러를 반환해야 함
    """
    mod = _load_module()

    resp = mod["lambda_handler"]({}, None)
    assert "error" in resp
    assert resp["error"]["code"] == "PRE_VALIDATION_FAILED"
    assert "date_range.start/end required" in resp["error"]["message"]


def test_build_dates_end_before_start() -> None:
    """
    Given: 종료 날짜가 시작 날짜보다 이전임
    When: build_dates를 실행하면
    Then: PRE_VALIDATION_FAILED 에러를 반환해야 함
    """
    mod = _load_module()

    resp = mod["lambda_handler"]({"date_range": {"start": "2025-09-07", "end": "2025-09-05"}}, None)
    assert "error" in resp
    assert resp["error"]["code"] == "PRE_VALIDATION_FAILED"
    assert "start must be <= end" in resp["error"]["message"]


def test_build_dates_max_backfill_days_default() -> None:
    """
    Given: MAX_BACKFILL_DAYS 환경변수가 설정되지 않음
    When: 31일을 초과하는 범위로 build_dates를 실행하면
    Then: 기본값(31일) 제한이 적용되어야 함
    """
    mod = _load_module()
    # Don't set MAX_BACKFILL_DAYS - should default to 31

    # 32 days range
    resp = mod["lambda_handler"]({"date_range": {"start": "2025-09-01", "end": "2025-10-02"}}, None)
    assert "error" in resp
    assert resp["error"]["code"] == "PRE_VALIDATION_FAILED"
    assert "date range too long" in resp["error"]["message"]


def test_build_dates_exactly_at_limit() -> None:
    """
    Given: 정확히 제한 길이의 날짜 범위
    When: build_dates를 실행하면
    Then: 성공적으로 처리해야 함
    """
    mod = _load_module()
    os.environ["MAX_BACKFILL_DAYS"] = "5"

    # Exactly 5 days
    resp = mod["lambda_handler"]({"date_range": {"start": "2025-09-01", "end": "2025-09-05"}}, None)
    assert "dates" in resp
    assert len(resp["dates"]) == 5
    expected = [
        "2025-09-01",
        "2025-09-02",
        "2025-09-03",
        "2025-09-04",
        "2025-09-05",
    ]
    assert resp["dates"] == expected


@pytest.mark.parametrize(
    "invalid_date",
    [
        "2025-13-01",  # Invalid month
        "2025-02-30",  # Invalid day for February
        "2025-04-31",  # Invalid day for April
        "not-a-date",  # Completely invalid
        "2025/09/07",  # Wrong format
        "2025.09.07",  # Wrong format
        "",  # Empty string
    ],
)
def test_build_dates_invalid_date_formats(invalid_date) -> None:
    """
    Given: 다양한 잘못된 날짜 형식들
    When: build_dates를 실행하면
    Then: PRE_VALIDATION_FAILED 에러를 반환해야 함
    """
    mod = _load_module()
    os.environ["MAX_BACKFILL_DAYS"] = "31"

    resp = mod["lambda_handler"]({"date_range": {"start": invalid_date, "end": "2025-09-07"}}, None)
    assert "error" in resp
    assert resp["error"]["code"] == "PRE_VALIDATION_FAILED"
    assert "invalid date format" in resp["error"]["message"]


@pytest.mark.parametrize(
    "max_days,start_date,end_date,expected_valid",
    [
        (1, "2025-09-07", "2025-09-07", True),  # Exactly 1 day
        (1, "2025-09-07", "2025-09-08", False),  # 2 days, exceeds limit
        (7, "2025-09-01", "2025-09-07", True),  # Exactly 7 days
        (7, "2025-09-01", "2025-09-08", False),  # 8 days, exceeds limit
        (31, "2025-01-01", "2025-01-31", True),  # Exactly 31 days
        (31, "2025-01-01", "2025-02-01", False),  # 32 days, exceeds limit
    ],
)
def test_build_dates_max_days_limit_parametrized(max_days, start_date, end_date, expected_valid) -> None:
    """
    Given: 다양한 제한값과 날짜 범위 조합
    When: build_dates를 실행하면
    Then: 제한값에 따른 올바른 결과를 반환해야 함
    """
    mod = _load_module()
    os.environ["MAX_BACKFILL_DAYS"] = str(max_days)

    resp = mod["lambda_handler"]({"date_range": {"start": start_date, "end": end_date}}, None)

    if expected_valid:
        assert "dates" in resp
        assert "error" not in resp
        # Verify the number of days is correct
        expected_length = (mod["_parse_date"](end_date) - mod["_parse_date"](start_date)).days + 1
        assert len(resp["dates"]) == expected_length
    else:
        assert "error" in resp
        assert resp["error"]["code"] == "PRE_VALIDATION_FAILED"
        assert "date range too long" in resp["error"]["message"]


def test_build_dates_parse_date_function_edge_cases() -> None:
    """
    Given: _parse_date 함수의 다양한 입력
    When: 날짜 파싱을 시도하면
    Then: 올바른 결과 또는 예외를 반환해야 함
    """
    mod = _load_module()

    # Valid dates
    assert mod["_parse_date"]("2025-09-07").isoformat() == "2025-09-07"
    assert mod["_parse_date"]("2024-02-29").isoformat() == "2024-02-29"  # Leap year
    assert mod["_parse_date"]("2000-01-01").isoformat() == "2000-01-01"

    # Invalid dates should raise ValueError
    with pytest.raises(ValueError):
        mod["_parse_date"]("2025-02-30")  # Invalid day

    with pytest.raises(ValueError):
        mod["_parse_date"]("2025-13-01")  # Invalid month

    with pytest.raises(Exception):  # Could be ValueError or other parsing error
        mod["_parse_date"]("not-a-date")


def test_build_dates_large_valid_range() -> None:
    """
    Given: 큰 하지만 유효한 날짜 범위
    When: build_dates를 실행하면
    Then: 성공적으로 모든 날짜를 생성해야 함
    """
    mod = _load_module()
    os.environ["MAX_BACKFILL_DAYS"] = "100"  # Allow large range

    resp = mod["lambda_handler"]({"date_range": {"start": "2025-01-01", "end": "2025-01-31"}}, None)
    assert "dates" in resp
    assert len(resp["dates"]) == 31  # January has 31 days
    assert resp["dates"][0] == "2025-01-01"
    assert resp["dates"][-1] == "2025-01-31"

    # Verify all dates are consecutive
    for i in range(1, len(resp["dates"])):
        prev_date = mod["_parse_date"](resp["dates"][i - 1])
        curr_date = mod["_parse_date"](resp["dates"][i])
        assert (curr_date - prev_date).days == 1


def test_build_dates_context_parameter_ignored() -> None:
    """
    Given: Lambda context 파라미터가 제공됨
    When: build_dates를 실행하면
    Then: context를 무시하고 정상 동작해야 함
    """
    mod = _load_module()
    os.environ["MAX_BACKFILL_DAYS"] = "5"

    # Mock context object
    class MockContext:
        function_name = "test-function"
        aws_request_id = "test-request-id"

    resp = mod["lambda_handler"](
        {"date_range": {"start": "2025-09-07", "end": "2025-09-08"}},
        MockContext(),
    )
    assert resp["dates"] == ["2025-09-07", "2025-09-08"]
