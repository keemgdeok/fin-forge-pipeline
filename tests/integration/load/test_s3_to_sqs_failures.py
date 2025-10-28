from __future__ import annotations

import pytest
from moto import mock_aws

from tests.fixtures.load_agent import LoaderError
from tests.integration.load import helpers

pytestmark = [pytest.mark.integration, pytest.mark.load]


@pytest.mark.parametrize(
    "part,exception_factory,expected_code,attempts",
    [
        (
            "part-301.parquet",
            lambda: LoaderError("FILE_NOT_FOUND", "S3 returned 404"),
            "FILE_NOT_FOUND",
            3,
        ),
        (
            "part-302.parquet",
            lambda: ConnectionError("clickhouse unreachable"),
            "CONNECTION_ERROR",
            3,
        ),
        (
            "part-303.parquet",
            lambda: PermissionError("access denied"),
            "PERMISSION_ERROR",
            1,
        ),
        (
            "part-304.parquet",
            lambda: LoaderError("SECRETS_ERROR", "unable to read secret"),
            "SECRETS_ERROR",
            1,
        ),
    ],
)
@mock_aws
def test_exceptions_eventually_move_to_dlq(
    make_load_queues,
    part: str,
    exception_factory,
    expected_code: str,
    attempts: int,
) -> None:
    """
    Given: 특정 예외를 반복 발생시키는 로더 프로세스
    When: 정의된 재시도 횟수만큼 run_once 실행
    Then: 동일한 에러 코드가 기록되고 메시지가 DLQ로 이동해야 함
    """

    def process(_: dict) -> str:
        raise exception_factory()

    dlq_messages = helpers.run_dlq_scenario(
        make_load_queues,
        part=part,
        process=process,
        expected_code=expected_code,
        attempts=attempts,
    )
    assert len(dlq_messages) == 1
