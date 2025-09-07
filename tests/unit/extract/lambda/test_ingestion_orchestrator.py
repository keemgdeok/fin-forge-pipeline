import os
import runpy
from tests.fixtures.clients import SQSStub, BotoStub


def test_orchestrator_batches_messages(monkeypatch) -> None:
    """
    Given: 23개의 심볼, chunk_size=5, batch_size=10 환경
    When: 오케스트레이터를 실행하면
    Then: 총 5개의 청크 메시지가 1회 배치 전송으로 발행되어야 함
    """
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["QUEUE_URL"] = "https://sqs.example/queue"
    os.environ["CHUNK_SIZE"] = "5"
    os.environ["SQS_SEND_BATCH_SIZE"] = "10"

    mod = runpy.run_path("src/lambda/functions/ingestion_orchestrator/handler.py")
    sqs = SQSStub()
    monkeypatch.setitem(mod["main"].__globals__, "boto3", BotoStub(sqs=sqs))

    # 23 symbols -> chunks of 5 => 5 chunk messages
    # send_batch_size=10 => entries are flushed once at end (1 batch)
    event = {
        "symbols": [f"SYM{i}" for i in range(23)],
        "domain": "market",
        "table_name": "prices",
        "period": "1mo",
        "interval": "1d",
        "file_format": "json",
    }

    resp = mod["main"](event, None)
    assert resp["published"] == 5  # 5 chunk messages
    assert resp["chunks"] == 5
    assert len(sqs.batches) == 1
    assert sum(len(b) for b in sqs.batches) == 5
