import os
import runpy


def _load_module():
    return runpy.run_path("src/lambda/functions/ingestion_orchestrator/handler.py")


class _SQS:
    def __init__(self):
        self.batches = []

    def send_message_batch(self, **kwargs):
        # Record batch entries
        self.batches.append(kwargs["Entries"])
        # Simulate SQS response
        return {"Successful": [{"Id": e["Id"]} for e in kwargs["Entries"]]}


class _Boto:
    def __init__(self, sqs):
        self._sqs = sqs

    def client(self, name: str):
        if name == "sqs":
            return self._sqs
        raise ValueError(name)


def test_orchestrator_batches_messages(monkeypatch):
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["QUEUE_URL"] = "https://sqs.example/queue"
    os.environ["CHUNK_SIZE"] = "5"
    os.environ["SQS_SEND_BATCH_SIZE"] = "10"

    mod = _load_module()
    sqs = _SQS()
    monkeypatch.setitem(mod["main"].__globals__, "boto3", _Boto(sqs))

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
    assert len(sqs.batches) == 1
    assert sum(len(b) for b in sqs.batches) == 5
