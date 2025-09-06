import os
import json
from datetime import datetime, timezone

from moto import mock_aws
import boto3


 


def _receive_all_sqs_messages(queue_url: str):
    sqs_client = boto3.client("sqs", region_name=os.environ.get("AWS_REGION", "us-east-1"))
    records = []
    while True:
        resp = sqs_client.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=0)
        msgs = resp.get("Messages", [])
        if not msgs:
            break
        for m in msgs:
            records.append({"messageId": m["MessageId"], "body": m["Body"]})
    return {"Records": records}


@mock_aws
def test_orchestrator_symbols_from_ssm(monkeypatch, orchestrator_env, make_queue, load_module):
    # Set default env
    # Env defaults
    os.environ["ENVIRONMENT"] = "dev"

    # moto SQS
    queue_url = make_queue("extract-e2e-ssm")
    orchestrator_env(queue_url, chunk_size=3, batch_size=10)

    # moto SSM: create parameter with symbols
    ssm = boto3.client("ssm", region_name="us-east-1")
    param_name = "/ingestion/symbols"
    ssm.put_parameter(Name=param_name, Value='["AAPL","MSFT","GOOG","AMZN"]', Type="String")
    os.environ["SYMBOLS_SSM_PARAM"] = param_name
    # Clear S3 config to force SSM path
    os.environ.pop("SYMBOLS_S3_BUCKET", None)
    os.environ.pop("SYMBOLS_S3_KEY", None)

    # Orchestrator should read SSM and publish ceil(4/3)=2 messages
    event = {
        "symbols": [],
        "domain": "market",
        "table_name": "prices",
        "period": "1mo",
        "interval": "1d",
        "file_format": "json",
    }
    mod_orc = load_module("src/lambda/functions/ingestion_orchestrator/handler.py")
    resp = mod_orc["main"](event, None)
    assert resp["published"] == 2

    # Read messages and validate symbols coverage equals SSM param
    records = _receive_all_sqs_messages(queue_url)["Records"]
    all_syms = set()
    for r in records:
        body = json.loads(r["body"]) if isinstance(r["body"], str) else r["body"]
        all_syms.update(body.get("symbols", []))
    assert all_syms == {"AAPL", "MSFT", "GOOG", "AMZN"}


@mock_aws
def test_orchestrator_symbols_from_s3(monkeypatch, orchestrator_env, make_queue, make_bucket, load_module):
    # Env
    os.environ["ENVIRONMENT"] = "dev"

    # moto SQS
    queue_url = make_queue("extract-e2e-s3")
    orchestrator_env(queue_url, chunk_size=3, batch_size=10)

    # moto S3: upload symbol universe as newline-separated text
    bucket = make_bucket("config-bucket")
    key = "symbols/universe.txt"
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.put_object(Bucket=bucket, Key=key, Body=b"AAPL\nMSFT\nGOOG\nNVDA\nAMZN\n")
    os.environ["SYMBOLS_S3_BUCKET"] = bucket
    os.environ["SYMBOLS_S3_KEY"] = key
    # Ensure SSM path not used
    os.environ.pop("SYMBOLS_SSM_PARAM", None)

    # Orchestrator should read S3 object and publish ceil(5/3)=2 messages
    event = {
        "symbols": [],
        "domain": "market",
        "table_name": "prices",
        "period": "1mo",
        "interval": "1d",
        "file_format": "json",
    }
    mod_orc = load_module("src/lambda/functions/ingestion_orchestrator/handler.py")
    resp = mod_orc["main"](event, None)
    assert resp["published"] == 2

    # Validate union of symbols equals file contents
    records = _receive_all_sqs_messages(queue_url)["Records"]
    all_syms = set()
    for r in records:
        body = json.loads(r["body"]) if isinstance(r["body"], str) else r["body"]
        all_syms.update(body.get("symbols", []))
    assert all_syms == {"AAPL", "MSFT", "GOOG", "NVDA", "AMZN"}


@mock_aws
def test_dlq_redrive_on_worker_failures(monkeypatch, make_queue, load_module):
    # Create DLQ
    sqs = boto3.client("sqs", region_name="us-east-1")
    dlq_url = make_queue("extract-dlq")
    dlq_attrs = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])  # get ARN
    dlq_arn = dlq_attrs["Attributes"]["QueueArn"]

    # Create main queue with redrive policy and small visibility timeout
    main_url = make_queue("extract-main")
    redrive = json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "2"})
    sqs.set_queue_attributes(QueueUrl=main_url, Attributes={"RedrivePolicy": redrive, "VisibilityTimeout": "0"})

    # Put one valid ingestion message
    msg_body = json.dumps(
        {
            "symbols": ["AAPL"],
            "domain": "market",
            "table_name": "prices",
            "period": "1mo",
            "interval": "1d",
            "file_format": "json",
        }
    )
    sqs.send_message(QueueUrl=main_url, MessageBody=msg_body)

    # Prepare Worker that always fails
    os.environ["ENVIRONMENT"] = "dev"
    os.environ["RAW_BUCKET"] = "raw-bucket-dev"  # not used since we force failure
    os.environ["ENABLE_GZIP"] = "false"
    mod_wrk = load_module("src/lambda/functions/ingestion_worker/handler.py")

    def _fail(*args, **kwargs):
        raise Exception("forced failure")

    # Patch worker to fail processing
    monkeypatch.setitem(mod_wrk["main"].__globals__, "process_event", _fail)

    # Receive and process (without deletes) 3 times so that receiveCount exceeds maxReceiveCount=2
    for _ in range(3):
        resp = sqs.receive_message(QueueUrl=main_url, MaxNumberOfMessages=1, WaitTimeSeconds=0)
        msgs = resp.get("Messages", [])
        if not msgs:
            break
        sqs_event = {"Records": [{"messageId": msgs[0]["MessageId"], "body": msgs[0]["Body"]}]}
        try:
            _ = mod_wrk["main"](sqs_event, None)
        except Exception:
            # Our worker catches exceptions and returns batchItemFailures,
            # but in case it bubbles up in test, ignore.
            pass
        # No delete: message should reappear immediately due to VisibilityTimeout=0

    # DLQ should have the message now
    dlq_msgs = sqs.receive_message(QueueUrl=dlq_url, MaxNumberOfMessages=10, WaitTimeSeconds=0).get("Messages", [])
    assert len(dlq_msgs) >= 1


@mock_aws
def test_e2e_basic_flow(monkeypatch, orchestrator_env, worker_env, yf_stub, make_queue, make_bucket, load_module):
    # Orchestrator environment
    os.environ["ENVIRONMENT"] = "dev"
    # moto SQS queue
    queue_url = make_queue("extract-e2e-queue")
    orchestrator_env(queue_url, chunk_size=2, batch_size=10)
    # moto S3 bucket
    bucket = make_bucket("raw-bucket-dev")
    s3 = boto3.client("s3", region_name="us-east-1")

    event = {
        "symbols": ["AAPL", "MSFT", "GOOG"],
        "domain": "market",
        "table_name": "prices",
        "period": "1mo",
        "interval": "1d",
        "file_format": "json",
    }

    mod_orc = load_module("src/lambda/functions/ingestion_orchestrator/handler.py")
    resp = mod_orc["main"](event, None)
    assert resp["published"] == 2

    # Worker environment
    worker_env(bucket, enable_gzip=False)
    mod_wrk = load_module("src/lambda/functions/ingestion_worker/handler.py")
    # Patch only YahooFinance (AWS to moto)
    yf_stub(["AAPL", "MSFT", "GOOG"])

    sqs_event = _receive_all_sqs_messages(queue_url)
    wr = mod_wrk["main"](sqs_event, None)
    assert wr == {"batchItemFailures": []}
    # 각 심볼의 prefix별 정확한 객체 수 검증(심볼당 1개)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for sym in ["AAPL", "MSFT", "GOOG"]:
        prefix = (
            f"market/prices/ingestion_date={today}/data_source=yahoo_finance/"
            f"symbol={sym}/interval=1d/period=1mo/"
        )
        listed_sym = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        assert int(listed_sym.get("KeyCount", 0)) == 1


@mock_aws
def test_e2e_gzip(monkeypatch, orchestrator_env, worker_env, yf_stub, make_queue, make_bucket, load_module):
    # Orchestrator prepares one symbol message
    os.environ["ENVIRONMENT"] = "dev"
    queue_url = make_queue("extract-e2e-queue-gzip")
    orchestrator_env(queue_url, chunk_size=1, batch_size=10)

    mod_orc = load_module("src/lambda/functions/ingestion_orchestrator/handler.py")

    event = {
        "symbols": ["AAPL"],
        "domain": "market",
        "table_name": "prices",
        "period": "1mo",
        "interval": "1d",
        "file_format": "json",
    }
    mod_orc["main"](event, None)

    # Worker with gzip enabled
    bucket = make_bucket("raw-bucket-dev")
    s3 = boto3.client("s3", region_name="us-east-1")
    worker_env(bucket, enable_gzip=True)
    mod_wrk = load_module("src/lambda/functions/ingestion_worker/handler.py")
    yf_stub(["AAPL"])

    sqs_event = _receive_all_sqs_messages(queue_url)
    wr = mod_wrk["main"](sqs_event, None)
    assert wr == {"batchItemFailures": []}
    listed = s3.list_objects_v2(Bucket=bucket, Prefix="market/prices/")
    assert int(listed.get("KeyCount", 0)) == 1
    key = listed["Contents"][0]["Key"]
    assert key.endswith(".gz")
    head = s3.head_object(Bucket=bucket, Key=key)
    assert head.get("ContentEncoding") == "gzip"


@mock_aws
def test_e2e_partial_batch_failure(monkeypatch, worker_env, yf_stub, make_queue, make_bucket, load_module):
    # Create queue with valid and invalid message
    queue_url = make_queue("extract-e2e-partial")
    sqs = boto3.client("sqs", region_name=os.environ.get("AWS_REGION", "us-east-1"))
    valid_body = json.dumps(
        {
            "symbols": ["AAPL"],
            "domain": "market",
            "table_name": "prices",
            "period": "1mo",
            "interval": "1d",
            "file_format": "json",
        }
    )
    sqs.send_message(QueueUrl=queue_url, MessageBody=valid_body)
    sqs.send_message(QueueUrl=queue_url, MessageBody="{not-json}")

    os.environ["ENVIRONMENT"] = "dev"
    bucket = make_bucket("raw-bucket-dev")
    s3 = boto3.client("s3", region_name="us-east-1")
    worker_env(bucket, enable_gzip=False)
    mod_wrk = load_module("src/lambda/functions/ingestion_worker/handler.py")
    yf_stub(["AAPL"])

    sqs_event = _receive_all_sqs_messages(queue_url)
    wr = mod_wrk["main"](sqs_event, None)
    assert any(f.get("itemIdentifier") for f in wr.get("batchItemFailures", []))
    listed = s3.list_objects_v2(Bucket=bucket, Prefix="market/prices/")
    assert int(listed.get("KeyCount", 0)) == 1


@mock_aws
def test_e2e_idempotency_skip(monkeypatch, orchestrator_env, worker_env, yf_stub, make_queue, make_bucket, load_module):
    # Orchestrator for two symbols
    os.environ["ENVIRONMENT"] = "dev"
    queue_url = make_queue("extract-e2e-idem")
    orchestrator_env(queue_url, chunk_size=2, batch_size=10)

    mod_orc = load_module("src/lambda/functions/ingestion_orchestrator/handler.py")

    event = {
        "symbols": ["AAPL", "MSFT"],
        "domain": "market",
        "table_name": "prices",
        "period": "1mo",
        "interval": "1d",
        "file_format": "json",
    }
    mod_orc["main"](event, None)

    # Worker sees existing prefix for MSFT -> skip write for MSFT
    bucket = make_bucket("raw-bucket-dev")
    s3 = boto3.client("s3", region_name="us-east-1")
    worker_env(bucket, enable_gzip=False)
    # Pre-create an existing MSFT object under today's prefix
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    prefix = f"market/prices/ingestion_date={today}/data_source=yahoo_finance/symbol=MSFT/interval=1d/period=1mo/"
    s3.put_object(Bucket=bucket, Key=f"{prefix}existing.json", Body=b"x")

    mod_wrk = load_module("src/lambda/functions/ingestion_worker/handler.py")
    yf_stub(["AAPL", "MSFT"])

    sqs_event = _receive_all_sqs_messages(queue_url)
    wr = mod_wrk["main"](sqs_event, None)
    assert wr == {"batchItemFailures": []}
    listed_msft = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    assert int(listed_msft.get("KeyCount", 0)) == 1
    prefix_aapl = f"market/prices/ingestion_date={today}/data_source=yahoo_finance/symbol=AAPL/"
    listed_aapl = s3.list_objects_v2(Bucket=bucket, Prefix=prefix_aapl)
    assert int(listed_aapl.get("KeyCount", 0)) == 1
