from aws_cdk import App
from aws_cdk.assertions import Template

from infrastructure.core.shared_storage_stack import SharedStorageStack
from infrastructure.pipelines.daily_prices_data import ingestion_stack as ing


def _base_config():
    return {
        "lambda_memory": 256,
        "lambda_timeout": 60,
        "worker_timeout": 120,
        "worker_memory": 256,
        "orchestrator_chunk_size": 5,
        "sqs_batch_size": 1,
        "sqs_send_batch_size": 10,
        "max_retries": 5,
        "ingestion_domain": "market",
        "ingestion_table_name": "prices",
        "ingestion_file_format": "json",
    }


def test_fanout_resources(fake_python_function) -> None:
    """
    Given: 인제스트 팬아웃 스택 기본 설정
    When: 스택을 합성하면
    Then: SQS 큐 2개(메인+DLQ), 워커 ESM, 스케줄 Rule이 생성되어야 함
    """
    app = App()
    cfg = _base_config()

    # Patch PythonFunction to avoid bundling
    fake_python_function(ing)

    shared = SharedStorageStack(app, "SharedStorageFanout", environment="dev", config=cfg)
    stack = ing.DailyPricesDataIngestionStack(
        app,
        "IngestionFanout",
        environment="dev",
        config=cfg,
        shared_storage_stack=shared,
        lambda_execution_role_arn="arn:aws:iam::111122223333:role/lambda",
    )

    t = Template.from_stack(stack)

    # Two queues: main + DLQ
    queues = t.find_resources("AWS::SQS::Queue")
    assert len(queues) >= 2

    # Event source mapping exists for worker
    mappings = t.find_resources("AWS::Lambda::EventSourceMapping")
    assert mappings, "SQS event source mapping must exist"

    # EventBridge Rule exists for orchestrator
    rules = t.find_resources("AWS::Events::Rule")
    assert rules, "Schedule rule must exist"

    # Ensure rule targets the orchestrator Lambda
    def _has_lambda_target(r):
        targets = r.get("Properties", {}).get("Targets", [])
        return bool(targets)

    assert any(_has_lambda_target(r) for r in rules.values())


def test_symbol_universe_deployment(fake_python_function) -> None:
    """Symbol universe asset should be deployed via BucketDeployment when configured."""

    app = App()
    cfg = {
        **_base_config(),
        "symbol_universe_asset_path": "data/symbols",
        "symbol_universe_asset_file": "nasdaq_sp500.json",
        "symbol_universe_s3_key": "market/universe/nasdaq_sp500.json",
        "symbol_universe_s3_bucket": None,
    }

    fake_python_function(ing)

    shared = SharedStorageStack(app, "SharedStorageSymbols", environment="dev", config=cfg)
    stack = ing.DailyPricesDataIngestionStack(
        app,
        "IngestionSymbols",
        environment="dev",
        config=cfg,
        shared_storage_stack=shared,
        lambda_execution_role_arn="arn:aws:iam::111122223333:role/lambda",
    )

    template = Template.from_stack(stack)
    deployments = template.find_resources("Custom::CDKBucketDeployment")
    assert deployments, "Symbol universe deployment custom resource must exist"
