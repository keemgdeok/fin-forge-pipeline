from aws_cdk import App, Duration
from aws_cdk.assertions import Template

from infrastructure.core.shared_storage_stack import SharedStorageStack
from infrastructure.pipelines.customer_data import ingestion_stack as ing


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


def test_fanout_resources(monkeypatch):
    app = App()
    cfg = _base_config()

    # Patch PythonFunction to avoid bundling
    def _fake_python_function(scope, id, **kwargs):
        from aws_cdk import aws_lambda as lambda_

        return lambda_.Function(
            scope,
            id,
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=lambda_.Code.from_inline("def handler(event, context): return {}"),
            memory_size=kwargs.get("memory_size", 128),
            timeout=kwargs.get("timeout", Duration.seconds(10)),
            log_retention=kwargs.get("log_retention"),
            role=kwargs.get("role"),
            layers=kwargs.get("layers", []),
            environment=kwargs.get("environment", {}),
            reserved_concurrent_executions=kwargs.get("reserved_concurrent_executions"),
        )

    monkeypatch.setattr(ing, "PythonFunction", _fake_python_function, raising=False)

    shared = SharedStorageStack(app, "SharedStorageFanout", environment="dev", config=cfg)
    stack = ing.CustomerDataIngestionStack(
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
