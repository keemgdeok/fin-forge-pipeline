from aws_cdk import App
from aws_cdk.assertions import Template

from infrastructure.core.security_stack import SecurityStack


def _base_config():
    return {
        "processing_triggers": [
            {"domain": "market", "table_name": "prices"},
            {"domain": "sales", "table_name": "orders"},
        ]
    }


def test_lambda_role_has_schema_prefix_permissions() -> None:
    app = App()
    cfg = _base_config()

    sec = SecurityStack(app, "SecStackSchema", environment="dev", config=cfg)
    template = Template.from_stack(sec)
    tpl = template.to_json()

    # Expect inline policy containing _schema/* path and Get/PutObject actions
    assert "_schema/*" in tpl
    assert "s3:GetObject" in tpl and "s3:PutObject" in tpl
