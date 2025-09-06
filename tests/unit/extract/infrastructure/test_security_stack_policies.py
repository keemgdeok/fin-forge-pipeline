from aws_cdk import App
from aws_cdk.assertions import Template

from infrastructure.core.security_stack import SecurityStack


def _cfg():
    return {}


def test_lambda_role_has_sqs_send_permissions():
    """
    Given: 보안 스택을 합성하면
    When: Lambda 실행 역할의 정책을 검사하면
    Then: sqs:SendMessage 또는 sqs:SendMessageBatch 권한이 존재해야 함
    """
    app = App()
    stack = SecurityStack(app, "SecStack", environment="dev", config=_cfg())
    t = Template.from_stack(stack)

    roles = t.find_resources("AWS::IAM::Role")
    # Find LambdaExecutionRole by name
    lambda_roles = [r for r in roles.values() if r.get("Properties", {}).get("RoleName", "").endswith("lambda-role")]
    assert lambda_roles, "Lambda execution role must exist"

    # Check inline policy contains sqs:SendMessage
    def _has_sqs_send(res):
        for pol in res.get("Properties", {}).get("Policies", []):
            doc = pol.get("PolicyDocument", {})
            for st in doc.get("Statement", []):
                actions = st.get("Action", [])
                if isinstance(actions, str):
                    actions = [actions]
                if any(a in ("sqs:SendMessage", "sqs:SendMessageBatch") for a in actions):
                    return True
        return False

    assert any(_has_sqs_send(r) for r in roles.values())
