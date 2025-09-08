import runpy


def test_data_validator_lazy_stepfunctions(monkeypatch) -> None:
    """
    Given: boto3.client 호출을 기록하는 스텁
    When: DataValidator 인스턴스를 생성하면
    Then: s3/glue만 생성되고 stepfunctions는 접근 시점에 지연 생성되어야 함
    """
    # Load module from shared layer
    mod = runpy.run_path("src/lambda/layers/common/python/shared/validation/data_validator.py")

    created = []

    class _Boto:
        def client(self, name: str, region_name=None):
            created.append(name)

            class _C:
                pass

            return _C()

    # Patch boto3 usage inside module
    monkeypatch.setitem(mod["boto3"].__dict__, "client", _Boto().client)

    dv = mod["DataValidator"]()
    # Should have created s3 and glue clients only
    assert "s3" in created and "glue" in created
    assert "stepfunctions" not in created

    # Accessing stepfunctions_client should lazily create it
    _ = dv.stepfunctions_client
    assert "stepfunctions" in created
