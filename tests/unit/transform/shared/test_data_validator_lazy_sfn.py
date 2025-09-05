import runpy


def test_data_validator_lazy_stepfunctions(monkeypatch):
    # Load module
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
