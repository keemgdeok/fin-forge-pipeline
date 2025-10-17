from __future__ import annotations

import io
import zipfile

from tests.e2e.utils.zipper import zip_lambda_from_path


from pathlib import Path


def test_zip_lambda_from_path_packages_source(tmp_path: Path) -> None:
    module_file = tmp_path / "handler.py"
    module_file.write_text("def lambda_handler(event, context):\n    return {'status': 'ok'}\n")

    zip_bytes = zip_lambda_from_path(module_file)
    with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as archive:
        assert archive.namelist() == ["index.py"]
        info = archive.getinfo("index.py")
        assert (info.external_attr >> 16) & 0o777 == 0o755
        contents = archive.read("index.py").decode("utf-8")
        assert "lambda_handler" in contents
