from __future__ import annotations

import io
import zipfile
from pathlib import Path


def zip_lambda_from_path(module_path: Path) -> bytes:
    """Package the given module file as index.py within an in-memory zip."""
    buffer = io.BytesIO()
    source = module_path.read_text()
    with zipfile.ZipFile(buffer, "w") as zf:
        info = zipfile.ZipInfo("index.py")
        info.external_attr = 0o755 << 16  # ensure Lambda can read the file
        zf.writestr(info, source)
    buffer.seek(0)
    return buffer.read()
