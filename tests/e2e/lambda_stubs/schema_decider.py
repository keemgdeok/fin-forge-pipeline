from __future__ import annotations

from typing import Any, Dict


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    """Glue 크롤러 실행 여부를 결정하는 단순 스텁."""
    return {"shouldRunCrawler": True}
