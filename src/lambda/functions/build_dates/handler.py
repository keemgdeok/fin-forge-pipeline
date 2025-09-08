"""BuildDateArray Lambda: produces an array of ds strings from a date_range.

Input:
{
  "date_range": {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}
}

Output:
{ "dates": ["YYYY-MM-DD", ...] }

Errors:
- PRE_VALIDATION_FAILED when input invalid or range too long
"""

from __future__ import annotations

from datetime import date, timedelta
import os
from typing import Any, Dict


def _parse_date(s: str) -> date:
    y, m, d = s.split("-")
    return date(int(y), int(m), int(d))


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    rng = event.get("date_range") or {}
    start = str(rng.get("start", ""))
    end = str(rng.get("end", ""))
    if not start or not end:
        return {
            "error": {
                "code": "PRE_VALIDATION_FAILED",
                "message": "date_range.start/end required",
            }
        }

    try:
        d0 = _parse_date(start)
        d1 = _parse_date(end)
    except Exception:
        return {
            "error": {
                "code": "PRE_VALIDATION_FAILED",
                "message": "invalid date format",
            }
        }

    if d1 < d0:
        return {
            "error": {
                "code": "PRE_VALIDATION_FAILED",
                "message": "start must be <= end",
            }
        }

    max_days = int(os.environ.get("MAX_BACKFILL_DAYS", "31"))
    delta = (d1 - d0).days + 1
    if delta > max_days:
        return {
            "error": {
                "code": "PRE_VALIDATION_FAILED",
                "message": "date range too long",
            }
        }

    dates: list[str] = []
    cur = d0
    while cur <= d1:
        dates.append(cur.isoformat())
        cur += timedelta(days=1)

    return {"dates": dates}
