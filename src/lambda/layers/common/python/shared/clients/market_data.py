"""Market data client(s) used by ingestion Lambdas.

Provides a thin facade over optional providers (e.g., Yahoo Finance) without
hard-depending on heavy libraries at import time.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional


@dataclass(frozen=True)
class PriceRecord:
    symbol: str
    timestamp: datetime
    close: Optional[float]
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    volume: Optional[float] = None

    def as_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "timestamp": self.timestamp.isoformat(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        }


class YahooFinanceClient:
    """Optional Yahoo Finance-backed client.

    If the optional `yfinance` dependency is not available, this client will
    return an empty dataset instead of raising, allowing the Lambda to operate
    in dry-run mode or within tests without network access.
    """

    def __init__(self) -> None:
        try:
            import yfinance as yf  # type: ignore

            self._yf = yf
        except Exception:
            self._yf = None

    def fetch_prices(self, symbols: Iterable[str], period: str, interval: str) -> List[PriceRecord]:
        if not symbols:
            return []
        if self._yf is None:
            # Dependency not present; return empty results (handled by caller)
            return []

        out: List[PriceRecord] = []
        # Fetch sequentially to keep it simple/safe; can be optimized later
        for sym in symbols:
            try:
                ticker = self._yf.Ticker(sym)
                df = ticker.history(period=period, interval=interval)
                # df index is DatetimeIndex; columns include Open/High/Low/Close/Volume
                if df is None or df.empty:
                    continue
                for ts, row in df.iterrows():  # type: ignore[assignment]
                    out.append(
                        PriceRecord(
                            symbol=sym,
                            timestamp=(
                                ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else datetime.fromisoformat(str(ts))
                            ),
                            open=_safe_float(row, "Open"),
                            high=_safe_float(row, "High"),
                            low=_safe_float(row, "Low"),
                            close=_safe_float(row, "Close"),
                            volume=_safe_float(row, "Volume"),
                        )
                    )
            except Exception:
                # Ignore per-symbol failures; caller can log aggregated stats
                continue
        return out


def _safe_float(row: Any, key: str) -> Optional[float]:
    try:
        val = row[key]
        if val is None:
            return None
        return float(val)
    except Exception:
        return None
