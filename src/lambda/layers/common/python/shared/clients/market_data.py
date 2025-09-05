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
        """Fetch prices for one or more symbols.

        Tries batch download via yfinance.download for multiple tickers to reduce
        API calls. Falls back to per-ticker history on errors.
        """
        syms = [s for s in symbols if str(s).strip()]
        if not syms:
            return []
        if self._yf is None:
            # Dependency not present; return empty results (handled by caller)
            return []

        # Attempt batched download when multiple tickers
        if len(syms) > 1:
            try:
                df = self._yf.download(
                    tickers=syms,
                    period=period,
                    interval=interval,
                    group_by="ticker",
                    auto_adjust=False,
                    threads=True,
                )
                out: List[PriceRecord] = []
                # df for multi-tickers: columns are MultiIndex (('AAPL','Open'), ...)
                # For single ticker, returns simple columns. Normalize both.
                if hasattr(df, "columns") and len(getattr(df, "columns", [])) > 0:
                    # Detect multi-ticker by MultiIndex columns
                    if getattr(df.columns, "levels", None) is not None and len(df.columns.levels) >= 2:
                        # MultiIndex case
                        for sym in syms:
                            try:
                                sub = df[sym]
                                if sub is None or sub.empty:
                                    continue
                                for ts, row in sub.iterrows():  # type: ignore[assignment]
                                    out.append(
                                        PriceRecord(
                                            symbol=sym,
                                            timestamp=(
                                                ts.to_pydatetime()
                                                if hasattr(ts, "to_pydatetime")
                                                else datetime.fromisoformat(str(ts))
                                            ),
                                            open=_safe_float(row, "Open"),
                                            high=_safe_float(row, "High"),
                                            low=_safe_float(row, "Low"),
                                            close=_safe_float(row, "Close"),
                                            volume=_safe_float(row, "Volume"),
                                        )
                                    )
                            except Exception:
                                # Skip problematic symbol; fallback later if needed
                                continue
                        return out
                    else:
                        # Single-like frame; treat as per-ticker for first symbol
                        pass
            except Exception:
                # fallback to per-ticker below
                pass

        # Fallback to per-ticker sequential fetch
        out: List[PriceRecord] = []
        for sym in syms:
            try:
                ticker = self._yf.Ticker(sym)
                df = ticker.history(period=period, interval=interval)
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
