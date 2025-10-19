from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Iterable, List

import pytest


def stub_yahoo_finance(
    monkeypatch: pytest.MonkeyPatch,
    *,
    price: float = 150.0,
    days: int = 20,
) -> None:
    """시장 데이터 클라이언트를 고정 응답으로 스텁 처리한다."""
    from shared.clients import market_data
    from shared.ingestion import service as ingestion_service

    class _StaticYahooClient:
        def fetch_prices(
            self,
            symbols: Iterable[str],
            period: str,
            interval: str,
        ) -> List["market_data.PriceRecord"]:
            now = datetime(2025, 9, 7, tzinfo=timezone.utc)
            results: List["market_data.PriceRecord"] = []
            for day_offset in range(days):
                ts = now - timedelta(days=day_offset)
                for idx, symbol in enumerate(symbols):
                    results.append(
                        market_data.PriceRecord(
                            symbol=symbol,
                            timestamp=ts,
                            close=price + idx,
                            adjusted_close=price + idx + 1,
                            open=price + idx - 0.5,
                            high=price + idx + 1.5,
                            low=price + idx - 1.5,
                            volume=1_000_000 + (idx * 10_000),
                        )
                    )
            return results

    monkeypatch.setattr(market_data, "YahooFinanceClient", _StaticYahooClient)
    monkeypatch.setattr(ingestion_service, "YahooFinanceClient", _StaticYahooClient)
