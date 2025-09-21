"""Fetch and merge NASDAQ + S&P 500 symbol universes.

Usage:
    python data/symbols/find_symbols.py \
        --output-dir data/symbols \
        --nasdaq-output nasdaq.json \
        --sp500-output sp500.json \
        --union-output nasdaq_sp500.json

The script downloads the latest public listings, normalises tickers (dot → dash),
filters out NASDAQ test issues, and writes three JSON files:
  * individual NASDAQ list
  * individual S&P 500 list
  * union of both universes with duplicates removed and sorted

This helper is intended for manual refresh runs before committing updated symbol
assets to the repository so that CDK can redeploy them.
"""

from __future__ import annotations

import argparse
import io
import json
import sys
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd
import requests

# External data sources
SP500_CSV_URL = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
NASDAQ_LIST_URL = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt"


def fetch_sp500() -> list[str]:
    """Fetch the current S&P 500 components from public CSV dataset."""
    response = requests.get(SP500_CSV_URL, timeout=30)
    response.raise_for_status()

    df = pd.read_csv(io.StringIO(response.text))
    if "Symbol" not in df.columns:
        raise RuntimeError("S&P 500 dataset missing 'Symbol' column")

    symbols = df["Symbol"].astype(str).str.strip().str.upper().str.replace(r"\.", "-", regex=True).tolist()
    return sorted({s for s in symbols if s})


def fetch_nasdaq() -> list[str]:
    """Fetch the current NASDAQ listings from NASDAQ Trader directory."""
    response = requests.get(NASDAQ_LIST_URL, timeout=30)
    response.raise_for_status()

    text = response.text
    df = pd.read_csv(io.StringIO(text), sep="|")
    if "Symbol" not in df.columns:
        raise RuntimeError("NASDAQ listings missing 'Symbol' column")

    filtered = df[df.get("Test Issue", "N") == "N"]
    symbols = filtered["Symbol"].astype(str).str.strip().str.upper().str.replace(r"\.", "-", regex=True).tolist()
    return sorted({s for s in symbols if s})


def write_json(path: Path, symbols: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(list(symbols), file, ensure_ascii=False, indent=2)
        file.write("\n")


def compute_union(*collections: Iterable[str]) -> list[str]:
    universe: set[str] = set()
    for coll in collections:
        for symbol in coll:
            if symbol:
                universe.add(symbol)
    return sorted(universe)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch NASDAQ + S&P500 symbol universes")
    parser.add_argument("--output-dir", type=Path, default=Path("data/symbols"))
    parser.add_argument("--nasdaq-output", type=str, default="nasdaq.json")
    parser.add_argument("--sp500-output", type=str, default="sp500.json")
    parser.add_argument("--union-output", type=str, default="nasdaq_sp500.json")
    return parser.parse_args(argv)


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)

    try:
        sp500_symbols = fetch_sp500()
        nasdaq_symbols = fetch_nasdaq()
    except Exception as exc:  # pragma: no cover - network errors not unit-tested locally
        print(f"Failed to fetch symbols: {exc}", file=sys.stderr)
        return 1

    output_dir: Path = args.output_dir
    write_json(output_dir / args.sp500_output, sp500_symbols)
    write_json(output_dir / args.nasdaq_output, nasdaq_symbols)

    union_symbols = compute_union(sp500_symbols, nasdaq_symbols)
    write_json(output_dir / args.union_output, union_symbols)

    print(f"S&P 500 symbols: {len(sp500_symbols)}, NASDAQ symbols: {len(nasdaq_symbols)}, Union: {len(union_symbols)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
