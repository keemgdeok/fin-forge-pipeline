"""Deterministic data builders for reliable, reproducible tests.

This module provides data builders that generate consistent, predictable test data
to eliminate flakiness and ensure test reproducibility across environments.

All timestamps, random values, and IDs are deterministic and reproducible.
"""

import hashlib
import itertools
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass, field
from enum import Enum


class TimeMode(Enum):
    """Timestamp generation modes for deterministic testing."""

    FIXED = "fixed"  # Always the same timestamp
    SEQUENTIAL = "sequential"  # Incrementing timestamps
    REALISTIC = "realistic"  # Business hours, market patterns


@dataclass
class DeterministicConfig:
    """Configuration for deterministic test data generation."""

    # Time configuration
    base_timestamp: datetime = field(default_factory=lambda: datetime(2025, 9, 7, 9, 30, 0, tzinfo=timezone.utc))
    time_mode: TimeMode = TimeMode.SEQUENTIAL
    time_increment_seconds: int = 60

    # Seed for pseudo-random but deterministic generation
    seed: int = 42

    # Market data configuration
    base_symbols: List[str] = field(
        default_factory=lambda: ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "NFLX"]
    )
    base_price: float = 100.0
    price_variation_percent: float = 0.1

    # Exchange configuration
    exchanges: List[str] = field(default_factory=lambda: ["NASDAQ", "NYSE", "AMEX"])

    # Volume configuration
    base_volume: int = 100000
    volume_variation_percent: float = 0.2


class DeterministicGenerator:
    """Generates deterministic, reproducible test data."""

    def __init__(self, config: Optional[DeterministicConfig] = None):
        self.config = config or DeterministicConfig()
        self._symbol_cycle = itertools.cycle(self.config.base_symbols)
        self._exchange_cycle = itertools.cycle(self.config.exchanges)
        self._record_counter = 0

        # Initialize deterministic pseudo-random state
        self._pseudo_random_state = self.config.seed

    def _deterministic_hash(self, input_value: Union[str, int]) -> int:
        """Generate deterministic hash for given input."""
        hash_input = f"{input_value}_{self.config.seed}".encode("utf-8")
        return int(hashlib.md5(hash_input).hexdigest()[:8], 16)

    def _deterministic_float(self, record_id: int, field_name: str, base_value: float, variation: float) -> float:
        """Generate deterministic float with controlled variation."""
        hash_val = self._deterministic_hash(f"{record_id}_{field_name}")
        # Convert hash to [-1, 1] range
        normalized = (hash_val % 10000) / 5000.0 - 1.0
        return base_value + (base_value * variation * normalized)

    def _deterministic_int(self, record_id: int, field_name: str, base_value: int, variation: float) -> int:
        """Generate deterministic integer with controlled variation."""
        float_result = self._deterministic_float(record_id, field_name, float(base_value), variation)
        return max(1, int(float_result))  # Ensure positive integers

    def _generate_timestamp(self, record_id: int) -> datetime:
        """Generate deterministic timestamp based on configuration."""
        if self.config.time_mode == TimeMode.FIXED:
            return self.config.base_timestamp

        elif self.config.time_mode == TimeMode.SEQUENTIAL:
            return self.config.base_timestamp + timedelta(seconds=record_id * self.config.time_increment_seconds)

        elif self.config.time_mode == TimeMode.REALISTIC:
            # Generate realistic market hours timestamps
            days_offset = record_id // 390  # 390 minutes per trading day (6.5 hours)
            minutes_in_day = record_id % 390

            trading_day_start = self.config.base_timestamp.replace(hour=9, minute=30)
            target_date = trading_day_start + timedelta(days=days_offset)

            # Skip weekends
            while target_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
                target_date += timedelta(days=1)

            return target_date + timedelta(minutes=minutes_in_day)

        return self.config.base_timestamp

    def generate_market_record(
        self,
        record_id: int,
        symbol: Optional[str] = None,
        include_extended_fields: bool = False,
        force_corruption: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Generate a single deterministic market data record."""

        # Use provided symbol or cycle through predefined ones
        if symbol is None:
            symbol_index = record_id % len(self.config.base_symbols)
            symbol = self.config.base_symbols[symbol_index]

        # Generate deterministic values
        price = self._deterministic_float(
            record_id, "price", self.config.base_price, self.config.price_variation_percent
        )

        volume = self._deterministic_int(
            record_id, "volume", self.config.base_volume, self.config.volume_variation_percent
        )

        timestamp = self._generate_timestamp(record_id)

        exchange_index = record_id % len(self.config.exchanges)
        exchange = self.config.exchanges[exchange_index]

        # Base record structure
        record = {
            "id": record_id,
            "symbol": symbol,
            "price": round(price, 2),
            "exchange": exchange,
            "timestamp": timestamp.isoformat() + "Z",
            "volume": volume,
            "bid": round(price - 0.05, 2),
            "ask": round(price + 0.05, 2),
        }

        # Add extended fields if requested
        if include_extended_fields:
            market_cap = self._deterministic_int(record_id, "market_cap", 1000000000, 0.5)
            sectors = ["TECH", "FINANCE", "HEALTHCARE", "ENERGY", "UTILITIES"]
            sector_index = record_id % len(sectors)

            record.update(
                {
                    "market_cap": market_cap,
                    "sector": sectors[sector_index],
                    "currency": "USD",
                    "last_updated": timestamp.isoformat() + "Z",
                    "high_52w": round(price * 1.2, 2),
                    "low_52w": round(price * 0.8, 2),
                    "pe_ratio": round(self._deterministic_float(record_id, "pe_ratio", 15.0, 0.3), 1),
                }
            )

        # Apply controlled corruption if specified
        if force_corruption:
            record = self._apply_deterministic_corruption(record, record_id, force_corruption)

        return record

    def _apply_deterministic_corruption(
        self, record: Dict[str, Any], record_id: int, corruption_type: str
    ) -> Dict[str, Any]:
        """Apply deterministic data corruption for testing."""
        corrupted_record = record.copy()

        if corruption_type == "null_symbol":
            corrupted_record["symbol"] = None

        elif corruption_type == "negative_price":
            corrupted_record["price"] = -abs(corrupted_record["price"])

        elif corruption_type == "zero_volume":
            corrupted_record["volume"] = 0

        elif corruption_type == "invalid_exchange":
            hash_val = self._deterministic_hash(f"{record_id}_invalid_exchange")
            invalid_exchanges = ["INVALID", "FAKE", "UNKNOWN", "TEST"]
            corrupted_record["exchange"] = invalid_exchanges[hash_val % len(invalid_exchanges)]

        elif corruption_type == "invalid_timestamp":
            corrupted_record["timestamp"] = "invalid_timestamp_format"

        elif corruption_type == "extreme_price":
            hash_val = self._deterministic_hash(f"{record_id}_extreme_price")
            if hash_val % 2 == 0:
                corrupted_record["price"] = 0.000001  # Extremely low
            else:
                corrupted_record["price"] = 999999.99  # Extremely high

        elif corruption_type == "missing_required_field":
            # Remove a required field deterministically
            required_fields = ["symbol", "price", "exchange", "timestamp"]
            field_to_remove = required_fields[record_id % len(required_fields)]
            if field_to_remove in corrupted_record:
                del corrupted_record[field_to_remove]

        return corrupted_record

    def generate_dataset(
        self,
        size: int,
        corruption_patterns: Optional[Dict[str, float]] = None,
        include_extended_fields: bool = False,
        duplicate_symbol_probability: float = 0.0,
    ) -> List[Dict[str, Any]]:
        """Generate deterministic dataset with controlled characteristics."""

        if corruption_patterns is None:
            corruption_patterns = {}

        dataset: List[Dict[str, Any]] = []

        for i in range(size):
            # Determine if this record should be corrupted
            corruption_type = None
            for pattern, probability in corruption_patterns.items():
                # Use deterministic "probability" based on record ID
                hash_val = self._deterministic_hash(f"{i}_{pattern}")
                if (hash_val % 10000) / 10000.0 < probability:
                    corruption_type = pattern
                    break  # Only apply one corruption per record

            # Handle duplicate symbols
            symbol = None
            if duplicate_symbol_probability > 0:
                hash_val = self._deterministic_hash(f"{i}_duplicate")
                if (hash_val % 10000) / 10000.0 < duplicate_symbol_probability and i > 0:
                    # Use symbol from a previous record
                    prev_record_idx = hash_val % min(i, 10)  # Look back up to 10 records
                    symbol = dataset[prev_record_idx]["symbol"]

            record = self.generate_market_record(
                record_id=i,
                symbol=symbol,
                include_extended_fields=include_extended_fields,
                force_corruption=corruption_type,
            )

            dataset.append(record)

        return dataset

    def generate_time_series_dataset(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        interval_minutes: int = 60,
        include_weekends: bool = False,
    ) -> List[Dict[str, Any]]:
        """Generate deterministic time series data for multiple symbols."""

        dataset = []
        current_time = start_date
        record_id = 0

        while current_time <= end_date:
            # Skip weekends unless specifically included
            if not include_weekends and current_time.weekday() >= 5:
                current_time += timedelta(minutes=interval_minutes)
                continue

            # Skip non-trading hours (before 9:30 AM or after 4:00 PM)
            if (
                current_time.hour < 9
                or (current_time.hour == 9 and current_time.minute < 30)
                or current_time.hour >= 16
            ):
                current_time += timedelta(minutes=interval_minutes)
                continue

            for symbol in symbols:
                # Generate deterministic price based on symbol and time
                symbol_hash = self._deterministic_hash(symbol)
                time_hash = self._deterministic_hash(current_time.strftime("%Y%m%d%H%M"))
                combined_hash = symbol_hash ^ time_hash

                base_price = 50.0 + (symbol_hash % 1000) / 10.0  # $50-150 base range
                time_variation = (time_hash % 2000) / 10000.0 - 0.1  # ±10% variation
                price = base_price * (1 + time_variation)

                volume = 50000 + (combined_hash % 100000)

                record = {
                    "id": record_id,
                    "symbol": symbol,
                    "price": round(price, 2),
                    "exchange": "NASDAQ",  # Simplified for time series
                    "timestamp": current_time.isoformat() + "Z",
                    "volume": volume,
                    "bid": round(price - 0.05, 2),
                    "ask": round(price + 0.05, 2),
                }

                dataset.append(record)
                record_id += 1

            current_time += timedelta(minutes=interval_minutes)

        return dataset


class DeterministicTransformEventBuilder:
    """Build deterministic transform pipeline events."""

    def __init__(self, config: Optional[DeterministicConfig] = None):
        self.config = config or DeterministicConfig()

    def build_preflight_event(
        self,
        domain: str = "market",
        table_name: str = "prices",
        ds: Optional[str] = None,
        include_s3_trigger: bool = True,
        execution_suffix: str = "",
    ) -> Dict[str, Any]:
        """Build deterministic preflight event."""

        if ds is None:
            ds = self.config.base_timestamp.strftime("%Y-%m-%d")

        event = {
            "environment": "test",
            "domain": domain,
            "table_name": table_name,
            "ds": ds,
            "reprocess": False,
            "catalog_update": "on_schema_change",
        }

        if include_s3_trigger:
            event.update(
                {
                    "source_bucket": "test-raw-bucket",
                    "source_key": f"{domain}/{table_name}/ingestion_date={ds}/data.json",
                    "file_type": "json",
                }
            )

        if execution_suffix:
            event["execution_id"] = f"test_exec_{ds}_{execution_suffix}"

        return event

    def build_schema_check_event(
        self,
        domain: str = "market",
        table_name: str = "prices",
        schema_version: str = "1.0",
        include_columns: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        """Build deterministic schema check event."""

        if include_columns is None:
            include_columns = [
                {"name": "symbol", "type": "string"},
                {"name": "price", "type": "double"},
                {"name": "exchange", "type": "string"},
                {"name": "timestamp", "type": "timestamp"},
                {"name": "volume", "type": "bigint"},
            ]

        return {
            "domain": domain,
            "table_name": table_name,
            "current_schema": {"columns": include_columns, "codec": "zstd", "version": schema_version},
        }

    def build_glue_job_args(
        self, ds: Optional[str] = None, domain: str = "market", table_name: str = "prices"
    ) -> Dict[str, str]:
        """Build deterministic Glue job arguments."""

        if ds is None:
            ds = self.config.base_timestamp.strftime("%Y-%m-%d")

        return {
            "--ds": ds,
            "--raw_bucket": "test-raw-bucket",
            "--raw_prefix": f"{domain}/{table_name}/",
            "--curated_bucket": "test-curated-bucket",
            "--curated_prefix": f"{domain}/{table_name}/",
            "--job-bookmark-option": "job-bookmark-enable",
            "--enable-s3-parquet-optimized-committer": "1",
            "--codec": "zstd",
            "--target_file_mb": "256",
            "--schema_fingerprint_s3_uri": f"s3://test-artifacts-bucket/{domain}/{table_name}/_schema/latest.json",
            "--file_type": "json",
        }


# Convenient pre-configured instances for common use cases
DEFAULT_GENERATOR = DeterministicGenerator()
REALISTIC_TIME_GENERATOR = DeterministicGenerator(DeterministicConfig(time_mode=TimeMode.REALISTIC))
FIXED_TIME_GENERATOR = DeterministicGenerator(DeterministicConfig(time_mode=TimeMode.FIXED))

DEFAULT_EVENT_BUILDER = DeterministicTransformEventBuilder()


# Helper functions for backward compatibility
def build_deterministic_market_data(
    count: int = 100, include_extended_fields: bool = False, corruption_rate: float = 0.0
) -> List[Dict[str, Any]]:
    """Generate deterministic market data with optional corruption."""
    corruption_patterns = {}
    if corruption_rate > 0:
        corruption_patterns = {
            "null_symbol": corruption_rate * 0.3,
            "negative_price": corruption_rate * 0.2,
            "zero_volume": corruption_rate * 0.2,
            "invalid_exchange": corruption_rate * 0.3,
        }

    return DEFAULT_GENERATOR.generate_dataset(
        size=count, corruption_patterns=corruption_patterns, include_extended_fields=include_extended_fields
    )


def build_deterministic_transform_event(
    domain: str = "market", table_name: str = "prices", ds: Optional[str] = None, **kwargs
) -> Dict[str, Any]:
    """Build deterministic transform event."""
    return DEFAULT_EVENT_BUILDER.build_preflight_event(domain=domain, table_name=table_name, ds=ds, **kwargs)


def build_deterministic_time_series(
    symbols: List[str], days: int = 5, interval_minutes: int = 60
) -> List[Dict[str, Any]]:
    """Build deterministic time series data."""
    config = DeterministicConfig()
    generator = DeterministicGenerator(config)

    start_date = config.base_timestamp
    end_date = start_date + timedelta(days=days)

    return generator.generate_time_series_dataset(
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        interval_minutes=interval_minutes,
        include_weekends=False,
    )
