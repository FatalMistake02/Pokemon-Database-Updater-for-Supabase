#!/usr/bin/env python3
"""Export Cardly card and set catalogs directly from their upstream APIs.

This script deliberately does not read Supabase or Neon. It reuses the source
fetching and normalization functions in pokemon-db-updater.py, then writes a
small set of bulk JSON files intended for the Cardly mobile app.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import re
import sys
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Iterable, Iterator, TypeVar

import requests


SCHEMA_VERSION = 2
DEFAULT_FETCH_WORKERS = 8
DEFAULT_REQUEST_DELAY = 0.1
DEFAULT_CHECKPOINT_INTERVAL = 600.0
CARD_FETCH_ATTEMPTS = 6
CARD_FETCH_RETRY_DELAY = 0.5
CARD_FETCH_MAX_RETRY_DELAY = 15.0
CARD_FETCH_BATCH_SIZE = 256
REGIONS = {
    "international": "data",
    "japan": "data-asia",
}


class RequestRateLimiter:
    """Keep concurrent request starts separated by a minimum interval."""

    def __init__(self, interval: float):
        self.interval = interval
        self._lock = threading.Lock()
        self._next_start = 0.0

    def wait(self) -> None:
        if self.interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            wait_for = max(0.0, self._next_start - now)
            self._next_start = max(now, self._next_start) + self.interval
        if wait_for > 0:
            time.sleep(wait_for)

    def defer(self, delay: float) -> None:
        """Pause new request starts when the upstream service is struggling."""
        with self._lock:
            self._next_start = max(self._next_start, time.monotonic() + delay)


T = TypeVar("T")


def request_with_retries(
    operation: Callable[[], T],
    label: str,
    rate_limiter: RequestRateLimiter | None = None,
) -> T:
    """Retry transient HTTP failures and slow all workers during backoff."""
    for attempt in range(1, CARD_FETCH_ATTEMPTS + 1):
        if rate_limiter is not None:
            rate_limiter.wait()
        try:
            return operation()
        except requests.RequestException as exc:
            if attempt == CARD_FETCH_ATTEMPTS:
                raise
            response = getattr(exc, "response", None)
            status_code = getattr(response, "status_code", None)
            if isinstance(status_code, int) and 400 <= status_code < 500 and status_code not in {
                408,
                425,
                429,
            }:
                raise
            delay = min(
                CARD_FETCH_RETRY_DELAY * (2 ** (attempt - 1)),
                CARD_FETCH_MAX_RETRY_DELAY,
            )
            retry_after = response.headers.get("Retry-After") if response is not None else None
            try:
                delay = max(delay, float(retry_after))
            except (TypeError, ValueError):
                pass
            print(
                f"Retrying {label} after {type(exc).__name__} "
                f"({attempt}/{CARD_FETCH_ATTEMPTS}) in {delay:g}s",
                file=sys.stderr,
            )
            if rate_limiter is not None:
                rate_limiter.defer(delay)
            else:
                time.sleep(delay)
    raise AssertionError("unreachable")


def fetch_card_details_parallel(
    executor: ThreadPoolExecutor,
    source: ModuleType,
    version: str,
    requests_to_make: Iterable[tuple[str, str | None, Any]],
    rate_limiter: RequestRateLimiter,
) -> Iterator[tuple[str, Any, Exception | None]]:
    """Fetch card details concurrently while preserving input order."""

    def fetch_one(request: tuple[str, str | None, Any]) -> tuple[str, Any, Exception | None]:
        card_id, set_id, local_id = request
        try:
            details = request_with_retries(
                lambda: source.fetch_card_details(
                    card_id,
                    version,
                    set_id=set_id,
                    local_id=local_id,
                ),
                f"{version} card {card_id}",
                rate_limiter,
            )
            return card_id, details, None
        except Exception as exc:
            print(
                f"Failed to fetch {version} card {card_id} after "
                f"{CARD_FETCH_ATTEMPTS} attempts: {exc}",
                file=sys.stderr,
            )
            return card_id, None, exc

    requests_iterator = iter(requests_to_make)
    while batch := list(islice(requests_iterator, CARD_FETCH_BATCH_SIZE)):
        yield from executor.map(fetch_one, batch)


def load_source_module(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location("cardly_source_updater", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load source updater: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def is_pocket_set(row: dict[str, Any]) -> bool:
    set_id = str(row.get("id") or "").lower()
    name = str(row.get("name") or "").lower()
    series = row.get("serie") or row.get("series") or ""
    if isinstance(series, dict):
        series = series.get("name") or ""
    return "pocket" in set_id or "pocket" in name or "pocket" in str(series).lower()


def clean_export_row(row: dict[str, Any]) -> dict[str, Any]:
    # Source update timestamps make identical catalogs look different every run.
    # Publication time and content hashes belong in manifest.json instead.
    return {key: value for key, value in row.items() if key != "updated_at"}


def stable_id(value: Any, label: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"Missing {label} ID")
    return text


def read_existing_rows(path: Path) -> list[dict[str, Any]]:
    """Read optional prior catalog rows for per-card failure fallback."""
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    if not isinstance(value, list):
        return []
    return [row for row in value if isinstance(row, dict)]


def prices_by_card(rows: Iterable[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        card_id = str(row.get("card_id") or "").strip()
        if card_id:
            grouped.setdefault(card_id, []).append(row)
    return grouped


class PeriodicCheckpointWriter:
    """Write occasional snapshots without blocking upstream fetching."""

    def __init__(self, region_dir: Path, interval: float):
        self.region_dir = region_dir
        self.interval = interval
        self._next_write = time.monotonic() + interval if interval > 0 else 0.0
        self._executor = (
            ThreadPoolExecutor(max_workers=1, thread_name_prefix="catalog-checkpoint")
            if interval > 0
            else None
        )
        self._future: Future[None] | None = None

    @staticmethod
    def _write_snapshot(region_dir: Path, files: dict[str, list[dict[str, Any]]]) -> None:
        for filename, rows in files.items():
            write_json_atomic(region_dir / filename, rows)

    def maybe_write(self, **files: list[dict[str, Any]]) -> bool:
        if self._executor is None:
            return False
        now = time.monotonic()
        if now < self._next_write:
            return False
        if self._future is not None:
            if not self._future.done():
                return False
            self._future.result()

        # A shallow list snapshot is enough: completed rows are not mutated again.
        snapshots = {f"{name}.json": list(rows) for name, rows in files.items()}
        self._future = self._executor.submit(self._write_snapshot, self.region_dir, snapshots)
        self._next_write = now + self.interval
        return True

    def close(self) -> None:
        if self._executor is None:
            return
        self._executor.shutdown(wait=True)
        if self._future is not None:
            self._future.result()

    def __enter__(self) -> "PeriodicCheckpointWriter":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()


def export_region(
    source: ModuleType,
    version: str,
    output_root: Path,
    limit_sets: int | None = None,
    request_delay: float = DEFAULT_REQUEST_DELAY,
    workers: int = DEFAULT_FETCH_WORKERS,
    checkpoint_interval: float = DEFAULT_CHECKPOINT_INTERVAL,
) -> dict[str, Any]:
    directory_name = REGIONS[version]
    region_dir = output_root / directory_name
    existing_cards = {
        str(row.get("id")): row
        for row in read_existing_rows(region_dir / "cards.json")
        if row.get("id")
    }
    existing_card_prices = prices_by_card(read_existing_rows(region_dir / "prices.json"))
    print(f"Fetching {version} sets directly from the upstream source...")
    summaries = request_with_retries(
        lambda: source.fetch_all_sets(version),
        f"{version} set list",
    )
    if not isinstance(summaries, list):
        raise RuntimeError(f"The {version} source did not return a set list")

    summaries = [row for row in summaries if isinstance(row, dict) and not is_pocket_set(row)]
    if limit_sets is not None:
        summaries = summaries[:limit_sets]
    if not summaries:
        raise RuntimeError(f"The {version} source returned no usable sets")

    sets: list[dict[str, Any]] = []
    cards: list[dict[str, Any]] = []
    prices: list[dict[str, Any]] = []
    seen_set_ids: set[str] = set()
    seen_set_keys: set[str] = set()
    seen_card_ids: set[str] = set()
    failed_card_ids: list[str] = []

    rate_limiter = RequestRateLimiter(request_delay)
    with PeriodicCheckpointWriter(region_dir, checkpoint_interval) as checkpoints:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="card-fetch") as executor:
            for set_index, summary in enumerate(summaries, 1):
                summary_id = stable_id(summary.get("id"), "set")
                print(f"[{version} {set_index}/{len(summaries)}] Fetching set {summary_id}")
                details = request_with_retries(
                    lambda: source.fetch_set_details(summary_id, version),
                    f"{version} set {summary_id}",
                )
                if not isinstance(details, dict):
                    raise RuntimeError(f"Could not fetch complete details for {version} set {summary_id}")
                if is_pocket_set(details):
                    continue

                source_name = source.detect_data_source(details)
                set_row = clean_export_row(source.transform_set_data(details, version, source_name))
                set_id = stable_id(set_row.get("id") or summary_id, "set")
                set_row["id"] = set_id
                set_key = set_id.casefold()
                if set_key in seen_set_keys:
                    print(
                        f"Skipping duplicate {version} set {summary_id}; "
                        f"it resolves to {set_id}",
                        file=sys.stderr,
                    )
                    continue
                seen_set_keys.add(set_key)
                seen_set_ids.add(set_id)

                card_summaries = details.get("cards", [])
                if not isinstance(card_summaries, list):
                    raise RuntimeError(f"Set {set_id} did not return a card list")
                if not all(isinstance(row, dict) for row in card_summaries):
                    raise RuntimeError(f"Set {set_id} returned an invalid card summary")

                card_requests = [
                    (
                        stable_id(card_summary.get("id") or card_summary.get("uuid"), "card"),
                        summary_id,
                        card_summary.get("localId") or card_summary.get("local_id"),
                    )
                    for card_summary in card_summaries
                ]
                fetched_cards = fetch_card_details_parallel(
                    executor,
                    source,
                    version,
                    card_requests,
                    rate_limiter,
                )

                set_card_count = 0
                for card_index, (card_source_id, details_card, fetch_error) in enumerate(fetched_cards, 1):
                    if fetch_error is not None or not isinstance(details_card, dict):
                        failed_card_ids.append(card_source_id)
                        existing_card = existing_cards.get(card_source_id)
                        if existing_card is None:
                            print(
                                f"Skipping {version} card {card_source_id}; no prior catalog row is available",
                                file=sys.stderr,
                            )
                            continue
                        card_row = dict(existing_card)
                        card_row["set_id"] = set_id
                        card_row["set_name"] = card_row.get("set_name") or set_row.get("name")
                        card_row["version"] = version
                        card_id = stable_id(card_row.get("id") or card_source_id, "card")
                        print(
                            f"Preserving prior catalog data for failed {version} card {card_id}",
                            file=sys.stderr,
                        )
                        prices.extend(dict(row) for row in existing_card_prices.get(card_id, []))
                    else:
                        try:
                            card_source = source.detect_data_source(details_card)
                            card_row = clean_export_row(
                                source.transform_card_data(details_card, version, card_source)
                            )
                            card_id = stable_id(card_row.get("id") or card_source_id, "card")
                        except Exception as exc:
                            failed_card_ids.append(card_source_id)
                            existing_card = existing_cards.get(card_source_id)
                            if existing_card is None:
                                print(
                                    f"Skipping invalid {version} card {card_source_id}: {exc}",
                                    file=sys.stderr,
                                )
                                continue
                            card_row = dict(existing_card)
                            card_id = stable_id(card_row.get("id") or card_source_id, "card")
                            print(
                                f"Preserving prior catalog data for invalid {version} card {card_id}: {exc}",
                                file=sys.stderr,
                            )

                    card_id = stable_id(card_row.get("id") or card_source_id, "card")
                    if card_id in seen_card_ids:
                        raise RuntimeError(f"Duplicate {version} card ID: {card_id}")
                    seen_card_ids.add(card_id)

                    card_row["id"] = card_id
                    card_row["set_id"] = set_id
                    card_row["set_name"] = card_row.get("set_name") or set_row.get("name")
                    card_row["version"] = version
                    cards.append(card_row)
                    if fetch_error is None and isinstance(details_card, dict):
                        pricing = details_card.get("pricing")
                        if isinstance(pricing, dict):
                            try:
                                new_prices = [
                                    clean_export_row(row)
                                    for row in source.transform_price_data(card_id, pricing)
                                ]
                                prices.extend(new_prices)
                            except Exception as exc:
                                failed_card_ids.append(card_source_id)
                                prices.extend(dict(row) for row in existing_card_prices.get(card_id, []))
                                print(
                                    f"Preserving prior prices for invalid {version} card {card_id}: {exc}",
                                    file=sys.stderr,
                                )
                    set_card_count += 1

                    if card_index % 100 == 0:
                        print(f"  Fetched {card_index}/{len(card_summaries)} cards")

                set_row["card_count"] = set_card_count
                set_row["version"] = version
                sets.append(set_row)
                if checkpoints.maybe_write(sets=sets, cards=cards, prices=prices):
                    print(
                        f"  Saving background checkpoint after {set_index}/{len(summaries)} sets"
                    )

    if not cards:
        raise RuntimeError(f"The {version} export contained no cards")
    unknown_set_ids = sorted({stable_id(card.get("set_id"), "card set") for card in cards} - seen_set_ids)
    if unknown_set_ids:
        raise RuntimeError(f"Cards reference unknown sets: {', '.join(unknown_set_ids[:10])}")

    sets.sort(key=lambda row: stable_id(row.get("id"), "set"))
    cards.sort(key=lambda row: stable_id(row.get("id"), "card"))
    region_dir.mkdir(parents=True, exist_ok=True)
    write_json_atomic(region_dir / "sets.json", sets)
    write_json_atomic(region_dir / "cards.json", cards)
    prices.sort(
        key=lambda row: (
            stable_id(row.get("card_id"), "price card"),
            str(row.get("market_source") or ""),
            str(row.get("condition") or ""),
            str(row.get("price_type") or ""),
        )
    )
    write_json_atomic(region_dir / "prices.json", prices)
    print(f"Captured {len(prices)} price rows for {version}")
    if failed_card_ids:
        print(
            f"WARNING: {len(set(failed_card_ids))} {version} cards used prior data or were skipped",
            file=sys.stderr,
        )

    return {
        "version": version,
        "directory": directory_name,
        "setCount": len(sets),
        "cardCount": len(cards),
        "priceCount": len(prices),
        "failedCardCount": len(set(failed_card_ids)),
        "failedCardIds": sorted(set(failed_card_ids)),
        "sets": file_metadata(output_root, region_dir / "sets.json"),
        "cards": file_metadata(output_root, region_dir / "cards.json"),
        "prices": file_metadata(output_root, region_dir / "prices.json"),
    }


def read_json_list(path: Path, label: str) -> list[dict[str, Any]]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"Could not load {label} from {path}: {exc}") from exc
    if not isinstance(value, list) or not value:
        raise RuntimeError(f"The existing {label} is empty or invalid: {path}")
    if not all(isinstance(row, dict) for row in value):
        raise RuntimeError(f"The existing {label} contains an invalid row: {path}")
    return value


def export_region_prices(
    source: ModuleType,
    version: str,
    output_root: Path,
    request_delay: float = DEFAULT_REQUEST_DELAY,
    workers: int = DEFAULT_FETCH_WORKERS,
    checkpoint_interval: float = DEFAULT_CHECKPOINT_INTERVAL,
) -> dict[str, Any]:
    """Refresh prices while preserving the existing release-gated sets and cards."""
    directory_name = REGIONS[version]
    region_dir = output_root / directory_name
    sets_path = region_dir / "sets.json"
    cards_path = region_dir / "cards.json"
    prices_path = region_dir / "prices.json"
    sets = read_json_list(sets_path, f"{version} sets")
    cards = read_json_list(cards_path, f"{version} cards")
    existing_card_prices = prices_by_card(read_existing_rows(prices_path))
    prices: list[dict[str, Any]] = []
    failed_card_ids: list[str] = []

    card_requests = [
        (
            stable_id(card.get("id"), "card"),
            card.get("set_id"),
            card.get("local_id") or card.get("localId") or card.get("number"),
        )
        for card in cards
    ]
    print(
        f"Refreshing prices for {len(cards)} existing {version} cards "
        f"with {workers} workers..."
    )
    rate_limiter = RequestRateLimiter(request_delay)
    with PeriodicCheckpointWriter(region_dir, checkpoint_interval) as checkpoints:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="price-fetch") as executor:
            fetched_cards = fetch_card_details_parallel(
                executor,
                source,
                version,
                card_requests,
                rate_limiter,
            )
            for card_index, (card_id, details, fetch_error) in enumerate(fetched_cards, 1):
                if fetch_error is not None or not isinstance(details, dict):
                    failed_card_ids.append(card_id)
                    prices.extend(dict(row) for row in existing_card_prices.get(card_id, []))
                    print(
                        f"Preserving prior prices for failed {version} card {card_id}",
                        file=sys.stderr,
                    )
                else:
                    pricing = details.get("pricing")
                    if isinstance(pricing, dict):
                        try:
                            new_prices = [
                                clean_export_row(row)
                                for row in source.transform_price_data(card_id, pricing)
                            ]
                            prices.extend(new_prices)
                        except Exception as exc:
                            failed_card_ids.append(card_id)
                            prices.extend(dict(row) for row in existing_card_prices.get(card_id, []))
                            print(
                                f"Preserving prior prices for invalid {version} card {card_id}: {exc}",
                                file=sys.stderr,
                            )

                if card_index % 100 == 0:
                    print(f"  Refreshed {card_index}/{len(cards)} card prices")
                    if checkpoints.maybe_write(prices=prices):
                        print(
                            f"  Saving background price checkpoint after "
                            f"{card_index}/{len(cards)} cards"
                        )

    prices.sort(
        key=lambda row: (
            stable_id(row.get("card_id"), "price card"),
            str(row.get("market_source") or ""),
            str(row.get("condition") or ""),
            str(row.get("price_type") or ""),
        )
    )
    write_json_atomic(prices_path, prices)
    print(f"Captured {len(prices)} price rows for {version}; sets and cards were not rebuilt")
    if failed_card_ids:
        print(
            f"WARNING: preserved prior prices for {len(set(failed_card_ids))} {version} cards",
            file=sys.stderr,
        )

    return {
        "version": version,
        "directory": directory_name,
        "setCount": len(sets),
        "cardCount": len(cards),
        "priceCount": len(prices),
        "failedCardCount": len(set(failed_card_ids)),
        "failedCardIds": sorted(set(failed_card_ids)),
        "sets": file_metadata(output_root, sets_path),
        "cards": file_metadata(output_root, cards_path),
        "prices": file_metadata(output_root, prices_path),
    }


def canonical_json_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")


def write_json_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(canonical_json_bytes(value))
    temporary.replace(path)


def file_metadata(root: Path, path: Path) -> dict[str, Any]:
    content = path.read_bytes()
    return {
        "path": path.relative_to(root).as_posix(),
        "bytes": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
    }


def content_version(regions: Iterable[dict[str, Any]]) -> str:
    hashes = [item[kind]["sha256"] for item in regions for kind in ("sets", "cards", "prices")]
    return hashlib.sha256("".join(hashes).encode("ascii")).hexdigest()


def existing_manifest(output_root: Path) -> dict[str, Any]:
    manifest_path = output_root / "manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return manifest if isinstance(manifest, dict) else {}


def existing_publication_time(output_root: Path, version: str) -> str | None:
    manifest = existing_manifest(output_root)
    if manifest.get("version") == version:
        value = manifest.get("publishedAt")
        return value if isinstance(value, str) else None
    return None


def publish_manifest(
    output_root: Path,
    regions: list[dict[str, Any]],
    tcgdex_release: str | None = None,
) -> dict[str, Any]:
    version = content_version(regions)
    published_at = existing_publication_time(output_root, version) or datetime.now(timezone.utc).isoformat()
    prior_manifest = existing_manifest(output_root)
    prior_release = prior_manifest.get("tcgdexRelease")
    failed_card_count = sum(int(region.get("failedCardCount") or 0) for region in regions)
    requested_release = tcgdex_release
    tcgdex_release = prior_release if requested_release and failed_card_count else requested_release or prior_release
    manifest = {
        "schemaVersion": SCHEMA_VERSION,
        "version": version,
        "publishedAt": published_at,
        "regions": {region["version"]: region for region in regions},
    }
    if tcgdex_release:
        manifest["tcgdexRelease"] = tcgdex_release
    if requested_release and failed_card_count:
        manifest["pendingTcgdexRelease"] = requested_release
    write_json_atomic(output_root / "manifest.json", manifest)
    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export cards and sets from upstream APIs for Cardly")
    parser.add_argument("--output", type=Path, required=True, help="Checked-out cards-database repository")
    parser.add_argument(
        "--source-module",
        type=Path,
        default=Path(__file__).with_name("pokemon-db-updater.py"),
        help="Path to pokemon-db-updater.py",
    )
    parser.add_argument("--region", choices=("both", "international", "japan"), default="both")
    parser.add_argument("--limit-sets", type=int, help="Testing only: export the first N sets")
    parser.add_argument(
        "--request-delay",
        type=float,
        default=DEFAULT_REQUEST_DELAY,
        help=f"Minimum seconds between concurrent request starts (default: {DEFAULT_REQUEST_DELAY})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_FETCH_WORKERS,
        help=f"Concurrent card requests (default: {DEFAULT_FETCH_WORKERS})",
    )
    parser.add_argument(
        "--checkpoint-interval",
        type=float,
        default=DEFAULT_CHECKPOINT_INTERVAL,
        help=(
            "Seconds between non-blocking file checkpoints; "
            f"0 disables them (default: {DEFAULT_CHECKPOINT_INTERVAL:g})"
        ),
    )
    parser.add_argument(
        "--prices-only",
        action="store_true",
        help="Refresh prices from existing catalog cards without rebuilding sets or cards",
    )
    parser.add_argument(
        "--tcgdex-release",
        help="TCGdex cards-database release tag represented by this catalog",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.limit_sets is not None and args.limit_sets < 1:
        raise ValueError("--limit-sets must be at least 1")
    if args.request_delay < 0:
        raise ValueError("--request-delay cannot be negative")
    if args.workers < 1:
        raise ValueError("--workers must be at least 1")
    if args.checkpoint_interval < 0:
        raise ValueError("--checkpoint-interval cannot be negative")
    if args.prices_only and args.limit_sets is not None:
        raise ValueError("--prices-only cannot be combined with --limit-sets")

    output_root = args.output.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    source = load_source_module(args.source_module.resolve())
    versions = REGIONS if args.region == "both" else (args.region,)
    if args.prices_only:
        regions = [
            export_region_prices(
                source,
                version,
                output_root,
                request_delay=args.request_delay,
                workers=args.workers,
                checkpoint_interval=args.checkpoint_interval,
            )
            for version in versions
        ]
    else:
        regions = [
            export_region(
                source,
                version,
                output_root,
                limit_sets=args.limit_sets,
                request_delay=args.request_delay,
                workers=args.workers,
                checkpoint_interval=args.checkpoint_interval,
            )
            for version in versions
        ]
    manifest = publish_manifest(output_root, regions, args.tcgdex_release)
    print(
        f"Catalog {manifest['version'][:12]} ready: "
        + ", ".join(f"{row['version']}={row['cardCount']} cards" for row in regions)
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Export cancelled", file=sys.stderr)
        raise SystemExit(130)
