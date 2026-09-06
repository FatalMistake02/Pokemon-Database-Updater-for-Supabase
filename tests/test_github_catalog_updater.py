import importlib.util
import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import requests


MODULE_PATH = Path(__file__).parents[1] / "github_catalog_updater.py"
SPEC = importlib.util.spec_from_file_location("github_catalog_updater", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


class FakeSource:
    def fetch_all_sets(self, version):
        return [{"id": "set-1", "name": "First Set"}]

    def fetch_set_details(self, set_id, version):
        return {"id": set_id, "name": "First Set", "cards": [{"id": "card-1"}]}

    def fetch_cards_in_set(self, set_id, version):
        return [{"id": "card-1"}]

    def fetch_card_details(self, card_id, version, set_id=None, local_id=None):
        return {"id": card_id, "name": "Pikachu", "pricing": {"market": 1}}

    def detect_data_source(self, row):
        return "fake"

    def transform_set_data(self, row, version, source):
        return {"id": row["id"], "name": row["name"], "version": version, "updated_at": "changes"}

    def transform_card_data(self, row, version, source):
        return {"id": row["id"], "name": row["name"], "set_id": None, "version": version, "updated_at": "changes"}

    def transform_price_data(self, card_id, pricing):
        return [{"card_id": card_id, "market_source": "fake", "price_type": "normal", "updated_at": "changes"}]


class CatalogExportTests(unittest.TestCase):
    def test_duplicate_set_details_are_skipped(self):
        class DuplicateSetSource(FakeSource):
            def fetch_all_sets(self, version):
                return [{"id": "alias"}, {"id": "canonical"}]

            def fetch_set_details(self, set_id, version):
                return {
                    "id": "Canonical",
                    "name": "One Set",
                    "cards": [{"id": "card-1"}],
                }

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            region = MODULE.export_region(
                DuplicateSetSource(),
                "japan",
                root,
                request_delay=0,
                checkpoint_interval=0,
            )

            sets = json.loads((root / "data-asia" / "sets.json").read_text(encoding="utf-8"))

        self.assertEqual(region["setCount"], 1)
        self.assertEqual([row["id"] for row in sets], ["Canonical"])

    def test_periodic_checkpoint_writes_snapshot_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            region_dir = Path(temporary) / "data"
            with patch.object(MODULE.time, "monotonic", side_effect=[100.0, 701.0]):
                with MODULE.PeriodicCheckpointWriter(region_dir, 600) as checkpoints:
                    wrote = checkpoints.maybe_write(
                        sets=[{"id": "set-1"}],
                        cards=[{"id": "card-1"}],
                        prices=[{"card_id": "card-1"}],
                    )

            self.assertTrue(wrote)
            self.assertEqual(
                json.loads((region_dir / "sets.json").read_text(encoding="utf-8")),
                [{"id": "set-1"}],
            )
            self.assertEqual(
                json.loads((region_dir / "cards.json").read_text(encoding="utf-8")),
                [{"id": "card-1"}],
            )
            self.assertEqual(
                json.loads((region_dir / "prices.json").read_text(encoding="utf-8")),
                [{"card_id": "card-1"}],
            )

    def test_export_writes_bulk_files_and_stable_manifest(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            region = MODULE.export_region(FakeSource(), "international", root, request_delay=0)
            manifest = MODULE.publish_manifest(root, [region])

            sets = json.loads((root / "data" / "sets.json").read_text(encoding="utf-8"))
            cards = json.loads((root / "data" / "cards.json").read_text(encoding="utf-8"))
            self.assertEqual(sets[0]["card_count"], 1)
            self.assertEqual(cards[0]["set_id"], "set-1")
            self.assertNotIn("updated_at", sets[0])
            self.assertNotIn("updated_at", cards[0])
            self.assertNotIn("pricing", cards[0])
            self.assertEqual(manifest["regions"]["international"]["cardCount"], 1)
            prices = json.loads((root / "data" / "prices.json").read_text(encoding="utf-8"))
            self.assertEqual(prices[0]["card_id"], "card-1")
            self.assertNotIn("updated_at", prices[0])
            self.assertEqual(manifest["regions"]["international"]["priceCount"], 1)
            self.assertEqual(manifest["schemaVersion"], 2)

            second = MODULE.publish_manifest(root, [region])
            self.assertEqual(second["version"], manifest["version"])
            self.assertEqual(second["publishedAt"], manifest["publishedAt"])

    def test_price_only_export_preserves_cards_sets_and_release(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            region_dir = root / "data"
            region_dir.mkdir()
            sets = [{"id": "set-1", "name": "First Set"}]
            cards = [{"id": "card-1", "set_id": "set-1", "local_id": "1"}]
            MODULE.write_json_atomic(region_dir / "sets.json", sets)
            MODULE.write_json_atomic(region_dir / "cards.json", cards)
            MODULE.write_json_atomic(region_dir / "prices.json", [])
            initial_region = {
                "version": "international",
                "directory": "data",
                "setCount": 1,
                "cardCount": 1,
                "priceCount": 0,
                "sets": MODULE.file_metadata(root, region_dir / "sets.json"),
                "cards": MODULE.file_metadata(root, region_dir / "cards.json"),
                "prices": MODULE.file_metadata(root, region_dir / "prices.json"),
            }
            MODULE.publish_manifest(root, [initial_region], "v2.47.0")
            original_sets = (region_dir / "sets.json").read_bytes()
            original_cards = (region_dir / "cards.json").read_bytes()

            region = MODULE.export_region_prices(FakeSource(), "international", root, request_delay=0)
            manifest = MODULE.publish_manifest(root, [region])

            self.assertEqual((region_dir / "sets.json").read_bytes(), original_sets)
            self.assertEqual((region_dir / "cards.json").read_bytes(), original_cards)
            self.assertEqual(manifest["tcgdexRelease"], "v2.47.0")
            self.assertEqual(manifest["regions"]["international"]["priceCount"], 1)


class ParallelFetchTests(unittest.TestCase):
    def test_card_details_are_fetched_concurrently_in_input_order(self):
        barrier = threading.Barrier(2)

        class ConcurrentSource:
            def fetch_card_details(self, card_id, version, set_id=None, local_id=None):
                barrier.wait(timeout=1)
                return {"id": card_id}

        card_requests = [("card-1", "set-1", "1"), ("card-2", "set-1", "2")]
        with MODULE.ThreadPoolExecutor(max_workers=2) as executor:
            result = list(
                MODULE.fetch_card_details_parallel(
                    executor,
                    ConcurrentSource(),
                    "international",
                    card_requests,
                    MODULE.RequestRateLimiter(0),
                )
            )

        self.assertEqual([card_id for card_id, _, _ in result], ["card-1", "card-2"])

    def test_transient_request_errors_are_retried(self):
        class RetrySource:
            def __init__(self):
                self.attempts = 0

            def fetch_card_details(self, card_id, version, set_id=None, local_id=None):
                self.attempts += 1
                if self.attempts < 3:
                    raise requests.Timeout("temporary timeout")
                return {"id": card_id}

        source = RetrySource()
        with patch.object(MODULE, "CARD_FETCH_RETRY_DELAY", 0):
            with MODULE.ThreadPoolExecutor(max_workers=1) as executor:
                result = list(
                    MODULE.fetch_card_details_parallel(
                        executor,
                        source,
                        "international",
                        [("card-1", "set-1", "1")],
                        MODULE.RequestRateLimiter(0),
                    )
                )

        self.assertEqual(result, [("card-1", {"id": "card-1"}, None)])
        self.assertEqual(source.attempts, 3)

    def test_failed_price_fetch_preserves_previous_prices(self):
        class FailedSource(FakeSource):
            def fetch_card_details(self, card_id, version, set_id=None, local_id=None):
                raise requests.Timeout("still unavailable")

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            region_dir = root / "data"
            region_dir.mkdir()
            MODULE.write_json_atomic(region_dir / "sets.json", [{"id": "set-1"}])
            MODULE.write_json_atomic(
                region_dir / "cards.json",
                [{"id": "card-1", "set_id": "set-1", "number": "1"}],
            )
            previous_price = {
                "card_id": "card-1",
                "market_source": "fake",
                "price_type": "normal",
                "market": 1.25,
            }
            MODULE.write_json_atomic(region_dir / "prices.json", [previous_price])

            with patch.object(MODULE, "CARD_FETCH_ATTEMPTS", 1):
                region = MODULE.export_region_prices(
                    FailedSource(),
                    "international",
                    root,
                    request_delay=0,
                    workers=1,
                )

            prices = json.loads((region_dir / "prices.json").read_text(encoding="utf-8"))

        self.assertEqual(prices, [previous_price])
        self.assertEqual(region["failedCardCount"], 1)
        self.assertEqual(region["failedCardIds"], ["card-1"])

    def test_failed_full_fetch_preserves_previous_card_and_price(self):
        class PartiallyFailedSource(FakeSource):
            def fetch_set_details(self, set_id, version):
                return {
                    "id": set_id,
                    "name": "First Set",
                    "cards": [{"id": "card-1"}, {"id": "card-2"}],
                }

            def fetch_card_details(self, card_id, version, set_id=None, local_id=None):
                if card_id == "card-2":
                    raise requests.Timeout("still unavailable")
                return super().fetch_card_details(card_id, version, set_id, local_id)

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            region_dir = root / "data"
            region_dir.mkdir()
            previous_card = {
                "id": "card-2",
                "name": "Preserved card",
                "set_id": "set-1",
                "version": "international",
            }
            previous_price = {
                "card_id": "card-2",
                "market_source": "fake",
                "price_type": "normal",
                "market": 2.5,
            }
            MODULE.write_json_atomic(region_dir / "cards.json", [previous_card])
            MODULE.write_json_atomic(region_dir / "prices.json", [previous_price])

            with patch.object(MODULE, "CARD_FETCH_ATTEMPTS", 1):
                region = MODULE.export_region(
                    PartiallyFailedSource(),
                    "international",
                    root,
                    request_delay=0,
                    workers=2,
                )
            with patch.object(
                MODULE,
                "existing_manifest",
                return_value={"tcgdexRelease": "v2.46.0"},
            ):
                manifest = MODULE.publish_manifest(root, [region], "v2.47.0")

            cards = json.loads((region_dir / "cards.json").read_text(encoding="utf-8"))
            prices = json.loads((region_dir / "prices.json").read_text(encoding="utf-8"))

        self.assertEqual([card["id"] for card in cards], ["card-1", "card-2"])
        self.assertEqual(cards[1]["name"], "Preserved card")
        self.assertIn(previous_price, prices)
        self.assertEqual(region["failedCardCount"], 1)
        self.assertEqual(region["failedCardIds"], ["card-2"])
        self.assertEqual(manifest["tcgdexRelease"], "v2.46.0")
        self.assertEqual(manifest["pendingTcgdexRelease"], "v2.47.0")


if __name__ == "__main__":
    unittest.main()
