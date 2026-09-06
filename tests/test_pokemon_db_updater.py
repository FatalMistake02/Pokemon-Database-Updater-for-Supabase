import importlib.util
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import requests


MODULE_PATH = Path(__file__).parents[1] / "pokemon-db-updater.py"
SPEC = importlib.util.spec_from_file_location("pokemon_db_updater", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


class CardDetailFetchTests(unittest.TestCase):
    def test_404_retries_with_encoded_set_and_local_id(self):
        missing = Mock(status_code=404)
        missing.raise_for_status.side_effect = requests.HTTPError("not found")
        found = Mock(status_code=200)
        found.raise_for_status.return_value = None
        found.json.return_value = {"id": "exu-?", "name": "Unown"}

        with patch.object(MODULE, "_upstream_get", side_effect=[missing, found]) as get:
            result = MODULE.fetch_card_details("exu-?", set_id="exu", local_id="?")

        self.assertEqual(result["id"], "exu-?")
        self.assertEqual(
            get.call_args_list[1].args[0],
            "https://api.tcgdex.net/v2/en/cards/exu-%253F",
        )

    def test_already_escaped_local_id_is_not_double_canonicalized(self):
        missing = Mock(status_code=404)
        missing.raise_for_status.side_effect = requests.HTTPError("not found")
        found = Mock(status_code=200)
        found.raise_for_status.return_value = None
        found.json.return_value = {"id": "exu-%3F", "name": "Unown"}

        with patch.object(MODULE, "_upstream_get", side_effect=[missing, found]) as get:
            MODULE.fetch_card_details("exu-%3F", set_id="exu", local_id="%3F")

        self.assertEqual(
            get.call_args_list[1].args[0],
            "https://api.tcgdex.net/v2/en/cards/exu-%253F",
        )

    def test_non_404_does_not_fall_back(self):
        failed = Mock(status_code=503)
        failed.raise_for_status.side_effect = requests.HTTPError("unavailable")

        with patch.object(MODULE, "_upstream_get", return_value=failed) as get:
            with self.assertRaises(requests.HTTPError):
                MODULE.fetch_card_details("exu-?", set_id="exu", local_id="?")

        get.assert_called_once()


class SetListFallbackTests(unittest.TestCase):
    def test_set_ids_are_derived_from_card_index_when_set_list_is_unavailable(self):
        unavailable = Mock(status_code=503)
        unavailable.raise_for_status.side_effect = requests.HTTPError(
            "unavailable",
            response=unavailable,
        )
        card_index = Mock(status_code=200)
        card_index.raise_for_status.return_value = None
        card_index.json.return_value = [
            {"id": "base1-4", "localId": "4", "name": "Charizard"},
            {"id": "base1-5", "localId": "5", "name": "Clefairy"},
            {"id": "tk-xy-n-6", "localId": "6", "name": "Energy"},
            {"id": "invalid", "name": "Missing local ID"},
        ]

        with patch.object(MODULE, "_upstream_get", side_effect=[unavailable, card_index]) as get:
            result = MODULE.fetch_all_sets("international")

        self.assertEqual(result, [{"id": "base1"}, {"id": "tk-xy-n"}])
        self.assertEqual(get.call_args_list[1].args[0], "https://api.tcgdex.net/v2/en/cards")
        self.assertEqual(get.call_args_list[1].kwargs["timeout"], (10, 60))


class JapaneseSetAliasTests(unittest.TestCase):
    def test_japanese_plus_set_detail_uses_card_bearing_tcgdex_id(self):
        found = Mock()
        found.raise_for_status.return_value = None
        found.json.return_value = {"id": "SM1p", "cards": []}

        with patch.object(MODULE, "_upstream_get", return_value=found) as get:
            result = MODULE.fetch_set_details("SM1+", "japan")

        self.assertEqual(result["id"], "SM1p")
        self.assertEqual(
            get.call_args.args[0],
            "https://api.tcgdex.net/v2/ja/sets/SM1p",
        )
        self.assertEqual(get.call_count, 1)

    def test_japanese_plus_set_cards_use_card_bearing_tcgdex_id(self):
        found = Mock()
        found.raise_for_status.return_value = None
        found.json.return_value = {"id": "SM1p", "cards": [{"id": "SM1p-001"}]}

        with patch.object(MODULE, "_upstream_get", return_value=found) as get:
            result = MODULE.fetch_cards_in_set("SM1+", "japan")

        self.assertEqual(result, [{"id": "SM1p-001"}])
        self.assertEqual(
            get.call_args.args[0],
            "https://api.tcgdex.net/v2/ja/sets/SM1p",
        )
        self.assertEqual(get.call_count, 1)

    def test_japanese_card_details_go_directly_to_tcgdex(self):
        found = Mock()
        found.raise_for_status.return_value = None
        found.json.return_value = {"id": "SM1p-001"}

        with patch.object(MODULE, "_upstream_get", return_value=found) as get:
            result = MODULE.fetch_card_details("SM1p-001", "japan")

        self.assertEqual(result["id"], "SM1p-001")
        self.assertEqual(
            get.call_args.args[0],
            "https://api.tcgdex.net/v2/ja/cards/SM1p-001",
        )
        self.assertEqual(get.call_count, 1)

    def test_tcgdex_set_index_collapses_plus_alias(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = [
            {"id": "SM1+", "name": "Sun & Moon"},
            {"id": "SM1p", "name": "Sun & Moon"},
            {"id": "sm2+", "name": "Beyond a New Challenge"},
            {"id": "SM2p", "name": "Beyond a New Challenge"},
            {"id": "SM1M", "name": "Collection Moon"},
        ]

        with patch.object(MODULE, "_upstream_get", return_value=response):
            result = MODULE.fetch_all_sets("japan")

        self.assertEqual([row["id"] for row in result], ["SM1p", "SM2p", "SM1M"])


class PriceTransformationTests(unittest.TestCase):
    def test_null_market_providers_are_ignored(self):
        pricing = {"cardmarket": None, "tcgplayer": None}

        self.assertEqual(MODULE.transform_price_data("card-1", pricing), [])

    def test_null_tcgplayer_variants_are_ignored(self):
        pricing = {
            "cardmarket": None,
            "tcgplayer": {
                "updated": "2026-09-06",
                "unit": "USD",
                "normal": None,
                "reverse": {"lowPrice": 1.25, "marketPrice": 2.5},
                "holofoil": None,
                "1stEdition": None,
            },
        }

        result = MODULE.transform_price_data("card-1", pricing)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["market_source"], "tcgplayer")
        self.assertEqual(result[0]["price_type"], "reverse")
        self.assertEqual(result[0]["low"], 1.25)
        self.assertEqual(result[0]["market"], 2.5)


class RecordingDatabase:
    def __init__(self):
        self.set_batches = []
        self.card_batches = []
        self.price_batches = []

    def upsert_sets(self, rows):
        self.set_batches.append(rows)

    def upsert_cards(self, rows):
        self.card_batches.append(rows)

    def replace_prices_bulk(self, card_ids, rows):
        self.price_batches.append((card_ids, rows))


class FakeCursor:
    def __init__(self):
        self.executemany_calls = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def executemany(self, query, values):
        self.executemany_calls.append((query, values))


class FakeConnection:
    closed = False

    def __init__(self):
        self.current_cursor = FakeCursor()
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.current_cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class CatalogLoadTests(unittest.TestCase):
    def test_catalog_is_loaded_in_batches_with_prices(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "data").mkdir()
            prices_root = root / "private"
            (prices_root / "data").mkdir(parents=True)
            sets = [{"id": "set-1", "name": "Set", "card_count": 3}]
            cards = [{"id": f"card-{number}", "set_id": "set-1"} for number in range(3)]
            prices = [{"card_id": "card-0", "market_source": "fake", "price_type": "normal"}]
            (root / "data" / "sets.json").write_text(json.dumps(sets), encoding="utf-8")
            (root / "data" / "cards.json").write_text(json.dumps(cards), encoding="utf-8")
            (prices_root / "data" / "prices.json").write_text(json.dumps(prices), encoding="utf-8")

            recording = RecordingDatabase()
            previous = MODULE.database
            MODULE.database = recording
            try:
                MODULE.seed_from_catalog(root, "international", batch_size=2, prices_root=prices_root)
            finally:
                MODULE.database = previous

            self.assertEqual([len(rows) for rows in recording.set_batches], [1])
            self.assertEqual([len(rows) for rows in recording.card_batches], [2, 1])
            self.assertEqual([len(ids) for ids, _ in recording.price_batches], [2, 1])
            self.assertEqual(recording.price_batches[0][1][0]["card_id"], "card-0")
            self.assertNotIn("card_count", recording.set_batches[0][0])

    def test_price_only_catalog_load_does_not_write_sets_or_cards(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "data").mkdir()
            sets = [{"id": "set-1", "name": "Set"}]
            cards = [{"id": "card-1", "set_id": "set-1"}]
            prices = [{"card_id": "card-1", "market_source": "fake", "price_type": "normal"}]
            (root / "data" / "sets.json").write_text(json.dumps(sets), encoding="utf-8")
            (root / "data" / "cards.json").write_text(json.dumps(cards), encoding="utf-8")
            (root / "data" / "prices.json").write_text(json.dumps(prices), encoding="utf-8")

            recording = RecordingDatabase()
            previous = MODULE.database
            MODULE.database = recording
            try:
                MODULE.seed_prices_from_catalog(root, "international", batch_size=100)
            finally:
                MODULE.database = previous

            self.assertEqual(recording.set_batches, [])
            self.assertEqual(recording.card_batches, [])
            self.assertEqual(recording.price_batches[0][0], ["card-1"])
            self.assertEqual(recording.price_batches[0][1][0]["card_id"], "card-1")

    def test_neon_card_batch_uses_one_transaction(self):
        connection = FakeConnection()
        target = MODULE.NeonTarget.__new__(MODULE.NeonTarget)
        target.conn = connection
        target.database_url = "unused"
        target.index = 1
        target.upsert_cards([{"id": "card-1"}, {"id": "card-2"}])

        self.assertEqual(connection.commits, 1)
        self.assertEqual(connection.rollbacks, 0)
        self.assertEqual(len(connection.current_cursor.executemany_calls), 1)
        self.assertEqual(len(connection.current_cursor.executemany_calls[0][1]), 2)

    def test_github_pages_catalog_download_is_hash_verified(self):
        files = {
            "data/sets.json": b'[{"id":"set-1"}]',
            "data/cards.json": b'[{"id":"card-1"}]',
            "data/prices.json": b'[]',
        }
        manifest = {
            "version": "catalog-version",
            "regions": {
                "international": {
                    kind: {"path": path, "sha256": hashlib.sha256(content).hexdigest()}
                    for kind, (path, content) in zip(("sets", "cards", "prices"), files.items())
                }
            },
        }
        manifest_response = Mock()
        manifest_response.raise_for_status.return_value = None
        manifest_response.json.return_value = manifest
        file_responses = []
        for content in files.values():
            response = Mock()
            response.raise_for_status.return_value = None
            response.content = content
            file_responses.append(response)

        with tempfile.TemporaryDirectory() as temporary:
            with patch.object(MODULE.requests, "get", side_effect=[manifest_response, *file_responses]):
                result = MODULE.download_catalog(
                    "https://example.test/catalog",
                    Path(temporary),
                    expected_version="catalog-version",
                    retry_delay=0,
                )
            self.assertEqual((result / "data" / "cards.json").read_bytes(), files["data/cards.json"])


if __name__ == "__main__":
    unittest.main()
