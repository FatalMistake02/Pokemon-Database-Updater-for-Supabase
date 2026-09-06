import os
import re
import json
import hashlib
import tempfile
import requests
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
import time
import argparse
from urllib.parse import quote, unquote, urljoin
from tqdm import tqdm

try:
    from supabase import create_client, Client
except ImportError:
    create_client = None
    Client = object

try:
    import psycopg
    from psycopg.types.json import Json
except ImportError:
    psycopg = None
    Json = None

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("Warning: python-dotenv not installed. Install it with: pip install python-dotenv")
    print("Or set environment variables manually.\n")

# Configuration
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
TCGDEX_BASE_URL_EN = "https://api.tcgdex.net/v2/en"
TCGDEX_BASE_URL_JP = "https://api.tcgdex.net/v2/ja"

database = None

SET_COLUMNS = [
    "id", "name", "series", "total", "release_date", "images", "legalities",
    "version", "updated_at",
]

CARD_COLUMNS = [
    "id", "name", "supertype", "subtypes", "hp", "types", "rarity", "set_id",
    "set_name", "set_series", "set_symbol_url", "set_logo_url", "number",
    "artist", "image_small_url", "image_large_url", "legality_standard",
    "legality_expanded", "legality_unlimited", "regulation_mark", "stage",
    "suffix", "description", "tcgplayer_url", "variants", "variants_detailed",
    "version", "updated_at",
]

PRICE_COLUMNS = [
    "card_id", "market_source", "condition", "currency", "low", "mid", "high",
    "average", "market", "trend", "price_type", "last_updated", "updated_at",
]

JSONB_COLUMNS = {
    "images", "legalities", "subtypes", "types", "variants", "variants_detailed",
}

DEFAULT_BATCH_SIZE = 100
_HTTP_THREAD_LOCAL = threading.local()


def _upstream_get(url: str, **kwargs):
    """Reuse upstream HTTP connections independently within each worker thread."""
    session = getattr(_HTTP_THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.headers["User-Agent"] = "Cardly-Catalog-Updater/1.0"
        _HTTP_THREAD_LOCAL.session = session
    return session.get(url, **kwargs)


class SupabaseTarget:
    def __init__(self, url: str, key: str):
        if create_client is None:
            raise RuntimeError("supabase is not installed. Run: pip install -r requirements.txt")
        self.client: Client = create_client(url, key)

    @property
    def name(self) -> str:
        return "Supabase"

    def upsert_set(self, row: Dict) -> None:
        self.upsert_sets([row])

    def upsert_sets(self, rows: List[Dict]) -> None:
        if rows:
            self.client.table("pokemon_sets").upsert(rows).execute()

    def upsert_card(self, row: Dict) -> None:
        self.upsert_cards([row])

    def upsert_cards(self, rows: List[Dict]) -> None:
        if rows:
            self.client.table("cards").upsert(rows).execute()

    def replace_prices(self, card_id: str, rows: List[Dict]) -> None:
        self.replace_prices_bulk([card_id], rows)

    def replace_prices_bulk(self, card_ids: List[str], rows: List[Dict]) -> None:
        if not card_ids:
            return
        self.client.table("card_prices").delete().in_("card_id", card_ids).execute()
        if rows:
            self.client.table("card_prices").insert(rows).execute()

    def fetch_card_ids(self, version: str) -> List[str]:
        rows = self.client.table("cards").select("id").eq("version", version).execute().data
        return [row["id"] for row in rows]


class NeonTarget:
    def __init__(self, database_url: str, index: int = 1):
        if psycopg is None:
            raise RuntimeError("psycopg is not installed. Run: pip install -r requirements.txt")
        self.database_url = database_url
        self.index = index
        self.conn = None

    @property
    def name(self) -> str:
        return f"Neon#{self.index}"

    def _adapt(self, column: str, value):
        if column in JSONB_COLUMNS and value is not None:
            return Json(value)
        return value

    def _connection(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg.connect(self.database_url)
        return self.conn

    def _upsert_many(
        self, table: str, rows: List[Dict], columns: List[str], conflict_columns: List[str]
    ) -> None:
        if not rows:
            return
        placeholders = ", ".join(["%s"] * len(columns))
        column_sql = ", ".join(columns)
        conflict_sql = ", ".join(conflict_columns)
        update_sql = ", ".join(
            [f"{column} = EXCLUDED.{column}" for column in columns if column not in conflict_columns]
        )
        query = f"""
            INSERT INTO {table} ({column_sql})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_sql}) DO UPDATE SET
            {update_sql};
        """

        conn = self._connection()
        try:
            with conn.cursor() as cur:
                cur.executemany(
                    query,
                    [[self._adapt(column, row.get(column)) for column in columns] for row in rows],
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def upsert_set(self, row: Dict) -> None:
        self.upsert_sets([row])

    def upsert_sets(self, rows: List[Dict]) -> None:
        self._upsert_many("pokemon_sets", rows, SET_COLUMNS, ["id"])

    def upsert_card(self, row: Dict) -> None:
        self.upsert_cards([row])

    def upsert_cards(self, rows: List[Dict]) -> None:
        self._upsert_many("cards", rows, CARD_COLUMNS, ["id"])

    def replace_prices(self, card_id: str, rows: List[Dict]) -> None:
        self.replace_prices_bulk([card_id], rows)

    def replace_prices_bulk(self, card_ids: List[str], rows: List[Dict]) -> None:
        if not card_ids:
            return
        conn = self._connection()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM card_prices WHERE card_id = ANY(%s);", (card_ids,))
                if rows:
                    placeholders = ", ".join(["%s"] * len(PRICE_COLUMNS))
                    query = f"INSERT INTO card_prices ({', '.join(PRICE_COLUMNS)}) VALUES ({placeholders});"
                    cur.executemany(
                        query,
                        [[self._adapt(column, row.get(column)) for column in PRICE_COLUMNS] for row in rows],
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def fetch_card_ids(self, version: str) -> List[str]:
        conn = self._connection()
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM cards WHERE version = %s ORDER BY id;", (version,))
            return [row[0] for row in cur.fetchall()]

    def init_schema(self) -> None:
        schema_path = Path(__file__).parent / "schema" / "neon_cards.sql"
        conn = self._connection()
        try:
            with conn.cursor() as cur:
                cur.execute(schema_path.read_text(encoding="utf-8"))
            conn.commit()
        except Exception:
            conn.rollback()
            raise


class MultiTarget:
    def __init__(self, targets):
        self.targets = targets

    @property
    def name(self) -> str:
        return " + ".join(target.name for target in self.targets)

    def upsert_set(self, row: Dict) -> None:
        for target in self.targets:
            target.upsert_set(row)

    def upsert_sets(self, rows: List[Dict]) -> None:
        for target in self.targets:
            target.upsert_sets(rows)

    def upsert_card(self, row: Dict) -> None:
        for target in self.targets:
            target.upsert_card(row)

    def upsert_cards(self, rows: List[Dict]) -> None:
        for target in self.targets:
            target.upsert_cards(rows)

    def replace_prices(self, card_id: str, rows: List[Dict]) -> None:
        for target in self.targets:
            target.replace_prices(card_id, rows)

    def replace_prices_bulk(self, card_ids: List[str], rows: List[Dict]) -> None:
        for target in self.targets:
            target.replace_prices_bulk(card_ids, rows)

    def fetch_card_ids(self, version: str) -> List[str]:
        ids = []
        for target in self.targets:
            ids.extend(target.fetch_card_ids(version))
        return sorted(set(ids))


def build_database_target(target_name: str):
    targets = []
    if target_name in ("supabase", "both"):
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY for Supabase uploads.")
        targets.append(SupabaseTarget(SUPABASE_URL, SUPABASE_KEY))

    if target_name in ("neon", "both"):
        neon_urls = get_neon_database_urls()
        if not neon_urls:
            raise RuntimeError("Missing DATABASE_URL, DATABASE_URL_2, or DATABASE_URLS for Neon uploads.")
        for index, database_url in enumerate(neon_urls, 1):
            targets.append(NeonTarget(database_url, index))

    if len(targets) == 1:
        return targets[0]
    if targets:
        return MultiTarget(targets)
    raise RuntimeError("No database target selected.")


def infer_default_target() -> str:
    has_supabase = bool(SUPABASE_URL and SUPABASE_KEY)
    has_neon = bool(get_neon_database_urls())
    if has_supabase and has_neon:
        return "both"
    if has_neon:
        return "neon"
    return "supabase"


def get_neon_database_urls() -> List[str]:
    raw_values = []

    for key in ("DATABASE_URL", "NEON_DATABASE_URL"):
        value = os.environ.get(key)
        if value:
            raw_values.append(value)

    for key in ("DATABASE_URLS", "NEON_DATABASE_URLS"):
        value = os.environ.get(key)
        if value:
            raw_values.extend(part.strip() for part in re.split(r"[\n,;]+", value))

    for index in range(2, 21):
        values = [
            os.environ.get(f"DATABASE_URL_{index}"),
            os.environ.get(f"NEON_DATABASE_URL_{index}"),
        ]
        for value in values:
            if value:
                raw_values.append(value)

    urls = []
    seen = set()
    for value in raw_values:
        value = value.strip()
        if value and value not in seen:
            urls.append(value)
            seen.add(value)
    return urls


def get_base_url(version: str) -> str:
    """Get the appropriate TCGdex API base URL for a catalog region."""
    return TCGDEX_BASE_URL_JP if version == "japan" else TCGDEX_BASE_URL_EN


def normalize_tcgdex_set_id(set_id: str, version: str) -> str:
    """Map printed Japanese set codes to TCGdex's card-bearing set IDs."""
    value = str(set_id)
    if version == "japan" and re.fullmatch(r"SM\d+\+", value, re.IGNORECASE):
        # TCGdex can expose printed codes such as SM1+ in its set index while
        # the detailed set and card IDs use SM1p. Normalize the alias before
        # requesting a detail route so an orphan index entry cannot abort an
        # otherwise complete catalog export.
        return f"{value[:-1]}p"
    return value


def normalize_tcgdex_set_summaries(sets: List[Dict], version: str) -> List[Dict]:
    """Normalize TCGdex set IDs and collapse aliases that resolve identically."""
    if version != "japan":
        return sets

    normalized = []
    canonical_ids = {
        str(row['id']).casefold()
        for row in sets
        if isinstance(row, dict)
        and row.get('id')
        and normalize_tcgdex_set_id(row['id'], version) == str(row['id'])
    }
    for row in sets:
        if not isinstance(row, dict) or not row.get('id'):
            normalized.append(row)
            continue
        normalized_id = normalize_tcgdex_set_id(row['id'], version)
        if (
            normalized_id != str(row['id'])
            and normalized_id.casefold() in canonical_ids
        ):
            print(f"Skipping duplicate {version} TCGdex set alias: {row['id']}")
            continue
        clean_row = dict(row)
        clean_row['id'] = normalized_id
        normalized.append(clean_row)
    return normalized


def fetch_all_sets(version: str = "international") -> List[Dict]:
    """Fetch all Pokemon card sets from TCGdex."""
    base_url = get_base_url(version)
    print(f"Fetching all {version} sets from TCGdex...")
    response = _upstream_get(f"{base_url}/sets", timeout=(10, 30))
    try:
        response.raise_for_status()
        sets = response.json()
    except requests.RequestException as exc:
        print(
            f"TCGdex set list is unavailable ({exc}); deriving set IDs from the card index..."
        )
        cards_response = _upstream_get(f"{base_url}/cards", timeout=(10, 60))
        cards_response.raise_for_status()
        cards = cards_response.json()
        if not isinstance(cards, list):
            raise RuntimeError("TCGdex card index did not return a list")

        sets_by_id = {}
        for card in cards:
            if not isinstance(card, dict):
                continue
            card_id = str(card.get('id') or '')
            local_id = str(card.get('localId') or '')
            suffix = f"-{local_id}"
            if not card_id or not local_id or not card_id.endswith(suffix):
                continue
            set_id = card_id[:-len(suffix)]
            if set_id:
                sets_by_id.setdefault(set_id, {'id': set_id})
        sets = list(sets_by_id.values())
        if not sets:
            raise RuntimeError("Could not derive any TCGdex set IDs from the card index")
        print(f"Derived {len(sets)} {version} set IDs from the TCGdex card index")

    if not isinstance(sets, list):
        raise RuntimeError("TCGdex set index did not return a list")
    return normalize_tcgdex_set_summaries(sets, version)


def fetch_set_details(set_id: str, version: str = "international") -> Dict:
    """Fetch detailed information for a specific set from TCGdex."""
    base_url = get_base_url(version)
    tcgdex_set_id = normalize_tcgdex_set_id(set_id, version)
    print(f"Fetching {version} set details from TCGdex for: {tcgdex_set_id}")
    response = _upstream_get(f"{base_url}/sets/{quote(tcgdex_set_id, safe='')}", timeout=(10, 30))
    response.raise_for_status()
    return response.json()


def fetch_cards_in_set(set_id: str, version: str = "international") -> List[Dict]:
    """Fetch all cards in a specific set from TCGdex."""
    base_url = get_base_url(version)
    tcgdex_set_id = normalize_tcgdex_set_id(set_id, version)
    print(f"Fetching {version} cards for set from TCGdex: {tcgdex_set_id}")
    response = _upstream_get(f"{base_url}/sets/{quote(tcgdex_set_id, safe='')}", timeout=(10, 30))
    response.raise_for_status()
    set_data = response.json()
    return set_data.get('cards', [])


def fetch_card_details(
    card_id: str,
    version: str = "international",
    set_id: Optional[str] = None,
    local_id: Optional[str] = None,
) -> Dict:
    """Fetch detailed information for a specific card from TCGdex."""
    base_url = get_base_url(version)
    response = _upstream_get(f"{base_url}/cards/{card_id}", timeout=(10, 30))
    try:
        response.raise_for_status()
    except requests.HTTPError:
        # Some valid local IDs contain URL-reserved characters (for example
        # the "?" Unown in set exu). TCGdex stores those characters escaped in
        # the logical card ID, so the percent sign must itself be escaped in
        # the request path: exu-? -> exu-%3F -> /cards/exu-%253F.
        if response.status_code != 404 or not set_id or local_id is None:
            raise
        canonical_local_id = quote(unquote(str(local_id)), safe="")
        canonical_set_id = normalize_tcgdex_set_id(set_id, version)
        canonical_card_id = f"{canonical_set_id}-{canonical_local_id}"
        encoded_card_id = quote(canonical_card_id, safe="")
        response = _upstream_get(f"{base_url}/cards/{encoded_card_id}", timeout=(10, 30))
        response.raise_for_status()
    return response.json()


def transform_set_data_jpn_cards(set_data: Dict) -> Dict:
    """Transform jpn-cards set data to match Supabase schema."""
    # jpn-cards set object fields (examples in docs): id, name, image_url, language, year, date, card_count, printed_count, set_code, uuid
    return {
        'id': set_data.get('id'),
        'name': set_data.get('name'),
        'series': None,  # jpn-cards does not always have a "series" field mapped the same way; keep None or map if you prefer
        'total': set_data.get('card_count') or set_data.get('card_count', None),
        'release_date': set_data.get('date') or set_data.get('year'),
        'images': {
            'logo': set_data.get('image_url'),
            'symbol': None
        },
        'legalities': None,
        'version': 'japan',
        'updated_at': datetime.now().isoformat()
    }


def transform_card_data_jpn_cards(card_data: Dict) -> Dict:
    """Transform jpn-cards card data to match Supabase schema."""
    # jpn-cards card objects usually include keys like:
    # id, setData (dict), name, types, hp, evolvesFrom, effect (array), attacks (array), rules, weaknesses, supertype, subtypes, rarity, cardLegalities, artist, imageUrl, cardUrl, sequenceNumber, printedNumber, uuid
    set_info = card_data.get('setData', {}) if isinstance(card_data.get('setData', {}), dict) else {}

    # Image fields: jpn-cards uses `imageUrl` for the card image in examples
    image_small_url = card_data.get('imageUrl') or card_data.get('image_url')
    # jpn-cards doesn't always provide a hires variant; reuse imageUrl if missing
    image_large_url = image_small_url

    # Card legalities in jpn-cards examples are in `cardLegalities`
    card_legal = card_data.get('cardLegalities') or {}
    variants_detailed = card_data.get('variants_detailed')

    return {
        'id': card_data.get('id'),
        'name': card_data.get('name'),
        'supertype': card_data.get('supertype'),
        'subtypes': card_data.get('subtypes'),
        'hp': str(card_data.get('hp')) if card_data.get('hp') else None,
        'types': card_data.get('types'),
        'rarity': card_data.get('rarity'),
        'set_id': set_info.get('id') if isinstance(set_info, dict) else None,
        'set_name': set_info.get('name') if isinstance(set_info, dict) else None,
        'set_series': None,
        'set_symbol_url': set_info.get('image_url') if isinstance(set_info, dict) else None,
        'set_logo_url': set_info.get('image_url') if isinstance(set_info, dict) else None,
        'number': card_data.get('printedNumber') or card_data.get('sequenceNumber') or card_data.get('printedNumber'),
        'artist': card_data.get('artist'),
        'image_small_url': image_small_url,
        'image_large_url': image_large_url,
        'legality_standard': card_legal.get('Standard') if isinstance(card_legal, dict) else None,
        'legality_expanded': card_legal.get('Expanded') if isinstance(card_legal, dict) else None,
        'legality_unlimited': card_legal.get('Unlimited') if isinstance(card_legal, dict) else None,
        'regulation_mark': None,
        'stage': None,
        'suffix': None,
        'description': None,
        'tcgplayer_url': card_data.get('cardUrl'),
        'variants': None,
        'variants_detailed': variants_detailed,
        'version': 'japan',
        'updated_at': datetime.now().isoformat()
    }


def transform_set_data(set_data: Dict, version: str = "international", source: str = "tcgdex") -> Dict:
    """Transform set data to match Supabase schema."""
    # Use jpn-cards transformer if data is from jpn-cards
    if source == "jpn-cards":
        return transform_set_data_jpn_cards(set_data)

    # Helper to ensure set images are returned as a dict with .png extensions
    def _ensure_png_url_local(url: Optional[str]) -> Optional[str]:
        if not url:
            return None
        # preserve query string
        if '?' in url:
            base, query = url.split('?', 1)
            query = '?' + query
        else:
            base, query = url, ''
        last_segment = base.rstrip('/').split('/')[-1]
        if '.' in last_segment:
            return base + query
        return base + '.png' + query

    return {
        'id': set_data.get('id'),
        'name': set_data.get('name'),
        'series': set_data.get('serie', {}).get('name') if isinstance(set_data.get('serie'), dict) else set_data.get('serie'),
        'total': set_data.get('cardCount', {}).get('total') if isinstance(set_data.get('cardCount'), dict) else set_data.get('total'),
        'release_date': set_data.get('releaseDate'),
        'images': {
            'logo': _ensure_png_url_local(set_data.get('logo')) if set_data.get('logo') else None,
            'symbol': _ensure_png_url_local(set_data.get('symbol')) if set_data.get('symbol') else None
        },
        'legalities': set_data.get('legal'),
        'version': version,
        'updated_at': datetime.now().isoformat()
    }


def transform_card_data(card_data: Dict, version: str = "international", source: str = "tcgdex") -> Dict:
    """Transform card data to match Supabase schema."""
    # Use jpn-cards transformer if data is from jpn-cards
    if source == "jpn-cards":
        return transform_card_data_jpn_cards(card_data)

    set_info = card_data.get('set', {})
    legalities = card_data.get('legal', {})

    # Extract TCGPlayer URL if available
    tcgplayer_url = None
    if 'tcgplayer' in card_data:
        tcgplayer_url = card_data['tcgplayer'].get('url')

    # Construct proper image URLs
    # TCGdex returns base URLs without extensions
    # Format: {base_url}/{quality}.{extension}
    base_image_url = card_data.get('image')
    image_small_url = None
    image_large_url = None

    if base_image_url:
        # Small image: low quality, webp format (245x337)
        image_small_url = f"{base_image_url}/low.webp"
        # Large image: high quality, webp format (600x825)
        image_large_url = f"{base_image_url}/high.webp"
    else:
        # Fallback to images.pokemontcg.io when TCGdex image is missing.
        # Use set id and card number if available to construct URLs.
        set_id_val = set_info.get('id') if isinstance(set_info, dict) else None
        card_num = card_data.get('localId') or card_data.get('number') or None
        if not card_num:
            # try extracting trailing part of card id like 'base1-1'
            cid = card_data.get('id')
            if isinstance(cid, str) and '-' in cid:
                card_num = cid.split('-')[-1]
        if set_id_val and card_num:
            image_small_url = f"https://images.pokemontcg.io/{set_id_val}/{card_num}.png"
            image_large_url = f"https://images.pokemontcg.io/{set_id_val}/{card_num}_hires.png"

    # Helper used to ensure set image urls include .png if missing
    def _ensure_png_for_set(val: Optional[str]) -> Optional[str]:
        if not val:
            return None
        if '?' in val:
            base, query = val.split('?', 1)
            query = '?' + query
        else:
            base, query = val, ''
        last = base.rstrip('/').split('/')[-1]
        if '.' in last:
            return base + query
        return base + '.png' + query

    variants_detailed = None
    if isinstance(card_data.get('variants_detailed'), list):
        variants_detailed = {}
        for item in card_data['variants_detailed']:
            if not isinstance(item, dict):
                continue
            variant_type = item.get('type')
            variant_size = item.get('size')
            if not variant_type or not variant_size:
                continue

            existing = variants_detailed.get(variant_type)
            if existing is None:
                variants_detailed[variant_type] = variant_size
            elif existing != variant_size:
                if isinstance(existing, list):
                    if variant_size not in existing:
                        existing.append(variant_size)
                else:
                    variants_detailed[variant_type] = [existing, variant_size]

        if not variants_detailed:
            variants_detailed = None

    return {
        'id': card_data.get('id'),
        'name': card_data.get('name'),
        'supertype': card_data.get('category'),
        'subtypes': card_data.get('dexId'),  # Note: TCGdex structure may differ
        'hp': str(card_data.get('hp')) if card_data.get('hp') else None,
        'types': card_data.get('types'),
        'rarity': card_data.get('rarity'),
        'set_id': set_info.get('id') if isinstance(set_info, dict) else None,
        'set_name': set_info.get('name') if isinstance(set_info, dict) else None,
        'set_series': set_info.get('serie') if isinstance(set_info, dict) else None,
        'set_symbol_url': _ensure_png_for_set(set_info.get('symbol')) if isinstance(set_info, dict) else None,
        'set_logo_url': _ensure_png_for_set(set_info.get('logo')) if isinstance(set_info, dict) else None,
        'number': card_data.get('localId'),
        'artist': card_data.get('illustrator'),
        'image_small_url': image_small_url,
        'image_large_url': image_large_url,
        'legality_standard': legalities.get('standard') if isinstance(legalities, dict) else None,
        'legality_expanded': legalities.get('expanded') if isinstance(legalities, dict) else None,
        'legality_unlimited': legalities.get('unlimited') if isinstance(legalities, dict) else None,
        'regulation_mark': card_data.get('regulationMark'),
        'stage': card_data.get('stage'),
        'suffix': card_data.get('suffix'),
        'description': card_data.get('effect') or card_data.get('description'),
        'tcgplayer_url': tcgplayer_url,
        'variants': card_data.get('variants'),
        'variants_detailed': variants_detailed,
        'version': version,
        'updated_at': datetime.now().isoformat()
    }


def transform_price_data(card_id: str, pricing_data: Dict) -> List[Dict]:
    """Transform TCGdex pricing data to match Supabase schema."""
    price_records = []

    # Process Cardmarket pricing
    cm = pricing_data.get('cardmarket')
    if isinstance(cm, dict):
        updated = cm.get('updated')

        # Regular/average prices
        price_records.append({
            'card_id': card_id,
            'market_source': 'cardmarket',
            'condition': 'average',
            'currency': cm.get('unit', 'EUR'),
            'low': cm.get('low'),
            'average': cm.get('avg'),
            'trend': str(cm.get('trend')),
            'price_type': 'normal',
            'last_updated': updated,
            'updated_at': datetime.now().isoformat()
        })

        # Holo prices
        if 'avg-holo' in cm or 'low-holo' in cm:
            price_records.append({
                'card_id': card_id,
                'market_source': 'cardmarket',
                'condition': 'average',
                'currency': cm.get('unit', 'EUR'),
                'low': cm.get('low-holo'),
                'average': cm.get('avg-holo'),
                'trend': str(cm.get('trend-holo')),
                'price_type': 'holo',
                'last_updated': updated,
                'updated_at': datetime.now().isoformat()
            })

    # Process TCGPlayer pricing
    tcp = pricing_data.get('tcgplayer')
    if isinstance(tcp, dict):
        updated = tcp.get('updated')
        unit = tcp.get('unit', 'USD')

        # Normal prices
        normal = tcp.get('normal')
        if isinstance(normal, dict):
            price_records.append({
                'card_id': card_id,
                'market_source': 'tcgplayer',
                'condition': 'normal',
                'currency': unit,
                'low': normal.get('lowPrice'),
                'mid': normal.get('midPrice'),
                'high': normal.get('highPrice'),
                'market': normal.get('marketPrice'),
                'price_type': 'normal',
                'last_updated': updated,
                'updated_at': datetime.now().isoformat()
            })

        # Reverse holo prices
        reverse = tcp.get('reverse')
        if isinstance(reverse, dict):
            price_records.append({
                'card_id': card_id,
                'market_source': 'tcgplayer',
                'condition': 'normal',
                'currency': unit,
                'low': reverse.get('lowPrice'),
                'mid': reverse.get('midPrice'),
                'high': reverse.get('highPrice'),
                'market': reverse.get('marketPrice'),
                'price_type': 'reverse',
                'last_updated': updated,
                'updated_at': datetime.now().isoformat()
            })

        # Holofoil prices
        holo = tcp.get('holofoil')
        if isinstance(holo, dict):
            price_records.append({
                'card_id': card_id,
                'market_source': 'tcgplayer',
                'condition': 'normal',
                'currency': unit,
                'low': holo.get('lowPrice'),
                'mid': holo.get('midPrice'),
                'high': holo.get('highPrice'),
                'market': holo.get('marketPrice'),
                'price_type': 'holofoil',
                'last_updated': updated,
                'updated_at': datetime.now().isoformat()
            })

        # 1st Edition prices
        first_ed = tcp.get('1stEdition')
        if isinstance(first_ed, dict):
            price_records.append({
                'card_id': card_id,
                'market_source': 'tcgplayer',
                'condition': 'normal',
                'currency': unit,
                'low': first_ed.get('lowPrice'),
                'mid': first_ed.get('midPrice'),
                'high': first_ed.get('highPrice'),
                'market': first_ed.get('marketPrice'),
                'price_type': '1stEdition',
                'last_updated': updated,
                'updated_at': datetime.now().isoformat()
            })

    return price_records


def detect_data_source(data: Dict) -> str:
    """Detect whether data came from jpn-cards or tcgdex based on structure."""
    # jpn-cards responses commonly include 'setData' (for cards) or 'imageUrl'/'cardUrl' keys.
    if isinstance(data, dict) and ('setData' in data or 'imageUrl' in data or 'cardUrl' in data):
        return "jpn-cards"
    # jpn-cards sets endpoint returns a list of set objects (with 'card_count' etc.)
    if isinstance(data, dict) and ('card_count' in data or 'set_code' in data):
        return "jpn-cards"
    return "tcgdex"


def batched(rows: List[Dict], batch_size: int):
    """Yield bounded chunks so hosted database APIs receive manageable payloads."""
    for start in range(0, len(rows), batch_size):
        yield rows[start:start + batch_size]


def prepare_catalog_rows(rows: List[Dict], columns: List[str], version: str) -> List[Dict]:
    """Validate and restrict public catalog rows to columns accepted by the database."""
    prepared = []
    updated_at = datetime.now(timezone.utc).isoformat()
    for row in rows:
        if not isinstance(row, dict) or not row.get("id"):
            raise ValueError(f"The {version} catalog contains a row without an ID")
        clean = {column: row.get(column) for column in columns}
        clean["version"] = version
        clean["updated_at"] = row.get("updated_at") or updated_at
        prepared.append(clean)
    return prepared


def load_catalog_region(catalog_root: Path, version: str):
    directory = "data" if version == "international" else "data-asia"
    region_root = catalog_root / directory
    try:
        sets = json.loads((region_root / "sets.json").read_text(encoding="utf-8"))
        cards = json.loads((region_root / "cards.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"Could not load the {version} catalog from {region_root}: {exc}") from exc
    if not isinstance(sets, list) or not isinstance(cards, list) or not sets or not cards:
        raise RuntimeError(f"The {version} catalog is incomplete")
    return (
        prepare_catalog_rows(sets, SET_COLUMNS, version),
        prepare_catalog_rows(cards, CARD_COLUMNS, version),
    )


def load_catalog_prices(prices_root: Optional[Path], version: str) -> List[Dict]:
    if prices_root is None:
        return []
    directory = "data" if version == "international" else "data-asia"
    path = prices_root / directory / "prices.json"
    try:
        rows = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"Could not load catalog prices from {path}: {exc}") from exc
    if not isinstance(rows, list):
        raise RuntimeError(f"Catalog prices must be a list: {path}")
    updated_at = datetime.now(timezone.utc).isoformat()
    return [
        {
            **{column: row.get(column) for column in PRICE_COLUMNS},
            "updated_at": row.get("updated_at") or updated_at,
        }
        for row in rows
        if isinstance(row, dict) and row.get("card_id")
    ]


def download_catalog(
    base_url: str,
    destination: Path,
    expected_version: Optional[str] = None,
    attempts: int = 60,
    retry_delay: float = 10,
) -> Path:
    """Download and hash-check a complete catalog published on GitHub Pages."""
    base_url = base_url.rstrip("/") + "/"
    manifest = None
    for attempt in range(1, attempts + 1):
        response = requests.get(
            urljoin(base_url, "manifest.json"),
            params={"expected": expected_version} if expected_version else None,
            timeout=30,
        )
        response.raise_for_status()
        manifest = response.json()
        if not expected_version or manifest.get("version") == expected_version:
            break
        if attempt == attempts:
            raise RuntimeError(
                f"GitHub Pages did not publish catalog {expected_version} after {attempts} checks"
            )
        print(f"Waiting for GitHub Pages catalog {expected_version[:12]} ({attempt}/{attempts})")
        time.sleep(retry_delay)

    if not isinstance(manifest, dict) or not isinstance(manifest.get("regions"), dict):
        raise RuntimeError("The catalog manifest is invalid")
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False), encoding="utf-8"
    )
    version = str(manifest.get("version") or "")
    for region in manifest["regions"].values():
        for kind in ("sets", "cards", "prices"):
            metadata = region.get(kind) if isinstance(region, dict) else None
            relative = metadata.get("path") if isinstance(metadata, dict) else None
            expected_hash = metadata.get("sha256") if isinstance(metadata, dict) else None
            if not relative or not expected_hash:
                raise RuntimeError(f"The catalog manifest is missing {kind} metadata")
            relative_path = Path(relative)
            if relative_path.is_absolute() or ".." in relative_path.parts:
                raise RuntimeError(f"Unsafe catalog path: {relative}")
            file_response = requests.get(
                urljoin(base_url, relative_path.as_posix()),
                params={"version": version},
                timeout=60,
            )
            file_response.raise_for_status()
            content = file_response.content
            actual_hash = hashlib.sha256(content).hexdigest()
            if actual_hash != expected_hash:
                raise RuntimeError(f"Hash mismatch downloading {relative}")
            output_path = destination / relative_path
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(content)
    print(f"Downloaded and verified catalog {version[:12]} from {base_url}")
    return destination


def seed_from_catalog(
    catalog_root: Path,
    version: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    prices_root: Optional[Path] = None,
) -> None:
    """Bulk-load a completed catalog. No upstream API requests are made here."""
    sets, cards = load_catalog_region(catalog_root, version)
    prices = load_catalog_prices(prices_root or catalog_root, version)
    known_card_ids = {row["id"] for row in cards}
    unknown_prices = {row["card_id"] for row in prices} - known_card_ids
    if unknown_prices:
        raise RuntimeError(f"Catalog prices reference unknown cards: {sorted(unknown_prices)[0]}")

    print(f"Bulk loading {version}: {len(sets)} sets, {len(cards)} cards, {len(prices)} prices")
    for rows in batched(sets, batch_size):
        database.upsert_sets(rows)
    for rows in tqdm(list(batched(cards, batch_size)), desc=f"Card batches ({version})", unit="batch"):
        database.upsert_cards(rows)

    prices_by_card = {}
    for row in prices:
        prices_by_card.setdefault(row["card_id"], []).append(row)
    for card_rows in tqdm(list(batched(cards, batch_size)), desc=f"Price batches ({version})", unit="batch"):
        card_ids = [row["id"] for row in card_rows]
        batch_prices = [row for card_id in card_ids for row in prices_by_card.get(card_id, [])]
        database.replace_prices_bulk(card_ids, batch_prices)

    print(f"Completed bulk load for {version}")


def seed_prices_from_catalog(
    catalog_root: Path,
    version: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    prices_root: Optional[Path] = None,
) -> None:
    """Replace prices from a catalog without writing any set or card rows."""
    _, cards = load_catalog_region(catalog_root, version)
    prices = load_catalog_prices(prices_root or catalog_root, version)
    known_card_ids = {row["id"] for row in cards}
    unknown_prices = {row["card_id"] for row in prices} - known_card_ids
    if unknown_prices:
        raise RuntimeError(f"Catalog prices reference unknown cards: {sorted(unknown_prices)[0]}")

    prices_by_card = {}
    for row in prices:
        prices_by_card.setdefault(row["card_id"], []).append(row)
    print(f"Loading {len(prices)} prices for {len(cards)} existing {version} cards")
    for card_rows in tqdm(
        list(batched(cards, batch_size)),
        desc=f"Price batches ({version})",
        unit="batch",
    ):
        card_ids = [row["id"] for row in card_rows]
        batch_prices = [row for card_id in card_ids for row in prices_by_card.get(card_id, [])]
        database.replace_prices_bulk(card_ids, batch_prices)
    print(f"Completed price-only catalog load for {version}")


def upsert_set(set_data: Dict, version: str = "international") -> bool:
    """Insert or update a set."""
    try:
        source = detect_data_source(set_data)
        transformed_data = transform_set_data(set_data, version, source)
        database.upsert_set(transformed_data)
        tqdm.write(f"✓ Upserted {version} set: {transformed_data['name']} (source: {source})")
        return True
    except Exception as e:
        tqdm.write(f"✗ Error upserting {version} set {set_data.get('id')}: {e}")
        return False


def upsert_card(card_data: Dict, version: str = "international") -> bool:
    """Insert or update a card."""
    try:
        source = detect_data_source(card_data)
        transformed_data = transform_card_data(card_data, version, source)
        database.upsert_card(transformed_data)
        tqdm.write(f"✓ Upserted {version} card: {transformed_data['name']} ({transformed_data['id']}) (source: {source})")
        return True
    except Exception as e:
        tqdm.write(f"✗ Error upserting {version} card {card_data.get('id')}: {e}")
        return False


def upsert_prices(card_id: str, pricing_data: Dict) -> int:
    """Insert or update card prices."""
    if not pricing_data:
        return 0

    try:
        price_records = transform_price_data(card_id, pricing_data)

        if not price_records:
            return 0
        database.replace_prices(card_id, price_records)
        tqdm.write(f"  ✓ Inserted {len(price_records)} price records for card {card_id}")
        return len(price_records)
    except Exception as e:
        tqdm.write(f"  ✗ Error upserting prices for card {card_id}: {e}")
        return 0


def seed_all_data(limit_sets: Optional[int] = None, version: str = "international"):
    """
    Main function to seed all data from APIs to Supabase.
    Both international and Japanese cards are fetched from TCGdex.
    
    Args:
        limit_sets: Optional limit on number of sets to process (for testing)
        version: "international" or "japan"
    """
    print("="*60)
    print(f"Starting seeding process ({version})")
    print("API: TCGdex")
    print("="*60)
    
    # Fetch all sets
    sets = fetch_all_sets(version)
    print(f"\nFound {len(sets)} {version} sets")

    # Filter out Pokémon TCG Pocket sets (by name or id)
    def is_pocket_set(set_summary):
        set_id = str(set_summary.get('id') or '').lower()
        set_name = str(set_summary.get('name') or '').lower()

        return (
            "pocket" in set_id
            or "pocket" in set_name
            or "ポケモンカードゲームスカーレット&バイオレット" in set_name
            or "ポケモンカードゲーム" in set_name
        )



    sets = [s for s in sets if not is_pocket_set(s)]
    print(f"After filtering, {len(sets)} sets remain (Pocket sets excluded)")

    if limit_sets:
        sets = sets[:limit_sets]
        print(f"Limiting to {limit_sets} sets for testing")

    sets_success = 0
    cards_success = 0
    cards_failed = 0
    prices_success = 0

    # Process each set (show progress bar)
    for i, set_summary in enumerate(tqdm(sets, desc=f"Sets ({version})", unit="set"), 1):
        set_id = set_summary.get('id')
        tqdm.write(f"\n[{i}/{len(sets)}] Processing {version} set: {set_id}")

        try:
            # Fetch detailed set information
            set_details = fetch_set_details(set_id, version)
            if set_details is None:
                print(f"✗ Could not fetch set details for {set_id}")
                continue
                
            # Check fetched set name/serie for 'pocket'
            name = (set_details.get('name') or '').lower()
            serie = set_details.get('serie') or set_details.get('series')
            if isinstance(serie, dict):
                serie_name = (serie.get('name') or '').lower()
            else:
                serie_name = (serie or '').lower()
            if 'pocket' in name or 'pocket' in serie_name:
                print(f"Skipping set '{set_id}' (Pocket set detected by name or serie)")
                continue

            # Upsert set
            if upsert_set(set_details, version):
                sets_success += 1

            # Fetch and process all cards in the set
            if version == "japan":
                cards = fetch_cards_in_set(set_id, version)
            else:
                cards = set_details.get('cards', [])

            tqdm.write(f"Found {len(cards)} cards in set {set_id}")
            card_rows = []
            prices_by_card = {}
            for j, card_summary in enumerate(tqdm(cards, desc=f"Cards in {set_id}", unit="card", leave=False), 1):
                card_id = card_summary.get('id')

                try:
                    # Fetch detailed card information
                    card_details = fetch_card_details(card_id, version)
                    if card_details is None:
                        print(f"✗ Could not fetch card details for {card_id}")
                        cards_failed += 1
                        continue

                    source = detect_data_source(card_details)
                    card_row = transform_card_data(card_details, version, source)
                    card_rows.append(card_row)
                    pricing = card_details.get('pricing')
                    if isinstance(pricing, dict):
                        prices_by_card[card_row['id']] = transform_price_data(card_row['id'], pricing)

                    # Rate limiting - be respectful to the API
                    if j % 10 == 0:
                        tqdm.write(f"  Progress: {j}/{len(cards)} cards processed")
                        time.sleep(0.5)

                except Exception as e:
                    print(f"✗ Error processing card {card_id}: {e}")
                    cards_failed += 1
                    continue

            # Database writes are deliberately deferred until the set has been fetched.
            # This turns hundreds of hosted-Postgres commits into one bounded transaction.
            for rows in batched(card_rows, DEFAULT_BATCH_SIZE):
                database.upsert_cards(rows)
                cards_success += len(rows)
                card_ids = [row['id'] for row in rows]
                price_rows = [
                    price
                    for current_card_id in card_ids
                    for price in prices_by_card.get(current_card_id, [])
                ]
                database.replace_prices_bulk(card_ids, price_rows)
                prices_success += len(price_rows)
            tqdm.write(f"Bulk upserted {len(card_rows)} cards for set {set_id}")

            # Pause between sets
            time.sleep(1)

        except Exception as e:
            print(f"✗ Error processing set {set_id}: {e}")
            continue

    # Print summary
    print("\n" + "="*60)
    print(f"Seeding Summary ({version})")
    print("="*60)
    print(f"Sets processed: {sets_success}/{len(sets)}")
    print(f"Cards succeeded: {cards_success}")
    print(f"Cards failed: {cards_failed}")
    print(f"Price records created: {prices_success}")
    print("="*60)


def seed_single_set(set_id: str, version: str = "international"):
    """Seed a single set and its cards (useful for testing)."""
    # Skip if set_id or set name contains 'pocket'
    if 'pocket' in (set_id or '').lower():
        tqdm.write(f"Skipping set '{set_id}' (Pocket set detected)")
        return

    tqdm.write(f"Seeding single {version} set: {set_id}")
    try:
        # Fetch and upsert set
        set_details = fetch_set_details(set_id, version)
        if set_details is None:
            print(f"✗ Could not fetch set details for {set_id}")
            return
            
        # Also skip if set name or serie contains 'pocket'
        name = (set_details.get('name') or '').lower()
        serie = set_details.get('serie') or set_details.get('series')
        if isinstance(serie, dict):
            serie_name = (serie.get('name') or '').lower()
        else:
            serie_name = (serie or '').lower()
        if 'pocket' in name or 'pocket' in serie_name:
            print(f"Skipping set '{set_id}' (Pocket set detected by name or serie)")
            return
        upsert_set(set_details, version)

        # Fetch and upsert cards
        cards = set_details.get('cards', [])
        tqdm.write(f"Found {len(cards)} cards")

        for card_summary in tqdm(cards, desc=f"Single set {set_id} cards", unit="card"):
            card_id = card_summary.get('id')
            card_details = fetch_card_details(card_id, version)
            if card_details is None:
                tqdm.write(f"✗ Could not fetch card details for {card_id}")
                continue
            upsert_card(card_details, version)

            # Upsert pricing data if available
            if 'pricing' in card_details:
                upsert_prices(card_id, card_details['pricing'])

            time.sleep(0.3)

        print(f"✓ Successfully seeded {version} set {set_id}")

    except Exception as e:
        print(f"✗ Error seeding {version} set {set_id}: {e}")


def seed_both_versions(limit_sets: Optional[int] = None):
    """Seed both international and Japanese versions."""
    print("\n" + "="*60)
    print("SEEDING BOTH INTERNATIONAL AND JAPANESE VERSIONS")
    print("="*60 + "\n")
    
    # Seed international first
    seed_all_data(limit_sets=limit_sets, version="international")
    
    print("\n" + "="*60)
    print("Pausing before Japanese seeding...")
    print("="*60)
    time.sleep(2)
    
    # Then seed Japanese
    seed_all_data(limit_sets=limit_sets, version="japan")
    
    print("\n" + "="*60)
    print("COMPLETED BOTH VERSIONS")
    print("="*60)

def update_card_prices(card_id: str, version: str = "international", show_prices: bool = False) -> int:
    """Fetch and update prices for a single card."""
    try:
        card_details = fetch_card_details(card_id, version)
        if not card_details:
            return 0

        pricing = card_details.get("pricing")
        if not pricing:
            return 0

        if show_prices:
            print(f"\n📊 Pricing data for {card_id}:")
            print(pricing)

        return upsert_prices(card_id, pricing)

    except Exception as e:
        print(f"✗ Failed to update prices for {card_id}: {e}")
        return 0

def seed_prices_only(version: str = "international", show_prices: bool = False):
    """Update prices for all cards already in the database."""
    print("=" * 60)
    print(f"Updating prices only ({version})")
    print("=" * 60)

    cards = [{"id": card_id} for card_id in database.fetch_card_ids(version)]

    tqdm.write(f"Found {len(cards)} cards to update prices for")

    total_prices = 0

    for i, card in enumerate(tqdm(cards, desc=f"Price updates ({version})", unit="card"), 1):
        card_id = card["id"]
        tqdm.write(f"[{i}/{len(cards)}] Updating prices for {card_id}")

        count = update_card_prices(card_id, version, show_prices)
        total_prices += count

        if i % 10 == 0:
            time.sleep(0.5)

    tqdm.write("\n✓ Price update complete")
    tqdm.write(f"Total price records inserted: {total_prices}")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pokemon DB updater (sets, cards, prices)")
    parser.add_argument("--set", "-s", dest="set_id", help="Seed a single set by id (test mode)")
    parser.add_argument("--limit", "-l", dest="limit", type=int, help="Limit number of sets to process")
    parser.add_argument("--version", "-v", dest="version", choices=["international", "japan", "both"], 
                        default="international", help="Which version to seed (default: international)")
    parser.add_argument(
    "--prices-only",
    action="store_true",
    help="Update prices only (no sets or cards)"
    )

    parser.add_argument(
        "--show-prices",
        action="store_true",
        help="Print pricing data when fetched"
    )
    parser.add_argument(
        "--db-target",
        choices=["supabase", "neon", "both"],
        default=infer_default_target(),
        help="Database upload target. Defaults to both when Supabase and Neon env vars are present."
    )
    parser.add_argument(
        "--init-neon-schema",
        action="store_true",
        help="Create the Neon card tables from schema/neon_cards.sql, then exit."
    )
    parser.add_argument(
        "--catalog",
        type=Path,
        help="Bulk-load sets and cards from a completed Cardly catalog instead of fetching APIs.",
    )
    parser.add_argument(
        "--catalog-url",
        help="Download and verify the catalog from a GitHub Pages base URL before bulk loading.",
    )
    parser.add_argument(
        "--expected-catalog-manifest",
        type=Path,
        help="Wait until --catalog-url publishes the version in this local manifest.",
    )
    parser.add_argument(
        "--prices-catalog",
        type=Path,
        help="Optional alternate root for prices.json files (defaults to --catalog).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Rows per database request/transaction for catalog loads (default: {DEFAULT_BATCH_SIZE}).",
    )

    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("--batch-size must be at least 1")
    if args.catalog and args.catalog_url:
        parser.error("Use either --catalog or --catalog-url, not both")
    if args.expected_catalog_manifest and not args.catalog_url:
        parser.error("--expected-catalog-manifest requires --catalog-url")
    if args.prices_catalog and not (args.catalog or args.catalog_url):
        parser.error("--prices-catalog requires --catalog or --catalog-url")
    if (args.catalog or args.catalog_url) and (args.set_id or args.limit):
        parser.error("Catalog loading cannot be combined with --set or --limit")
    try:
        database = build_database_target(args.db_target)
    except Exception as e:
        print("=" * 60)
        print("ERROR: Could not initialize database target")
        print("=" * 60)
        print(e)
        print("\nFor Neon, add this to .env:")
        print('  DATABASE_URL="postgresql://USER:PASSWORD@HOST/dbname?sslmode=require&channel_binding=require"')
        print("\nFor Supabase, keep:")
        print("  SUPABASE_URL=your-project-url")
        print("  SUPABASE_KEY=your-service-role-or-anon-key")
        print("=" * 60)
        exit(1)

    print(f"✓ Database target ready: {database.name}\n")

    if args.init_neon_schema:
        if args.db_target == "supabase":
            print("Schema initialization is only for Neon. Use --db-target neon or both.")
            exit(1)
        if isinstance(database, MultiTarget):
            neon_targets = [target for target in database.targets if isinstance(target, NeonTarget)]
        else:
            neon_targets = [database]
        for neon_target in neon_targets:
            neon_target.init_schema()
        print("✓ Neon schema initialized from schema/neon_cards.sql")
        exit(0)

    if args.catalog or args.catalog_url:
        temporary = tempfile.TemporaryDirectory() if args.catalog_url else None
        try:
            if args.catalog_url:
                expected_version = None
                if args.expected_catalog_manifest:
                    expected_manifest = json.loads(
                        args.expected_catalog_manifest.read_text(encoding="utf-8")
                    )
                    expected_version = expected_manifest.get("version")
                catalog_root = download_catalog(
                    args.catalog_url,
                    Path(temporary.name),
                    expected_version=expected_version,
                )
            else:
                catalog_root = args.catalog.resolve()
            versions = ("international", "japan") if args.version == "both" else (args.version,)
            for version in versions:
                load_function = seed_prices_from_catalog if args.prices_only else seed_from_catalog
                load_function(
                    catalog_root,
                    version,
                    batch_size=args.batch_size,
                    prices_root=args.prices_catalog.resolve() if args.prices_catalog else None,
                )
        finally:
            if temporary is not None:
                temporary.cleanup()

    elif args.prices_only:
        if args.version == "both":
            seed_prices_only("international", show_prices=args.show_prices)
            time.sleep(2)
            seed_prices_only("japan", show_prices=args.show_prices)
        else:
            seed_prices_only(args.version, show_prices=args.show_prices)

    elif args.set_id:
        seed_single_set(args.set_id, version=args.version if args.version != "both" else "international")

    elif args.version == "both":
        seed_both_versions(limit_sets=args.limit)

    else:
        seed_all_data(limit_sets=args.limit, version=args.version)
