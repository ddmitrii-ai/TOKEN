import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup

# ----------------------------
# Конфиг
# ----------------------------

TOKEN_MAP_PATH = os.getenv("TOKEN_MAP_PATH", "token_map.json")

LISTEDON_SOURCES = [
    ("mxc", "MEXC"),
    ("gate", "Gate.io"),
]

MAX_PAGES_PER_EXCHANGE = int(os.getenv("MAX_PAGES_PER_EXCHANGE", "10"))

# Пока не режем по возрасту – сначала просто научимся парсить
USE_DATE_WINDOW = False
MIN_AGE_DAYS = int(os.getenv("MIN_AGE_DAYS", "7"))
MAX_AGE_DAYS = int(os.getenv("MAX_AGE_DAYS", "90"))

MIN_MCAP_USD = float(os.getenv("MIN_MCAP_USD", "3000000"))       # 3M
MAX_MCAP_USD = float(os.getenv("MAX_MCAP_USD", "1000000000"))    # 1B

TARGET_EXCHANGES = {
    "BINANCE",
    "MEXC",
    "GATE.IO",
    "KUCOIN",
    "BYBIT",
    "HTX",
    "BINGX",
}
MIN_EXCHANGES_REQUIRED = int(os.getenv("MIN_EXCHANGES_REQUIRED", "2"))

COINGECKO_API_BASE = "https://api.coingecko.com/api/v3"

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "MaTT-TokenMap-Updater/1.0 (+https://example.com)"
    }
)


@dataclass
class ListedonItem:
    symbol: str
    pair: str
    exchange_slug: str
    exchange_name: str
    listed_date: Optional[date]
    market_url: str


def log(msg: str) -> None:
    print(msg, flush=True)


# ----------------------------
# Парсинг listedon
# ----------------------------

MONTH_PATTERN = r"(January|February|March|April|May|June|July|August|September|October|November|December)"

# формат типа "November 10, 2025"
DATE_REGEX = re.compile(rf"^{MONTH_PATTERN} \d{{1,2}}, \d{{4}}$")

# Пары вида AAA/BBBB (AAA – тикер, BBBB – quote, обычно USDT)
PAIR_REGEX = re.compile(r"\b([A-Z0-9\.\-]{2,15})/([A-Z0-9]{2,10})\b")


def parse_listedon_date(raw: str) -> Optional[date]:
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.strptime(raw, "%B %d, %Y")
        return dt.date()
    except ValueError:
        return None


def fetch_listedon_for_exchange(slug: str, human_name: str) -> List[ListedonItem]:
    """
    Новый парсер:
      - идём по tr сверху вниз
      - если строка = "November 10, 2025" → current_date
      - если в строке видим "BNBHOLDER/USDT" → берём тикер BNBHOLDER, дату = current_date
    """
    items: List[ListedonItem] = []
    today = date.today()
    log(f"[{human_name}] Today (server local date) = {today}")

    debug_rows_logged = 0
    current_date: Optional[date] = None

    for page in range(1, MAX_PAGES_PER_EXCHANGE + 1):
        url = f"https://listedon.org/en/exchange/{slug}/search?page={page}&sort=date&order=1"
        log(f"[{human_name}] Fetching listedon page: {url}")
        resp = SESSION.get(url, timeout=20)
        if resp.status_code != 200:
            log(f"[{human_name}]  ! HTTP {resp.status_code}, stop paging")
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table")
        if not table:
            log(f"[{human_name}]  ! No table found, stop")
            break

        tbody = table.find("tbody")
        rows = tbody.find_all("tr") if tbody else table.find_all("tr")
        if not rows:
            log(f"[{human_name}]  ! No rows found, stop")
            break

        log(f"[{human_name}]  Rows on page {page}: {len(rows)}")

        for idx, tr in enumerate(rows, start=1):
            tds = tr.find_all("td")
            if not tds:
                continue

            texts = [td.get_text(" ", strip=True) for td in tds]
            row_text = " | ".join(texts)

            # логируем первые 30 строк, чтобы видеть реальную структуру
            if debug_rows_logged < 30:
                log(f"[{human_name}]  row#{idx} text: '{row_text}'")
                debug_rows_logged += 1

            # 1) если строка - дата (например, "November 10, 2025")
            if len(texts) == 1 and DATE_REGEX.match(texts[0]):
                parsed = parse_listedon_date(texts[0])
                if parsed:
                    current_date = parsed
                    log(f"[{human_name}]    -> current_date set to {current_date}")
                continue

            # 2) ищем пару AAA/BBBB в строке (она может быть там же, где тикер/время/тип)
            m = PAIR_REGEX.search(row_text)
            if not m:
                continue

            pair_text = m.group(0)
            base_symbol = m.group(1).upper()

            # ссылка на биржу (если есть)
            link_tag = tr.find("a")
            market_url = link_tag["href"] if link_tag and link_tag.has_attr("href") else ""

            # возраст (если дата уже увидена раньше)
            listed_date = current_date
            age_days: Optional[int] = None
            if listed_date:
                age_days = (today - listed_date).days

            log(
                f"[{human_name}]    FOUND pair '{pair_text}' "
                f"symbol='{base_symbol}' date={listed_date} age_days={age_days}"
            )

            items.append(
                ListedonItem(
                    symbol=base_symbol,
                    pair=pair_text,
                    exchange_slug=slug,
                    exchange_name=human_name,
                    listed_date=listed_date,
                    market_url=market_url,
                )
            )

    log(f"[{human_name}]  Total rows collected (no age filter): {len(items)}")
    return items


def fetch_listedon_items() -> List[ListedonItem]:
    all_items: List[ListedonItem] = []
    for slug, human in LISTEDON_SOURCES:
        all_items.extend(fetch_listedon_for_exchange(slug, human))
    return all_items


# ----------------------------
# Coingecko helpers
# ----------------------------

def cg_request(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{COINGECKO_API_BASE}{path}"
    resp = SESSION.get(url, params=params, timeout=20)
    if resp.status_code != 200:
        raise RuntimeError(f"Coingecko API error {resp.status_code}: {resp.text[:200]}")
    return resp.json()


def search_coins_by_symbol(symbol: str) -> List[Dict[str, Any]]:
    data = cg_request("/search", {"query": symbol})
    results = data.get("coins", [])
    out: List[Dict[str, Any]] = []
    for c in results:
        if str(c.get("symbol", "")).upper() == symbol.upper():
            out.append(c)
    return out


PLATFORM_TO_CHAIN = {
    "ethereum": "ethereum",
    "binance-smart-chain": "bnb",
    "bsc": "bnb",
    "bnb-smart-chain": "bnb",
    "solana": "solana",
}


def normalize_exchange_name(name: str) -> str:
    return name.strip().upper().replace(".COM", "").replace(" ", "")


def load_token_map(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        log(f"No existing {path}, starting from empty list.")
        return []
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise RuntimeError("token_map.json must be a list")
    return data


def save_token_map(path: str, tokens: List[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(tokens, f, ensure_ascii=False, indent=2)
    log(f"Saved {len(tokens)} tokens to {path}")


# ----------------------------
# Main logic: Coingecko + фильтры
# ----------------------------

def build_existing_index(existing: List[Dict[str, Any]]) -> Set[Tuple[str, str]]:
    idx: Set[Tuple[str, str]] = set()
    for t in existing:
        chain = str(t.get("chain", "")).lower()
        addr = str(t.get("address", "")).lower()
        if chain and addr:
            idx.add((chain, addr))
    return idx


def group_listedon_items(items: List[ListedonItem]) -> Dict[str, Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for it in items:
        sym = it.symbol.upper()
        g = grouped.setdefault(
            sym,
            {
                "symbol": sym,
                "exchanges": set(),
                "first_listed": it.listed_date,
            },
        )
        g["exchanges"].add(it.exchange_name)
        if it.listed_date and (g["first_listed"] is None or it.listed_date < g["first_listed"]):
            g["first_listed"] = it.listed_date
    return grouped


def coingecko_coin_details(coin_id: str) -> Dict[str, Any]:
    params = {
        "localization": "false",
        "tickers": "true",
        "market_data": "true",
        "community_data": "false",
        "developer_data": "false",
        "sparkline": "false",
    }
    return cg_request(f"/coins/{coin_id}", params=params)


def token_entries_from_coin(
    coin: Dict[str, Any],
    listed_meta: Dict[str, Any],
    existing_idx: Set[Tuple[str, str]],
) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []

    # 1) Маккап
    market_data = coin.get("market_data") or {}
    mcap = (market_data.get("market_cap") or {}).get("usd")
    if not isinstance(mcap, (int, float)):
        return []
    mcap = float(mcap)
    if not (MIN_MCAP_USD <= mcap <= MAX_MCAP_USD):
        return []

    # 2) На каких биржах торгуется на Coingecko
    tickers = coin.get("tickers") or []
    exchanges_seen: Set[str] = set()
    for t in tickers:
        market = t.get("market") or {}
        name = str(market.get("name") or "")
        norm = normalize_exchange_name(name)
        if not norm:
            continue

        if "BINANCE" in norm:
            exchanges_seen.add("BINANCE")
        elif "MEXC" in norm:
            exchanges_seen.add("MEXC")
        elif "GATE" in norm:
            exchanges_seen.add("GATE.IO")
        elif "KUCOIN" in norm:
            exchanges_seen.add("KUCOIN")
        elif "BYBIT" in norm:
            exchanges_seen.add("BYBIT")
        elif "HTX" in norm or "HUOBI" in norm:
            exchanges_seen.add("HTX")
        elif "BINGX" in norm:
            exchanges_seen.add("BINGX")

    if len(exchanges_seen & TARGET_EXCHANGES) < MIN_EXCHANGES_REQUIRED:
        return []

    # 3) Берём только сети ETH / BNB / SOL
    platforms = coin.get("platforms") or {}
    symbol = coin.get("symbol", "").upper()
    name = coin.get("name", "")

    listed_first: Optional[date] = listed_meta.get("first_listed")
    listed_exchanges: List[str] = sorted(list(listed_meta["exchanges"]))

    for platform_key, addr in platforms.items():
        addr = (addr or "").strip()
        if not addr:
            continue
        platform_key_norm = platform_key.strip().lower()
        chain = PLATFORM_TO_CHAIN.get(platform_key_norm)
        if not chain:
            continue

        key = (chain, addr.lower())
        if key in existing_idx:
            continue

        entry: Dict[str, Any] = {
            "symbol": symbol,
            "name": name,
            "chain": chain,
            "address": addr,
            "coingecko_id": coin.get("id"),
            "active": True,
            "listedon_exchanges": listed_exchanges,
        }
        if listed_first:
            entry["listedon_first_seen_at"] = listed_first.isoformat()

        entries.append(entry)

    return entries


def main() -> None:
    log("Fetching listedon data...")
    listedon_items = fetch_listedon_items()
    log(f"Total listedon items collected (no age filter): {len(listedon_items)}")

    if not listedon_items:
        log("No listedon items parsed at all – check HTML structure / selectors.")
        return

    grouped = group_listedon_items(listedon_items)
    log(f"Grouped into {len(grouped)} unique symbols.")

    candidates_symbols = list(grouped.keys())
    log(f"Symbols to try on Coingecko (before CG filters): {len(candidates_symbols)}")

    existing_tokens = load_token_map(TOKEN_MAP_PATH)
    existing_idx = build_existing_index(existing_tokens)
    log(f"Existing tokens: {len(existing_tokens)}")

    new_entries: List[Dict[str, Any]] = []
    seen_new_keys: Set[Tuple[str, str]] = set()

    for sym in candidates_symbols:
        meta = grouped[sym]
        log(f"Searching Coingecko for symbol: {sym}")
        try:
            search_results = search_coins_by_symbol(sym)
        except Exception as exc:
            log(f"  ! Coingecko search failed for {sym}: {exc}")
            continue

        if not search_results:
            log("  No exact symbol matches on Coingecko.")
            continue

        for coin_stub in search_results:
            coin_id = coin_stub.get("id")
            if not coin_id:
                continue
            try:
                coin = coingecko_coin_details(coin_id)
            except Exception as exc:
                log(f"  ! Failed to fetch coin details for {coin_id}: {exc}")
                continue

            entries = token_entries_from_coin(coin, meta, existing_idx)
            for e in entries:
                key = (e["chain"], e["address"].lower())
                if key in seen_new_keys or key in existing_idx:
                    continue
                seen_new_keys.add(key)
                new_entries.append(e)

    log(f"New token entries to add: {len(new_entries)}")
    if not new_entries:
        log("No new tokens to add.")
        return

    updated = existing_tokens + new_entries
    save_token_map(TOKEN_MAP_PATH, updated)


if __name__ == "__main__":
    main()
