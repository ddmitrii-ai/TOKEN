import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, date, timezone
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

# Окно по возрасту: берем монеты с age_days в [MIN_AGE_DAYS, MAX_AGE_DAYS]
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
    listed_date: date
    market_url: str


def log(msg: str) -> None:
    print(msg, flush=True)


# ----------------------------
# Парсинг listedon
# ----------------------------

def extract_date_part(raw: str) -> Optional[str]:
    """
    Из текста ячейки (например 'October 23, 2025 15:14' или с переносами)
    выдёргиваем только 'October 23, 2025'.
    """
    if not raw:
        return None
    s = " ".join(str(raw).split())
    m = re.search(r"[A-Za-z]+ \d{1,2}, \d{4}", s)
    if m:
        return m.group(0)
    return s


def parse_listedon_date(raw: str) -> Optional[date]:
    if not raw:
        return None
    date_part = extract_date_part(raw)
    if not date_part:
        return None
    try:
        dt = datetime.strptime(date_part, "%B %d, %Y")
        return dt.date()
    except ValueError:
        return None


def fetch_listedon_for_exchange(slug: str, human_name: str) -> List[ListedonItem]:
    """
    Парсим listedon для биржи. Берём страницы, пока:
      - не исчерпали MAX_PAGES_PER_EXCHANGE
      - не встретили записи старше MAX_AGE_DAYS (тогда дальше будет только старее).
    В эту функцию добавлено подробное логирование дат.
    """
    items: List[ListedonItem] = []
    today = datetime.now(timezone.utc).date()

    log(f"[{human_name}] Today (UTC date) = {today}, "
        f"MIN_AGE_DAYS={MIN_AGE_DAYS}, MAX_AGE_DAYS={MAX_AGE_DAYS}")

    debug_rows_logged = 0  # чтобы не заспамить логи

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

        stop_due_to_age = False

        for tr in rows:
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue

            # 0-й столбец — market/pair вида "RZTO/USDT"
            pair_text = tds[0].get_text(" ", strip=True)
            if not pair_text or "/" not in pair_text:
                continue
            base_symbol = pair_text.split("/")[0].strip().upper()

            link_tag = tds[0].find("a")
            market_url = link_tag["href"] if link_tag and link_tag.has_attr("href") else ""

            # последний столбец — дата (с временем), нам нужна только дата
            date_td = tds[-1]
            date_text_raw = date_td.get_text(" ", strip=True)
            listed_date = parse_listedon_date(date_text_raw)
            if not listed_date:
                if debug_rows_logged < 10:
                    log(f"[{human_name}]  !! Could not parse date from '{date_text_raw}'")
                    debug_rows_logged += 1
                continue

            age_days = (today - listed_date).days

            if debug_rows_logged < 10:
                log(
                    f"[{human_name}]  row debug: pair='{pair_text}', "
                    f"raw_date='{date_text_raw}', parsed_date={listed_date}, age_days={age_days}"
                )
                debug_rows_logged += 1

            # слишком новый — младше нижней границы
            if age_days < MIN_AGE_DAYS:
                continue

            # слишком старый — старше верхней границы, дальше будет ещё старее
            if age_days > MAX_AGE_DAYS:
                stop_due_to_age = True
                break

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

        if stop_due_to_age:
            log(f"[{human_name}]  Reached items older than {MAX_AGE_DAYS} days, stop paging.")
            break

    log(f"[{human_name}]  Found {len(items)} items within date window.")
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
# Main logic
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
        if it.listed_date < g["first_listed"]:
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

    market_data = coin.get("market_data") or {}
    mcap = (market_data.get("market_cap") or {}).get("usd")
    if not isinstance(mcap, (int, float)):
        return []
    mcap = float(mcap)
    if not (MIN_MCAP_USD <= mcap <= MAX_MCAP_USD):
        return []

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

    platforms = coin.get("platforms") or {}
    symbol = coin.get("symbol", "").upper()
    name = coin.get("name", "")

    listed_first: date = listed_meta["first_listed"]
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

        entries.append(
            {
                "symbol": symbol,
                "name": name,
                "chain": chain,
                "address": addr,
                "coingecko_id": coin.get("id"),
                "active": True,
                "listedon_first_seen_at": listed_first.isoformat(),
                "listedon_exchanges": listed_exchanges,
            }
        )

    return entries


def main() -> None:
    log("Fetching listedon data...")
    listedon_items = fetch_listedon_items()
    log(f"Total listedon items (raw, within window): {len(listedon_items)}")

    if not listedon_items:
        log("No listedon items in the configured date window.")
        return

    grouped = group_listedon_items(listedon_items)
    log(f"Grouped into {len(grouped)} unique symbols.")

    candidates_symbols = [
        sym for sym, meta in grouped.items()
        if len(meta["exchanges"]) >= 1
    ]
    log(f"Symbols passing listedon-exchange-count filter: {len(candidates_symbols)}")

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
