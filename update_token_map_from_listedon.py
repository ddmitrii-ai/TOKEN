import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup

# ----------------------------
# Конфиг
# ----------------------------

BASE_URL = "https://listedon.org"

# Биржи из ListedOn (slug после /en/exchange/...), которые считаем "валидными"
TARGET_EXCHANGES: Set[str] = {
    "mxc",
    "bybit_spot",
    "gate",
    "binance",
    "kucoin",
    "huobi",
    "bingx",
}

# Биржи, с которых собираем кандидатов (страницы /en/exchange/{slug})
SOURCE_EXCHANGES: List[str] = [
    "mxc",
    "bybit_spot",
    "gate",
    "binance",
    "kucoin",
    "huobi",
    "bingx",
]

# Окно по возрасту листинга на ЦЕЛЕВЫХ биржах (по данным тикер-страницы)
MIN_AGE_DAYS = 7
MAX_AGE_DAYS = 90

# CoinGecko фильтры
MIN_MCAP_USD = 3_000_000
MAX_MCAP_USD = 1_000_000_000
MIN_VOLUME_USD = 0  # при желании можно поднять, например 200_000

# Какие сети нас интересуют
ALLOWED_CHAINS = {"ethereum", "bnb", "solana"}

# Маппинг названий платформ из CoinGecko в наши chain-строки
PLATFORM_TO_CHAIN = {
    "ethereum": "ethereum",
    "eth": "ethereum",

    "binance-smart-chain": "bnb",
    "bnb-smart-chain": "bnb",
    "bsc": "bnb",

    "solana": "solana",
}

TOKEN_MAP_PATH = Path("token_map.json")

COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")  # опционально


# ----------------------------
# Модели
# ----------------------------

@dataclass
class ExchangeListing:
    exchange_slug: str
    listing_date: date


@dataclass
class TickerCandidate:
    symbol: str
    ticker_url: str
    discovered_on: Set[str] = field(default_factory=set)  # с каких бирж-источников увидели
    listings: List[ExchangeListing] = field(default_factory=list)

    def target_exchanges(self) -> Set[str]:
        return {l.exchange_slug for l in self.listings if l.exchange_slug in TARGET_EXCHANGES}

    def first_listing_date(self) -> Optional[date]:
        if not self.listings:
            return None
        return min(l.listing_date for l in self.listings)


# ----------------------------
# Утилиты
# ----------------------------

def http_get(url: str, **kwargs) -> Optional[requests.Response]:
    headers = kwargs.pop("headers", {})
    headers.setdefault("User-Agent", "Mozilla/5.0 (compatible; MaTT-bot/1.0)")
    try:
        resp = requests.get(url, headers=headers, timeout=15, **kwargs)
        if resp.status_code != 200:
            print(f"[HTTP] {url} -> {resp.status_code}")
            return None
        return resp
    except Exception as e:
        print(f"[HTTP] error GET {url}: {e}")
        return None


def parse_listedon_date_td(td) -> Optional[date]:
    """
    td на всех таблицах выглядит примерно так:

      <td class="date">
        " November 10"
        <span class="year">, 2025</span>
        <br/>
        <span class="time">11:59</span>
      </td>

    или на страницах тикера:

      <td class="date">November 10, 2025<br/><span class="time">06:30</span></td>

    Наша задача — вернуть date(2025, 11, 10).
    """
    if td is None:
        return None

    # Удаляем время
    for span in td.find_all("span", class_="time"):
        span.decompose()

    text = td.get_text(" ", strip=True)
    text = " ".join(text.split())
    text = text.replace(" ,", ",")

    for fmt in ("%B %d, %Y", "%B %d %Y"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.date()
        except ValueError:
            continue

    print(f"[WARN] could not parse date from '{text}'")
    return None


# ----------------------------
# Парсинг exchange-страниц
# ----------------------------

def fetch_exchange_candidates(exchange_slug: str,
                              max_pages: int = 10) -> List[Tuple[str, str]]:
    """
    Возвращает список (symbol, ticker_url) для всех строк на /exchange/{slug}
    БЕЗ фильтра по дате (дату берём позже с /ticker/XXX).
    """
    results: List[Tuple[str, str]] = []

    for page in range(1, max_pages + 1):
        url = f"{BASE_URL}/en/exchange/{exchange_slug}/search?page={page}&sort=date&order=1"
        print(f"[{exchange_slug.upper()}] Fetching listedon page: {url}")
        resp = http_get(url)
        if not resp:
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", class_="table-smart-items")
        if not table:
            print(f"[{exchange_slug.upper()}]  No table-smart-items on page {page}")
            break

        rows = table.select("tbody tr.item")
        print(f"[{exchange_slug.upper()}]  Rows on page {page}: {len(rows)}")
        if not rows:
            break

        for tr in rows:
            # Ищем ссылку на тикер
            ticker_link = tr.find("a", href=re.compile(r"/ticker/"))
            if not ticker_link:
                continue

            symbol = (ticker_link.text or "").strip().upper()
            href = ticker_link.get("href", "")
            ticker_url = requests.compat.urljoin(BASE_URL, href)

            results.append((symbol, ticker_url))

    print(f"[{exchange_slug.upper()}]  Total ticker rows collected: {len(results)}")
    return results


# ----------------------------
# Парсинг ticker-страницы (/en/ticker/XXX)
# ----------------------------

def fetch_ticker_details(symbol: str, ticker_url: str) -> TickerCandidate:
    """
    Для тикера собираем все листинги на биржах (exchange_slug + date).
    """
    resp = http_get(ticker_url)
    cand = TickerCandidate(symbol=symbol, ticker_url=ticker_url)

    if not resp:
        print(f"[TICKER] Failed to fetch {ticker_url}")
        return cand

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", class_="table-smart-items")
    if not table:
        print(f"[TICKER] No table-smart-items for {ticker_url}")
        return cand

    rows = table.select("tbody tr.item")
    for tr in rows:
        date_td = tr.find("td", class_="date")
        exch_td = tr.find("td", class_=re.compile(r"\bexchanges?\b"))

        listing_date = parse_listedon_date_td(date_td)
        if not listing_date or not exch_td:
            continue

        exch_link = exch_td.find("a", href=re.compile(r"/en/exchange/"))
        if not exch_link:
            continue

        href = exch_link.get("href", "")
        m = re.search(r"/en/exchange/([^/?#]+)", href)
        if not m:
            continue

        exch_slug = m.group(1).lower()
        cand.listings.append(ExchangeListing(exchange_slug=exch_slug,
                                             listing_date=listing_date))

    return cand


def listing_age_ok(cand: TickerCandidate,
                   today: Optional[date] = None) -> bool:
    """
    Есть ли хотя бы один листинг на бирже из TARGET_EXCHANGES
    с возрастом в диапазоне [MIN_AGE_DAYS, MAX_AGE_DAYS]?
    """
    if today is None:
        today = date.today()

    ok = False
    for l in cand.listings:
        if l.exchange_slug not in TARGET_EXCHANGES:
            continue
        age = (today - l.listing_date).days
        if MIN_AGE_DAYS <= age <= MAX_AGE_DAYS:
            ok = True
            break
    return ok


# ----------------------------
# CoinGecko
# ----------------------------

def coingecko_headers() -> Dict[str, str]:
    headers = {"Accept": "application/json"}
    if COINGECKO_API_KEY:
        headers["x-cg-pro-api-key"] = COINGECKO_API_KEY
    return headers


def coingecko_search_symbol(symbol: str) -> List[Dict]:
    url = "https://api.coingecko.com/api/v3/search"
    resp = http_get(url, params={"query": symbol}, headers=coingecko_headers())
    if not resp:
        return []
    try:
        data = resp.json()
    except Exception as e:
        print(f"[CG] failed parse search response for {symbol}: {e}")
        return []
    return data.get("coins", [])


def coingecko_get_coin(coin_id: str) -> Optional[Dict]:
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
    params = {
        "localization": "false",
        "tickers": "false",
        "market_data": "true",
        "community_data": "false",
        "developer_data": "false",
        "sparkline": "false",
    }
    resp = http_get(url, params=params, headers=coingecko_headers())
    if not resp:
        return None
    try:
        return resp.json()
    except Exception as e:
        print(f"[CG] failed parse coin {coin_id}: {e}")
        return None


def choose_chain_and_address(platforms: Dict[str, str]) -> Optional[Tuple[str, str]]:
    """
    platforms: {"ethereum": "0x...", "solana": "...", ...}
    Возвращаем (chain, address) или None.
    """
    if not platforms:
        return None

    priority = ["bnb", "ethereum", "solana"]

    candidates: List[Tuple[str, str]] = []
    for plat_name, addr in platforms.items():
        if not addr:
            continue
        chain = PLATFORM_TO_CHAIN.get(plat_name.lower())
        if chain in ALLOWED_CHAINS:
            candidates.append((chain, addr))

    if not candidates:
        return None

    for ch in priority:
        for chain, addr in candidates:
            if chain == ch:
                return chain, addr

    return candidates[0]


def find_token_on_coingecko(symbol: str) -> Optional[Dict]:
    candidates = coingecko_search_symbol(symbol)
    if not candidates:
        print(f"[CG] no search results for symbol {symbol}")
        return None

    def score(item: Dict) -> Tuple[int, int]:
        s = (item.get("symbol") or "").lower()
        exact = 1 if s == symbol.lower() else 0
        rank = item.get("market_cap_rank")
        rank_score = 10_000 if rank is None else int(rank)
        return (-exact, rank_score)

    candidates_sorted = sorted(candidates, key=score)

    for item in candidates_sorted:
        coin_id = item.get("id")
        if not coin_id:
            continue

        coin = coingecko_get_coin(coin_id)
        if not coin:
            continue

        platforms = coin.get("platforms") or {}
        chain_addr = choose_chain_and_address(platforms)
        if not chain_addr:
            continue

        chain, address = chain_addr
        market_data = coin.get("market_data") or {}
        mcap = (market_data.get("market_cap") or {}).get("usd")
        vol = (market_data.get("total_volume") or {}).get("usd")

        if mcap is None:
            continue

        if not (MIN_MCAP_USD <= mcap <= MAX_MCAP_USD):
            continue

        if vol is not None and vol < MIN_VOLUME_USD:
            continue

        name = coin.get("name") or item.get("name") or symbol

        return {
            "symbol": (coin.get("symbol") or symbol).upper(),
            "name": name,
            "coingecko_id": coin_id,
            "chain": chain,
            "address": address,
            "market_cap": mcap,
            "volume_24h": vol,
        }

    print(f"[CG] no suitable coin found for symbol {symbol}")
    return None


# ----------------------------
# token_map.json
# ----------------------------

def load_token_map(path: Path) -> List[Dict]:
    if not path.exists():
        print(f"[TOKEN_MAP] {path} not found, starting from empty list")
        return []
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise RuntimeError("token_map.json must contain a list")
    print(f"[TOKEN_MAP] Loaded {len(data)} existing tokens")
    return data


def save_token_map(path: Path, tokens: List[Dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(tokens, f, ensure_ascii=False, indent=2)
    print(f"[TOKEN_MAP] Saved {len(tokens)} tokens to {path}")


# ----------------------------
# Main
# ----------------------------

def main():
    print("Fetching listedon data...")

    # 1) Собираем кандидатов с exchange-страниц (без фильтра по дате)
    all_rows: List[Tuple[str, str, str]] = []  # (symbol, url, source_exchange)
    for exch in SOURCE_EXCHANGES:
        rows = fetch_exchange_candidates(exch)
        for sym, url in rows:
            all_rows.append((sym, url, exch))

    print(f"Total rows from sources (raw, no age filter): {len(all_rows)}")
    if not all_rows:
        print("No rows collected from exchanges, nothing to do.")
        return

    # 2) Группируем по ticker_url
    candidates_by_url: Dict[str, TickerCandidate] = {}

    for sym, url, exch in all_rows:
        cand = candidates_by_url.get(url)
        if not cand:
            cand = TickerCandidate(symbol=sym, ticker_url=url)
            candidates_by_url[url] = cand
        cand.discovered_on.add(exch)

    print(f"Unique ticker URLs to inspect: {len(candidates_by_url)}")

    # 3) Парсим /en/ticker/XXX и фильтруем по возрасту
    today = date.today()
    filtered_by_age: List[TickerCandidate] = []

    for url, cand in candidates_by_url.items():
        full = fetch_ticker_details(cand.symbol, url)
        cand.listings = full.listings

        if not cand.listings:
            continue

        if not listing_age_ok(cand, today=today):
            continue

        filtered_by_age.append(cand)

    print(f"Candidates after age filter (at least one listing in [{MIN_AGE_DAYS},{MAX_AGE_DAYS}] days): {len(filtered_by_age)}")

    # 4) Фильтр: ≥2 целевые биржи
    filtered_candidates: List[TickerCandidate] = []
    for cand in filtered_by_age:
        good_exch = cand.target_exchanges()
        if len(good_exch) >= 2:
            filtered_candidates.append(cand)

    print(f"Candidates with >=2 target exchanges: {len(filtered_candidates)}")
    if not filtered_candidates:
        print("No candidates after exchange-count filter, nothing to do.")
        return

    # 5) token_map.json
    tokens = load_token_map(TOKEN_MAP_PATH)

    existing_ids: Set[str] = set()
    existing_chain_addr: Set[Tuple[str, str]] = set()

    for t in tokens:
        cid = (t.get("coingecko_id") or "").lower()
        if cid:
            existing_ids.add(cid)
        chain = (t.get("chain") or "").lower()
        addr = (t.get("address") or "").lower()
        if chain and addr:
            existing_chain_addr.add((chain, addr))

    # 6) CoinGecko + добавление
    added: List[Dict] = []

    for cand in filtered_candidates:
        first_date = cand.first_listing_date()
        first_date_str = first_date.isoformat() if first_date else None
        exch_list = sorted(list(cand.target_exchanges()))

        print(f"\n[PROCESS] {cand.symbol} ({cand.ticker_url})")
        print(f"  target exchanges: {', '.join(exch_list)}")
        print(f"  first listing date (ListedOn): {first_date_str}")

        info = find_token_on_coingecko(cand.symbol)
        if not info:
            continue

        cid = info["coingecko_id"].lower()
        chain = info["chain"].lower()
        addr = info["address"].lower()

        if cid in existing_ids:
            print(f"  -> already in token_map by coingecko_id ({cid}), skip")
            continue

        if (chain, addr) in existing_chain_addr:
            print(f"  -> already in token_map by chain+address ({chain}, {addr}), skip")
            continue

        token_entry = {
            "symbol": info["symbol"],
            "name": info["name"],
            "chain": info["chain"],
            "address": info["address"],
            "coingecko_id": info["coingecko_id"],
            "active": True,
            "listedon_first_listing_date": first_date_str,
            "listedon_exchanges": exch_list,
            "listedon_url": cand.ticker_url,
        }

        tokens.append(token_entry)
        added.append(token_entry)

        existing_ids.add(cid)
        existing_chain_addr.add((chain, addr))

        print(f"  -> ADDED to token_map: {info['symbol']} | {info['name']} | {info['chain']} {info['address']}")

    # 7) Сохранение
    if added:
        save_token_map(TOKEN_MAP_PATH, tokens)
        print(f"\nDone. Added {len(added)} new tokens.")
    else:
        print("No new tokens to add.")


if __name__ == "__main__":
    main()
