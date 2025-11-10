import os
import json
import time
import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


# ------------------------
# Конфиг
# ------------------------

# Путь к token_map.json (лежит рядом со скриптом)
TOKEN_MAP_PATH = Path(__file__).with_name("token_map.json")

# Биржи, которые парсим на listedon
EXCHANGES = [
    ("mxc", "MEXC"),
    ("bybit_spot", "Bybit Spot"),
    ("gate", "Gate.io"),
    ("binance", "Binance"),
    ("kucoin", "KuCoin"),
    ("huobi", "Huobi / HTX"),
    ("bingx", "BingX"),
]

# Ограничения по возрасту листинга (в днях)
# Пример: MIN_AGE_DAYS=7, MAX_AGE_DAYS=90
MIN_AGE_DAYS = int(os.getenv("MIN_AGE_DAYS", "7"))
MAX_AGE_DAYS = int(os.getenv("MAX_AGE_DAYS", "90"))

# Минимальное количество бирж из списка, на которых должен быть тикер
MIN_EXCHANGES = int(os.getenv("MIN_EXCHANGES", "2"))

# Фильтр по капитализации (USD)
MCAP_MIN = float(os.getenv("MCAP_MIN", str(3_000_000)))        # 3M
MCAP_MAX = float(os.getenv("MCAP_MAX", str(1_000_000_000)))    # 1B

# Ограничение страниц на listedon (на каждую биржу)
MAX_PAGES_PER_EXCHANGE = int(os.getenv("MAX_PAGES_PER_EXCHANGE", "10"))

# CoinGecko API
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "").strip()
COINGECKO_BASE = "https://api.coingecko.com/api/v3"


# ------------------------
# HTTP helpers
# ------------------------


def http_get(url: str, params: Optional[Dict[str, Any]] = None) -> Optional[requests.Response]:
    headers = {
        "User-Agent": "Mozilla/5.0 (MaTT-listedon-bot)",
        "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
    }
    # если используешь Pro-ключ CoinGecko:
    if COINGECKO_API_KEY and "coingecko.com" in url:
        headers["x-cg-pro-api-key"] = COINGECKO_API_KEY

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        if resp.status_code != 200:
            print(f"HTTP {resp.status_code} for {url}")
            return None
        return resp
    except Exception as exc:
        print(f"HTTP error for {url}: {exc}")
        return None


# ------------------------
# Парсинг listedon для биржи
# ------------------------


def fetch_listedon_for_exchange(
    exchange_slug: str,
    exchange_label: str,
    max_pages: int = MAX_PAGES_PER_EXCHANGE,
) -> List[Dict[str, Any]]:
    """
    Парсим listedon для одной биржи:

      https://listedon.org/en/exchange/{exchange_slug}/search?page=X&sort=date&order=1

    Структура (важный момент):
      - строка с датой: <tr><th colspan="4">November 10, 2025</th></tr>
      - далее несколько строк:
          <tr>
            <td>11:59</td>
            <td>BNBHOLDER</td>
            <td>Listing</td>
            <td><a href="/en/ticker/BNBHOLDER">BNBHOLDER/USDT</a></td>
          </tr>

    Мы учитываем и <th>, и <td>.
    """
    base_url = f"https://listedon.org/en/exchange/{exchange_slug}/search"
    today = datetime.date.today()
    print(f"[{exchange_label}] Today (server local date) = {today.isoformat()}")

    items: List[Dict[str, Any]] = []

    for page in range(1, max_pages + 1):
        url = f"{base_url}?page={page}&sort=date&order=1"
        print(f"[{exchange_label}] Fetching listedon page: {url}")
        resp = http_get(url)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        table = soup.find("table")
        if not table:
            print(f"[{exchange_label}]  No table on page {page}")
            continue

        tbody = table.find("tbody") or table
        rows = tbody.find_all("tr")
        print(f"[{exchange_label}]  Rows on page {page}: {len(rows)}")

        current_date: Optional[datetime.date] = None

        for tr in rows:
            # ВАЖНО: смотрим и на <td>, и на <th>
            cells = tr.find_all(["td", "th"])
            if not cells:
                continue

            # --- строка с датой (одна ячейка вида "November 10, 2025") ---
            if len(cells) == 1:
                txt = cells[0].get_text(" ", strip=True)
                # пробуем распарсить дату
                try:
                    current_date = datetime.datetime.strptime(txt, "%B %d, %Y").date()
                except ValueError:
                    # не дата — игнорируем
                    pass
                continue

            # --- строка с листингом: Time | Ticker | Type | Pairs ---
            if len(cells) >= 4:
                if current_date is None:
                    # если ещё не было строки с датой — пропускаем
                    continue

                time_str = cells[0].get_text(" ", strip=True)
                ticker = cells[1].get_text(" ", strip=True)
                list_type = cells[2].get_text(" ", strip=True)
                pairs_cell = cells[3]
                pairs_text = pairs_cell.get_text(" ", strip=True)

                if list_type.lower() != "listing":
                    continue

                # ссылка на страницу тикера: /en/ticker/XXX
                pair_link = pairs_cell.find("a")
                ticker_url = (
                    urljoin("https://listedon.org", pair_link["href"])
                    if pair_link and pair_link.has_attr("href")
                    else None
                )

                items.append(
                    {
                        "symbol": ticker.strip(),
                        "pair": pairs_text,
                        "exchange_slug": exchange_slug,
                        "exchange_label": exchange_label,
                        "listedon_date": current_date.isoformat(),
                        "listedon_time": time_str,
                        "listedon_ticker_url": ticker_url,
                    }
                )

    print(f"[{exchange_label}]  Total rows collected (no age filter): {len(items)}")
    return items


# ------------------------
# Aggregation по тикеру
# ------------------------


def aggregate_listedon_items(all_items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Группируем по тикеру (symbol):
      {
        "SYMBOL": {
          "symbol": "SYMBOL",
          "entries": [...],
          "exchanges": {"MEXC", "Gate.io", ...},
          "first_date": date,
        },
        ...
      }
    """
    by_symbol: Dict[str, Dict[str, Any]] = {}
    for it in all_items:
        sym = it["symbol"].strip().upper()
        d = datetime.date.fromisoformat(it["listedon_date"])
        ex_label = it["exchange_label"]

        if sym not in by_symbol:
            by_symbol[sym] = {
                "symbol": sym,
                "entries": [],
                "exchanges": set(),
                "first_date": d,
            }
        info = by_symbol[sym]
        info["entries"].append(it)
        info["exchanges"].add(ex_label)
        if d < info["first_date"]:
            info["first_date"] = d

    return by_symbol


# ------------------------
# Работа с token_map.json
# ------------------------


def load_token_map(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        print(f"{path} does not exist, starting with empty list.")
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            print("token_map.json is not a list, ignoring.")
            return []
        return data
    except Exception as exc:
        print(f"Failed to load {path}: {exc}")
        return []


def save_token_map(path: Path, tokens: List[Dict[str, Any]]) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(tokens, f, ensure_ascii=False, indent=2)
        print(f"Saved token_map.json with {len(tokens)} entries.")
    except Exception as exc:
        print(f"Failed to save {path}: {exc}")


# ------------------------
# CoinGecko helpers
# ------------------------


def coingecko_search_symbol(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Поиск монеты по тикеру через /search.
    Возвращаем лучшую по совпадению symbol.
    """
    url = f"{COINGECKO_BASE}/search"
    resp = http_get(url, params={"query": symbol})
    if not resp:
        return None

    try:
        data = resp.json()
    except Exception:
        return None

    coins = data.get("coins") or []
    if not coins:
        return None

    symbol_lower = symbol.lower()

    # 1) точное совпадение по symbol
    exact = [c for c in coins if (c.get("symbol") or "").lower() == symbol_lower]
    if exact:
        return exact[0]

    # 2) иначе первая
    return coins[0]


def coingecko_fetch_coin_details(coin_id: str) -> Optional[Dict[str, Any]]:
    """
    Берём подробную инфу по монете:
      /coins/{id}?market_data=true&...
    """
    url = f"{COINGECKO_BASE}/coins/{coin_id}"
    params = {
        "localization": "false",
        "tickers": "false",
        "market_data": "true",
        "community_data": "false",
        "developer_data": "false",
        "sparkline": "false",
    }
    resp = http_get(url, params=params)
    if not resp:
        return None
    try:
        return resp.json()
    except Exception:
        return None


def choose_platform_and_chain(coin_details: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    """
    Выбираем сеть и адрес контракта:
      цепочки в token_map.json: "ethereum" | "bnb" | "solana"
    """
    platforms = coin_details.get("platforms") or {}
    platforms_norm = {k.lower(): v for k, v in platforms.items() if v}

    if "ethereum" in platforms_norm:
        return "ethereum", platforms_norm["ethereum"]

    for k in ("binance-smart-chain", "bsc", "bnb-smart-chain"):
        if k in platforms_norm:
            return "bnb", platforms_norm[k]

    if "solana" in platforms_norm:
        return "solana", platforms_norm["solana"]

    return None


def get_market_cap_usd(coin_details: Dict[str, Any]) -> Optional[float]:
    md = coin_details.get("market_data") or {}
    mc = md.get("market_cap") or {}
    v = mc.get("usd")
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


# ------------------------
# Main logic
# ------------------------


def main() -> None:
    print("Fetching listedon data...")

    all_items: List[Dict[str, Any]] = []

    # 1. Собираем все листинги со всех бирж
    for slug, label in EXCHANGES:
        items = fetch_listedon_for_exchange(slug, label, max_pages=MAX_PAGES_PER_EXCHANGE)
        all_items.extend(items)

    print(f"Total listedon items collected (no age filter): {len(all_items)}")

    if not all_items:
        print("No listedon items parsed at all – check HTML structure / selectors.")
        return

    # 2. Агрегируем по тикеру
    by_symbol = aggregate_listedon_items(all_items)

    today = datetime.date.today()
    candidates: List[Dict[str, Any]] = []

    print()
    print("Filtering by exchanges count and listing age...")
    for sym, info in by_symbol.items():
        exchanges = info["exchanges"]
        first_date: datetime.date = info["first_date"]
        age_days = (today - first_date).days

        if len(exchanges) < MIN_EXCHANGES:
            continue

        if age_days < MIN_AGE_DAYS or age_days > MAX_AGE_DAYS:
            continue

        candidates.append(info)

    print(
        f"Candidates after exchange-count >= {MIN_EXCHANGES} "
        f"and age in [{MIN_AGE_DAYS}, {MAX_AGE_DAYS}] days: {len(candidates)}"
    )

    if not candidates:
        print("No candidates after filters.")
        return

    # 3. Загружаем текущий token_map.json
    existing_tokens = load_token_map(TOKEN_MAP_PATH)
    print(f"Existing tokens: {len(existing_tokens)}")

    existing_by_cg: Dict[str, Dict[str, Any]] = {}
    existing_by_chain_addr: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for t in existing_tokens:
        cg = t.get("coingecko_id")
        if cg:
            existing_by_cg[cg] = t
        chain = (t.get("chain") or "").lower()
        addr = (t.get("address") or "").lower()
        if chain and addr:
            existing_by_chain_addr[(chain, addr)] = t

    new_tokens: List[Dict[str, Any]] = []

    print()
    print("Querying CoinGecko for candidates...")

    # 4. Для каждого кандидата — CoinGecko и фильтры
    for info in candidates:
        sym = info["symbol"]
        exchanges = sorted(info["exchanges"])
        first_date = info["first_date"]

        print(f"  [symbol={sym}] exchanges={exchanges}, first_date={first_date.isoformat()}")

        search_res = coingecko_search_symbol(sym)
        if not search_res:
            print(f"    -> No CoinGecko search result for symbol {sym}")
            continue

        coin_id = search_res.get("id")
        if not coin_id:
            print(f"    -> CoinGecko search result has no id for {sym}")
            continue

        if coin_id in existing_by_cg:
            print(f"    -> Already in token_map by coingecko_id={coin_id}, skipping")
            continue

        details = coingecko_fetch_coin_details(coin_id)
        if not details:
            print(f"    -> Failed to fetch details for {coin_id}")
            continue

        plat = choose_platform_and_chain(details)
        if not plat:
            print(f"    -> No supported platform (ethereum/bnb/solana) for {coin_id}")
            continue

        chain, address = plat
        addr_key = (chain.lower(), address.lower())
        if addr_key in existing_by_chain_addr:
            print("    -> Token with same chain+address already exists in token_map, skipping")
            continue

        mcap = get_market_cap_usd(details)
        if mcap is None:
            print(f"    -> No market cap data for {coin_id}, skipping")
            continue

        if not (MCAP_MIN <= mcap <= MCAP_MAX):
            print(
                f"    -> Market cap {mcap:,.0f} out of range "
                f"[{MCAP_MIN:,.0f}, {MCAP_MAX:,.0f}], skipping"
            )
            continue

        token_obj: Dict[str, Any] = {
            "symbol": sym,
            "name": details.get("name") or sym,
            "chain": chain,
            "address": address,
            "coingecko_id": coin_id,
            "active": True,
            "listedon_first_seen": first_date.isoformat(),
            "listedon_exchanges": exchanges,
        }

        new_tokens.append(token_obj)
        existing_by_cg[coin_id] = token_obj
        existing_by_chain_addr[addr_key] = token_obj

        print(
            f"    -> ADDED: {sym} / {chain} / {address} / mcap={mcap:,.0f} USD "
            f"exchanges={exchanges}"
        )

        time.sleep(1.0)

    print()
    print(f"New tokens to add: {len(new_tokens)}")

    if not new_tokens:
        print("Nothing to add, exiting.")
        return

    updated_tokens = existing_tokens + new_tokens
    updated_tokens_sorted = sorted(
        updated_tokens, key=lambda t: (t.get("symbol") or "", t.get("chain") or "")
    )

    save_token_map(TOKEN_MAP_PATH, updated_tokens_sorted)


if __name__ == "__main__":
    main()
