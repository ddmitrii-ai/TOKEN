import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup

# ---------------------------
# Конфиг
# ---------------------------

TOKEN_MAP_PATH = Path("token_map.json")

# окно по возрасту листинга (в днях)
MIN_AGE_DAYS = 7
MAX_AGE_DAYS = 90

# фильтр по капитализации
MIN_MCAP_USD = 3_000_000
MAX_MCAP_USD = 1_000_000_000

# целевые сети
ALLOWED_CHAINS = {"ethereum", "bnb", "solana"}

# биржи, которые учитываем
TARGET_EXCHANGES = {"binance", "gate", "kucoin", "mexc", "bybit", "htx", "bingx"}

# listedon: какие страницы парсим
LISTEDON_SOURCES = [
    {"slug": "mxc", "exchange_key": "mexc"},
    {"slug": "gate", "exchange_key": "gate"},
]

# Coingecko API
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
COINGECKO_API_KEY = None  # можно прочитать из env если нужно


# ---------------------------
# Вспомогательные структуры
# ---------------------------

EXCHANGE_ALIASES: Dict[str, List[str]] = {
    "binance": ["binance"],
    "gate": ["gate.io"],
    "kucoin": ["kucoin"],
    "mexc": ["mexc"],
    "bybit": ["bybit"],
    "htx": ["htx", "huobi"],
    "bingx": ["bingx"],
}


def normalize_exchange_name(name: str) -> Optional[str]:
    """
    Приводим market.name из Coingecko к нашему ключу.
    """
    n = name.lower()
    for key, aliases in EXCHANGE_ALIASES.items():
        for a in aliases:
            if a in n:
                return key
    return None


PLATFORM_TO_CHAIN: Dict[str, str] = {
    "ethereum": "ethereum",
    "binance-smart-chain": "bnb",
    "bsc": "bnb",
    "bnb-smart-chain": "bnb",
    "solana": "solana",
}


@dataclass
class ListedonItem:
    symbol: str          # тикер (JCT)
    pair: str            # пара (JCT/USDT)
    exchange_key: str    # mexc, gate
    listed_at: datetime  # время листинга (предположим UTC)


# ---------------------------
# HTTP helpers
# ---------------------------

def http_get(url: str, **kwargs) -> requests.Response:
    headers = kwargs.pop("headers", {})
    headers.setdefault("User-Agent", "Mozilla/5.0 (token-list-bot)")
    headers.setdefault("Accept", "text/html,application/json")
    resp = requests.get(url, headers=headers, timeout=30, **kwargs)
    resp.raise_for_status()
    return resp


def cg_get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = COINGECKO_BASE_URL.rstrip("/") + path
    headers = {"Accept": "application/json"}
    if COINGECKO_API_KEY:
        headers["x-cg-demo-api-key"] = COINGECKO_API_KEY
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


# ---------------------------
# Парсинг listedon
# ---------------------------

def parse_listedon_date(text: str) -> Optional[datetime]:
    """
    text типа "November 9, 2025 11:23"
    """
    s = " ".join(text.split())
    for fmt in ("%B %d, %Y %H:%M", "%B %d, %Y"):
        try:
            dt = datetime.strptime(s, fmt)
            # считаем, что это UTC
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def fetch_listedon_exchange(
    slug: str,
    exchange_key: str,
    min_age_days: int,
    max_age_days: int,
    max_pages: int = 50,
) -> List[ListedonItem]:
    """
    Собираем листинги с listedon для одной биржи.
    """
    now = datetime.now(timezone.utc)
    results: List[ListedonItem] = []

    for page in range(1, max_pages + 1):
        url = f"https://listedon.org/en/exchange/{slug}/search?page={page}&sort=date&order=1"
        resp = http_get(url)
        soup = BeautifulSoup(resp.text, "html.parser")

        table = soup.find("table")
        if not table:
            break

        trs = table.find_all("tr")
        if len(trs) <= 1:
            break

        stop = False
        for tr in trs[1:]:
            tds = tr.find_all("td")
            if len(tds) < 4:
                continue

            # 0: date, 1: ticker, 3: pair
            date_text = " ".join(list(tds[0].stripped_strings))
            dt = parse_listedon_date(date_text)
            if not dt:
                continue

            age_days = (now - dt).days

            # будущие/слишком новые листинги (меньше недели) — пропускаем, но продолжаем
            if age_days < min_age_days:
                continue

            # слишком старые — можно остановиться (таблица по дате)
            if age_days > max_age_days:
                stop = True
                break

            ticker_link = tds[1].find("a")
            symbol = (ticker_link.text if ticker_link else tds[1].get_text()).strip().upper()

            pair_link = tds[3].find("a")
            pair = (pair_link.text if pair_link else tds[3].get_text()).strip().upper()

            results.append(
                ListedonItem(
                    symbol=symbol,
                    pair=pair,
                    exchange_key=exchange_key,
                    listed_at=dt,
                )
            )

        if stop:
            break

        # маленькая пауза, чтобы не спамить сайт
        time.sleep(1.0)

    return results


def fetch_all_listedon_items() -> List[ListedonItem]:
    items: List[ListedonItem] = []
    for src in LISTEDON_SOURCES:
        part = fetch_listedon_exchange(
            slug=src["slug"],
            exchange_key=src["exchange_key"],
            min_age_days=MIN_AGE_DAYS,
            max_age_days=MAX_AGE_DAYS,
        )
        items.extend(part)
    return items


# ---------------------------
# Работа с Coingecko
# ---------------------------

def get_exchanges_for_coin(coin: Dict[str, Any], symbol_upper: str) -> Set[str]:
    exchs: Set[str] = set()
    for t in coin.get("tickers", []):
        base = (t.get("base") or "").upper()
        if base != symbol_upper:
            continue
        market_name = t.get("market", {}).get("name", "")
        norm = normalize_exchange_name(market_name)
        if norm:
            exchs.add(norm)
    return exchs


def choose_platforms(coin: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    Возвращает список (chain, address) для наших сетей.
    """
    platforms = coin.get("platforms") or {}
    out: List[Tuple[str, str]] = []
    for platform_name, addr in platforms.items():
        if not addr:
            continue
        chain = PLATFORM_TO_CHAIN.get(platform_name.lower())
        if not chain:
            continue
        if chain not in ALLOWED_CHAINS:
            continue
        out.append((chain, addr))
    return out


def find_coingecko_coin_for_listedon_item(item: ListedonItem) -> Optional[Dict[str, Any]]:
    """
    Ищем коин по тикеру и бирже.
    """
    symbol = item.symbol.upper()

    search = cg_get("/search", params={"query": symbol})
    candidates_ids = [c["id"] for c in search.get("coins", [])]

    best_coin = None
    best_mcap = 0.0

    for cid in candidates_ids:
        try:
            coin = cg_get(
                f"/coins/{cid}",
                params={
                    "localization": "false",
                    "tickers": "true",
                    "market_data": "true",
                    "community_data": "false",
                    "developer_data": "false",
                    "sparkline": "false",
                },
            )
        except Exception:
            # пропускаем, если коингеко что-то не отдал
            continue

        if (coin.get("symbol") or "").upper() != symbol:
            continue

        # сети
        platforms = choose_platforms(coin)
        if not platforms:
            continue

        market_data = coin.get("market_data") or {}
        mcap = (market_data.get("market_cap") or {}).get("usd")
        if not isinstance(mcap, (int, float)):
            continue
        if not (MIN_MCAP_USD <= mcap <= MAX_MCAP_USD):
            continue

        exchs = get_exchanges_for_coin(coin, symbol_upper=symbol)

        # должен быть на исходной бирже
        if item.exchange_key not in exchs:
            continue

        # и хотя бы на 2 биржах из TARGET_EXCHANGES
        exchs_in_list = exchs & TARGET_EXCHANGES
        if len(exchs_in_list) < 2:
            continue

        # выбираем с наибольшей mcap, если вдруг несколько id
        if mcap > best_mcap:
            best_mcap = mcap
            best_coin = coin

        # пауза между запросами
        time.sleep(0.5)

    return best_coin


# ---------------------------
# Работа с token_map.json
# ---------------------------

def load_token_map(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_token_map(path: Path, data: List[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")


def build_existing_sets(token_map: List[Dict[str, Any]]):
    by_chain_addr: Set[Tuple[str, str]] = set()
    by_cg_id: Set[str] = set()

    for t in token_map:
        chain = (t.get("chain") or "").lower()
        addr = (t.get("address") or "").lower()
        if chain and addr:
            by_chain_addr.add((chain, addr))
        cg_id = t.get("coingecko_id")
        if cg_id:
            by_cg_id.add(cg_id)
    return by_chain_addr, by_cg_id


def create_entries_from_coin(
    coin: Dict[str, Any],
    item: ListedonItem,
    existing_chain_addr: Set[Tuple[str, str]],
    existing_cg_ids: Set[str],
) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []

    cg_id = coin["id"]
    symbol = coin["symbol"].upper()
    name = coin["name"]

    exchs = get_exchanges_for_coin(coin, symbol_upper=symbol)
    exchs_in_list = sorted(list(exchs & TARGET_EXCHANGES))

    for chain, addr in choose_platforms(coin):
        key = (chain.lower(), addr.lower())
        if key in existing_chain_addr:
            # уже есть в token_map по этому адресу
            continue

        # если хотим жёстко не дублировать cg_id вообще:
        # if cg_id in existing_cg_ids:
        #     continue

        entry = {
            "symbol": symbol,
            "name": name,
            "chain": chain,
            "address": addr,
            "coingecko_id": cg_id,
            "active": True,
            # новые поля:
            "listed_at": item.listed_at.date().isoformat(),
            "listed_exchanges": exchs_in_list,
        }
        entries.append(entry)

    return entries


# ---------------------------
# main
# ---------------------------

def main() -> None:
    print("Loading existing token_map.json...")
    token_map = load_token_map(TOKEN_MAP_PATH)
    existing_chain_addr, existing_cg_ids = build_existing_sets(token_map)

    print(f"Existing tokens: {len(token_map)}")

    print("Fetching listedon data...")
    items = fetch_all_listedon_items()
    print(f"Listedon items in window [{MIN_AGE_DAYS}, {MAX_AGE_DAYS}] days: {len(items)}")

    # чтобы не обрабатывать один и тот же символ/биржу по нескольку раз
    seen_symbol_exchange: Set[Tuple[str, str]] = set()
    new_entries: List[Dict[str, Any]] = []

    for item in items:
        key = (item.symbol, item.exchange_key)
        if key in seen_symbol_exchange:
            continue
        seen_symbol_exchange.add(key)

        print(f"\n=== Processing {item.symbol} from {item.exchange_key} (listed {item.listed_at.date()}) ===")
        try:
            coin = find_coingecko_coin_for_listedon_item(item)
        except Exception as e:
            print(f"  Error while searching in Coingecko: {e}")
            continue

        if not coin:
            print("  No suitable Coingecko coin found (by symbol/exchanges/mcap/chains)")
            continue

        print(f"  Matched Coingecko id={coin['id']} name={coin['name']} symbol={coin['symbol']}")
        entries = create_entries_from_coin(coin, item, existing_chain_addr, existing_cg_ids)

        if not entries:
            print("  No new chain/address entries to add (already present or filtered)")
            continue

        for e in entries:
            print(f"  + adding {e['symbol']} on {e['chain']} at {e['address']}, listed_at={e['listed_at']}, exchanges={','.join(e['listed_exchanges'])}")
            token_map.append(e)
            existing_chain_addr.add((e["chain"].lower(), e["address"].lower()))
            existing_cg_ids.add(e["coingecko_id"])
            new_entries.append(e)

        # маленькая пауза между конченными монетами
        time.sleep(0.5)

    if not new_entries:
        print("\nNo new tokens to add.")
        return

    # можно отсортировать по символу/сети для красоты
    token_map_sorted = sorted(token_map, key=lambda t: (t.get("symbol", ""), t.get("chain", "")))

    print(f"\nSaving updated token_map.json (total {len(token_map_sorted)}, new {len(new_entries)})")
    save_token_map(TOKEN_MAP_PATH, token_map_sorted)
    print("Done.")


if __name__ == "__main__":
    main()
