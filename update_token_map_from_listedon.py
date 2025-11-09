#!/usr/bin/env python3
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple, Set, Optional

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone, date
from dateutil import parser as dateparser

# ---------------------------
# Конфиг
# ---------------------------

# Файл с токенами (тот же формат, что у тебя сейчас)
TOKEN_MAP_PATH = Path("token_map.json")
OUTPUT_PATH = Path("token_map.updated.json")

# Откуда берём кандидатов (листинги MEXC)
LISTEDON_URL_TEMPLATE = (
    "https://listedon.org/en/exchange/mxc/search?sort=date&order=1&page={page}"
)

# Сколько максимум тикеров собирать (можно 1000)
MAX_TICKERS = 1000

# Возраст листинга на MEXC (в днях)
# Для первой прогонки:  MIN_AGE_DAYS=7,  MAX_AGE_DAYS=90
# Для еженедельных обновлений: MIN_AGE_DAYS=7, MAX_AGE_DAYS=14
MIN_AGE_DAYS = int(os.getenv("MIN_AGE_DAYS", "7"))
MAX_AGE_DAYS = int(os.getenv("MAX_AGE_DAYS", "90"))

# Market cap фильтр (USD)
MIN_MCAP = 3_000_000
MAX_MCAP = 1_000_000_000

# Поддерживаемые платформы CoinGecko -> chain в token_map.json
SUPPORTED_PLATFORMS = {
    "ethereum": "ethereum",
    "binance-smart-chain": "bnb",
    "bnb-smart-chain": "bnb",
    "bsc": "bnb",
    "solana": "solana",
}

# Биржи, которые считаем
TARGET_EXCHANGES = {
    "binance": "binance",
    "gate": "gate",
    "gate.io": "gate",
    "kucoin": "kucoin",
    "mexc": "mexc",
    "mxc": "mexc",
    "bybit": "bybit",
    "htx": "htx",
    "huobi": "htx",
    "bingx": "bingx",
}

MIN_EXCHANGES_REQUIRED = 3

COINGECKO_BASE = "https://api.coingecko.com/api/v3"


# ---------------------------
# Утиль
# ---------------------------

def fetch_url(url: str, params: Dict[str, Any] | None = None) -> requests.Response:
    """GET с лёгким слипом, чтобы не спамить API."""
    time.sleep(1.0)
    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    return resp


def parse_date(text: str) -> Optional[date]:
    """Парсим дату листинга с listedon.org."""
    text = (text or "").strip()
    if not text:
        return None
    try:
        dt = dateparser.parse(text)
        return dt.date()
    except Exception:
        return None


def get_mexc_listings_in_age_range(
    min_age_days: int,
    max_age_days: int,
    limit: int = MAX_TICKERS,
) -> List[Tuple[str, date]]:
    """
    Возвращаем список (ticker, listing_date) для MEXC,
    отфильтрованный по возрасту листинга.
    """
    today = datetime.now(timezone.utc).date()

    tickers: List[Tuple[str, date]] = []
    seen: Set[str] = set()
    page = 1

    while len(tickers) < limit:
        url = LISTEDON_URL_TEMPLATE.format(page=page)
        print(f"[listedon] page {page}: {url}")
        resp = fetch_url(url)
        soup = BeautifulSoup(resp.text, "html.parser")

        rows = soup.select("table tbody tr")
        if not rows:
            # если таблицы нет — вероятно, закончились страницы
            break

        page_had_any = False

        for row in rows:
            # предполагаем: первая <a href="/en/coin/..."> — тикер
            a = row.select_one('a[href^="/en/coin/"]')
            if not a:
                continue

            symbol = a.get_text(strip=True)
            if not symbol:
                continue

            # предполагаем, что последняя колонка — дата листинга
            tds = row.find_all("td")
            if not tds:
                continue

            date_text = tds[-1].get_text(strip=True)
            listing_date = parse_date(date_text)
            if not listing_date:
                continue

            age_days = (today - listing_date).days

            # слишком свежие (< min_age) — пропускаем (наберём их на следующих запусках)
            if age_days < min_age_days:
                continue

            # слишком старые (> max_age) — можно заканчивать,
            # т.к. дальше по страницам будет ещё старее
            if age_days > max_age_days:
                continue

            page_had_any = True

            if symbol in seen:
                continue

            seen.add(symbol)
            tickers.append((symbol, listing_date))

            print(f"  + {symbol} (listed {listing_date}, age={age_days}d)")

            if len(tickers) >= limit:
                break

        if len(tickers) >= limit:
            break

        # если на странице ничего в нужном диапазоне не нашли — следующая страница
        # скорее всего ещё старее, но мы ещё дадим шанс
        page += 1

        # на всякий случай ограничим количество страниц (чтоб не зациклиться)
        if page > 50:
            break

    print(f"[listedon] collected {len(tickers)} tickers in age range [{min_age_days},{max_age_days}] days")
    return tickers


def coingecko_search(query: str) -> List[Dict[str, Any]]:
    url = f"{COINGECKO_BASE}/search"
    resp = fetch_url(url, params={"query": query})
    data = resp.json()
    return data.get("coins", [])


def coingecko_get_coin_full(coin_id: str) -> Dict[str, Any]:
    url = f"{COINGECKO_BASE}/coins/{coin_id}"
    resp = fetch_url(
        url,
        params={
            "localization": "false",
            "tickers": "true",
            "market_data": "true",
            "community_data": "false",
            "developer_data": "false",
            "sparkline": "false",
        },
    )
    return resp.json()


def extract_supported_contracts(coin: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    Достаём (chain, address) только для нужных сетей.
    """
    platforms = coin.get("platforms") or {}
    res: List[Tuple[str, str]] = []
    for platform_id, contract in platforms.items():
        if not contract:
            continue
        chain = SUPPORTED_PLATFORMS.get(platform_id.lower())
        if not chain:
            continue
        res.append((chain, contract))
    return res


def coin_passes_mcap_and_chain(coin: Dict[str, Any]) -> bool:
    market_data = coin.get("market_data") or {}
    mcap_usd = (market_data.get("market_cap") or {}).get("usd")

    if not isinstance(mcap_usd, (int, float)):
        return False

    if not (MIN_MCAP <= mcap_usd <= MAX_MCAP):
        return False

    contracts = extract_supported_contracts(coin)
    if not contracts:
        return False

    return True


def get_exchange_coverage_from_tickers(tickers: List[Dict[str, Any]]) -> Set[str]:
    """
    Берём список бирж из CoinGecko tickers и мапим в наш TARGET_EXCHANGES.
    """
    hit: Set[str] = set()

    for t in tickers:
        market = t.get("market") or {}
        ex_id = (market.get("identifier") or "").lower()
        ex_name = (market.get("name") or "").lower()
        candidates = {ex_id, ex_name}

        for cand in candidates:
            for pattern, label in TARGET_EXCHANGES.items():
                if pattern in cand:
                    hit.add(label)

    return hit


def build_token_entry(
    coin: Dict[str, Any],
    chain: str,
    address: str,
) -> Dict[str, Any]:
    return {
        "symbol": (coin.get("symbol") or "").upper(),
        "name": coin.get("name", (coin.get("symbol") or "").upper()),
        "chain": chain,
        "address": address,
        "coingecko_id": coin.get("id"),
        "active": True,
    }


# ---------------------------
# Основная логика
# ---------------------------

def main():
    # 1) читаем текущий token_map.json
    if TOKEN_MAP_PATH.exists():
        with open(TOKEN_MAP_PATH, "r", encoding="utf-8") as f:
            try:
                existing_tokens: List[Dict[str, Any]] = json.load(f)
            except json.JSONDecodeError as exc:
                print(f"ERROR: token_map.json is invalid JSON: {exc}")
                existing_tokens = []
    else:
        existing_tokens = []

    # индексы для дедупликации
    existing_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for t in existing_tokens:
        chain = str(t.get("chain", "")).lower()
        addr = str(t.get("address", "")).lower()
        if chain and addr:
            existing_by_key[(chain, addr)] = t

    # 2) собираем тикеры с listedon.org по возрасту листинга
    mexc_listings = get_mexc_listings_in_age_range(
        min_age_days=MIN_AGE_DAYS,
        max_age_days=MAX_AGE_DAYS,
        limit=MAX_TICKERS,
    )

    # 3) по каждому тикеру — CoinGecko + фильтры
    new_entries: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for symbol, listing_date in mexc_listings:
        print(f"\n=== Processing {symbol} (listed {listing_date}) ===")

        try:
            candidates = coingecko_search(symbol)
        except Exception as e:
            print(f"[coingecko search] failed for {symbol}: {e}")
            continue

        if not candidates:
            print(f"[coingecko search] no candidates for {symbol}")
            continue

        valid_coins: List[Dict[str, Any]] = []

        for cand in candidates:
            coin_id = cand.get("id")
            if not coin_id:
                continue

            try:
                full = coingecko_get_coin_full(coin_id)
            except Exception as e:
                print(f"[coingecko coin] failed for {coin_id}: {e}")
                continue

            # mcap + сети
            if not coin_passes_mcap_and_chain(full):
                continue

            # биржи
            cex_hits = get_exchange_coverage_from_tickers(full.get("tickers") or [])
            if len(cex_hits) < MIN_EXCHANGES_REQUIRED:
                continue

            # лог: посмотрим, что за кандидаты проходят
            print(
                f"  candidate {coin_id}: "
                f"chains={extract_supported_contracts(full)}, "
                f"cex_hits={sorted(cex_hits)}"
            )
            valid_coins.append(full)

        if not valid_coins:
            print(f"[filter] no valid coins for {symbol}")
            continue

        # выбираем кандидата с максимальным mcap
        def _mcap(coin: Dict[str, Any]) -> float:
            md = coin.get("market_data") or {}
            return float((md.get("market_cap") or {}).get("usd") or 0.0)

        valid_coins.sort(key=_mcap, reverse=True)
        selected = valid_coins[0]
        coin_id = selected.get("id")
        print(f"  -> selected {coin_id}")

        # 4) создаём записи по всем нужным сетям
        for chain, addr in extract_supported_contracts(selected):
            key = (chain.lower(), addr.lower())

            if key in existing_by_key:
                print(f"    skip {chain} {addr} — already in token_map.json")
                continue

            if key in new_entries:
                print(f"    duplicate {chain} {addr} in this run, skip")
                continue

            entry = build_token_entry(selected, chain=chain, address=addr)
            new_entries[key] = entry
            print(f"    + add entry: {entry['symbol']} {chain} {addr}")

    # 5) финальный список: старые + новые
    final_tokens: List[Dict[str, Any]] = []
    final_tokens.extend(existing_tokens)
    final_tokens.extend(new_entries.values())

    # (опционально) сортируем
    final_tokens.sort(key=lambda t: (t.get("symbol", ""), t.get("chain", "")))

    # 6) пишем обновлённый список
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(final_tokens, f, ensure_ascii=False, indent=2)

    print(f"\nDone. Existing: {len(existing_tokens)}, added: {len(new_entries)}, total: {len(final_tokens)}")
    print(f"Wrote updated token map to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
