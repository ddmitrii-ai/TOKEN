#!/usr/bin/env python3
import os
import sys
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import requests  # pip install requests


COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY", "")
CG_BASE_URL = "https://api.coingecko.com/api/v3"

# фильтр по mcap
MIN_MCAP = 1_000_000      # 1M
MAX_MCAP = 1_000_000_000  # 1B

# сколько страниц CoinGecko markets максимум смотреть (250 на страницу)
MAX_PAGES = 20

# какие сети нам нужны и как маппить из CoinGecko platform name
PLATFORM_TO_CHAIN: Dict[str, str] = {
    "ethereum": "ethereum",
    "binance-smart-chain": "bnb",
    "bnb-smart-chain": "bnb",
    "bnb-chain": "bnb",  # на всякий
    "solana": "solana",
}

ALLOWED_CHAINS = {"ethereum", "bnb", "solana"}

# таргет биржи и как их узнавать в CoinGecko tickers
TARGET_EXCHANGES = {"binance", "gate", "kucoin", "mexc", "bybit", "htx"}

EXCHANGE_MATCH_SUBSTRINGS: Dict[str, List[str]] = {
    "binance": ["binance"],
    "gate": ["gate.io", "gate"],
    "kucoin": ["kucoin"],
    "mexc": ["mexc"],
    "bybit": ["bybit"],
    "htx": ["htx", "huobi"],  # ребрендинг Huobi -> HTX
}


def cg_request(path: str, params: Dict[str, Any]) -> Any:
    url = CG_BASE_URL.rstrip("/") + path
    headers = {
        "Accept": "application/json",
        "User-Agent": "MaTT-Scanner/1.0",
    }
    if COINGECKO_API_KEY:
        headers["x-cg-demo-api-key"] = COINGECKO_API_KEY  # или x-cg-pro-api-key, если у тебя PRO

    resp = requests.get(url, headers=headers, params=params, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"CoinGecko error {resp.status_code}: {resp.text[:200]}")
    return resp.json()


def normalize_symbol(sym: str) -> str:
    return (sym or "").upper()


def detect_exchanges_from_tickers(tickers: List[Dict[str, Any]]) -> set:
    present = set()
    for t in tickers:
        market = t.get("market") or {}
        name_l = (market.get("name") or "").lower()
        ident_l = (market.get("identifier") or "").lower()

        for ex, subs in EXCHANGE_MATCH_SUBSTRINGS.items():
            for s in subs:
                if s in name_l or s in ident_l:
                    present.add(ex)
                    break
    return present


def fetch_markets_candidates() -> List[Dict[str, Any]]:
    """
    Берём монеты с CoinGecko по убыванию market cap,
    отфильтровываем только те, у которых mcap в нашем диапазоне.
    """
    candidates: List[Dict[str, Any]] = []
    for page in range(1, MAX_PAGES + 1):
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 250,
            "page": page,
            "sparkline": "false",
        }
        data = cg_request("/coins/markets", params)
        if not data:
            break

        stop = False
        for coin in data:
            mcap = coin.get("market_cap")
            if not isinstance(mcap, (int, float)):
                continue

            # если market cap уже ниже минимума – дальше только меньше
            if mcap < MIN_MCAP:
                stop = True
                break

            if MIN_MCAP <= mcap <= MAX_MCAP:
                candidates.append(
                    {
                        "id": coin["id"],
                        "symbol": coin["symbol"],
                        "name": coin["name"],
                        "market_cap": mcap,
                    }
                )

        if stop:
            break

        # немного паузы, чтобы не спамить API
        time.sleep(1.0)

    return candidates


def fetch_coin_details(coin_id: str) -> Dict[str, Any]:
    params = {
        "localization": "false",
        "tickers": "true",
        "market_data": "false",
        "community_data": "false",
        "developer_data": "false",
        "sparkline": "false",
    }
    return cg_request(f"/coins/{coin_id}", params)


def extract_platforms(coin: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    Из CoinGecko coin-объекта достаём список (chain, address)
    только для интересующих нас сетей.
    """
    res: List[Tuple[str, str]] = []

    platforms: Dict[str, str] = coin.get("platforms") or {}
    for platform_name, addr in platforms.items():
        if not addr:
            continue
        chain = PLATFORM_TO_CHAIN.get(platform_name)
        if not chain:
            continue
        if chain not in ALLOWED_CHAINS:
            continue
        res.append((chain, addr))

    # иногда у токена в платформе одна запись, но ещё есть top-level asset_platform_id + contract_address
    if not res:
        platform_name = coin.get("asset_platform_id")
        addr = coin.get("contract_address")
        if platform_name and addr:
            chain = PLATFORM_TO_CHAIN.get(platform_name)
            if chain in ALLOWED_CHAINS:
                res.append((chain, addr))

    return res


def make_token_entry(
    symbol: str,
    name: str,
    chain: str,
    address: str,
    coingecko_id: str,
    active: bool = True,
) -> Dict[str, Any]:
    return {
        "symbol": normalize_symbol(symbol),
        "name": name,
        "chain": chain,
        "address": address,
        "coingecko_id": coingecko_id,
        "active": active,
    }


def scan_tokens() -> List[Dict[str, Any]]:
    """
    Главная логика:
      - берём кандидатов по market cap
      - по каждому тянем детали
      - фильтруем по:
          * сети (eth/bnb/sol)
          * наличию хотя бы 3 целевых CEX
      - собираем token_map (без дублей по (chain, address))
    """
    candidates = fetch_markets_candidates()
    print(f"[+] Markets candidates in range {MIN_MCAP}..{MAX_MCAP}: {len(candidates)}", file=sys.stderr)

    tokens_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for i, c in enumerate(candidates, start=1):
        coin_id = c["id"]
        try:
            coin = fetch_coin_details(coin_id)
        except Exception as e:
            print(f"[!] Failed to fetch {coin_id}: {e}", file=sys.stderr)
            continue

        tickers = coin.get("tickers") or []
        ex_set = detect_exchanges_from_tickers(tickers)

        # условие: хотя бы 3 из целевого списка
        if len(ex_set & TARGET_EXCHANGES) < 3:
            continue

        platforms = extract_platforms(coin)
        if not platforms:
            continue

        symbol = coin.get("symbol") or ""
        name = coin.get("name") or ""

        for chain, addr in platforms:
            key = (chain, addr.lower())
            entry = make_token_entry(symbol, name, chain, addr, coin_id, active=True)
            entry["_market_cap"] = c["market_cap"]  # для выбора лучшего, если дубликат

            if key in tokens_by_key:
                # если уже есть – берём тот, у кого больше mcap
                if entry["_market_cap"] > tokens_by_key[key]["_market_cap"]:
                    tokens_by_key[key] = entry
            else:
                tokens_by_key[key] = entry

        # пауза, чтобы не убить лимит CoinGecko
        time.sleep(0.5)

    # чистим служебное поле _market_cap и собираем финальный список
    result: List[Dict[str, Any]] = []
    for entry in tokens_by_key.values():
        entry.pop("_market_cap", None)
        result.append(entry)

    # сортируем по символу и имени, чтобы было красиво
    result.sort(key=lambda x: (x["symbol"], x["chain"]))
    return result


def main():
    out_path = Path("token_map.json")
    tokens = scan_tokens()
    print(f"[+] Final tokens count: {len(tokens)}", file=sys.stderr)

    out_path.write_text(
        json.dumps(tokens, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[+] Saved token_map.json -> {out_path.resolve()}", file=sys.stderr)


if __name__ == "__main__":
    main()
