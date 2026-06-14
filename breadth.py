#!/usr/bin/env python3
"""
Market-breadth pipeline: "US Comm Stks 5 Day up 20%" (TC2000 replication).

For each US trading day D, count NYSE/NASDAQ stock-market symbols whose
close(D) / close(D - 5 trading sessions) - 1 >= threshold (default 0.20).

Data source: Massive.com REST API (Polygon-compatible).
  - Universe:        GET /v3/reference/tickers   (market=stocks, active=true)
  - Daily bars:      GET /v2/aggs/grouped/locale/us/market/stocks/{date}
  - Trading calendar is derived empirically: a date is a trading day iff the
    grouped-daily endpoint returns rows for it.

Output: breadth_5d_up20.json -> [{"date":"YYYY-MM-DD","value":<int>}, ...] asc.

The API key is read from the environment variable MASSIVE_API_KEY. It is never
printed or persisted.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import datetime as dt
import gzip
import json
import os
import sys
import threading
import time
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

API_BASE = "https://api.massive.com"
ET = ZoneInfo("America/New_York")

# Primary-exchange MIC codes that count as our two venues.
NYSE_MICS = {"XNYS"}
# Massive returns a single "XNAS" for all NASDAQ tiers today; the others are
# accepted defensively in case tiered codes ever appear.
NASDAQ_MICS = {"XNAS", "XNGS", "XNMS", "XNCM"}
ALLOWED_MICS = NYSE_MICS | NASDAQ_MICS

# Session is considered final after this ET hour (lets EOD consolidation settle).
SESSION_FINAL_HOUR_ET = 20


# --------------------------------------------------------------------------- #
# HTTP helpers
# --------------------------------------------------------------------------- #
class MassiveClient:
    def __init__(self, api_key: str, max_retries: int = 6):
        if not api_key:
            raise SystemExit("MASSIVE_API_KEY is not set in the environment.")
        self._key = api_key
        self.max_retries = max_retries
        self._local = threading.local()

    @property
    def _session(self) -> requests.Session:
        # One Session per thread (requests.Session is not thread-safe to share).
        s = getattr(self._local, "session", None)
        if s is None:
            s = requests.Session()
            s.headers.update({"Authorization": f"Bearer {self._key}"})
            self._local.session = s
        return s

    def get(self, path: str, params: dict | None = None) -> dict:
        url = path if path.startswith("http") else f"{API_BASE}{path}"
        backoff = 1.0
        for attempt in range(1, self.max_retries + 1):
            try:
                r = self._session.get(url, params=params, timeout=60)
            except requests.RequestException as e:
                if attempt == self.max_retries:
                    raise
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429 or 500 <= r.status_code < 600:
                # Respect Retry-After when present.
                ra = r.headers.get("Retry-After")
                sleep_s = float(ra) if ra and ra.isdigit() else backoff
                if attempt == self.max_retries:
                    r.raise_for_status()
                time.sleep(sleep_s)
                backoff = min(backoff * 2, 30)
                continue
            # Other 4xx: don't leak the key (it's only in the header), surface status.
            raise SystemExit(f"HTTP {r.status_code} for {path}: {r.text[:300]}")
        raise SystemExit(f"Exhausted retries for {path}")


# --------------------------------------------------------------------------- #
# Universe
# --------------------------------------------------------------------------- #
def build_universe(client: MassiveClient, common_stock_only: bool) -> set[str]:
    """Current active NYSE/NASDAQ stock-market symbols."""
    tickers: set[str] = set()
    params = {
        "market": "stocks",
        "active": "true",
        "limit": 1000,
        "order": "asc",
        "sort": "ticker",
    }
    path = "/v3/reference/tickers"
    next_url = None
    pages = 0
    while True:
        data = client.get(next_url or path, None if next_url else params)
        pages += 1
        for r in data.get("results", []):
            if r.get("market") != "stocks" or not r.get("active"):
                continue
            if r.get("primary_exchange") not in ALLOWED_MICS:
                continue
            if common_stock_only and r.get("type") != "CS":
                continue
            tickers.add(r["ticker"])
        next_url = data.get("next_url")
        if not next_url:
            break
    print(f"Universe: {len(tickers)} symbols "
          f"({'CS only' if common_stock_only else 'all stock types'}, "
          f"NYSE+NASDAQ, {pages} pages).")
    return tickers


# --------------------------------------------------------------------------- #
# Grouped daily bars (with on-disk cache)
# --------------------------------------------------------------------------- #
def cache_path(cache_dir: Path, date: str) -> Path:
    return cache_dir / f"grouped-{date}.json.gz"


def load_cached(cache_dir: Path, date: str) -> dict[str, float] | None:
    p = cache_path(cache_dir, date)
    if not p.exists():
        return None
    with gzip.open(p, "rt", encoding="utf-8") as f:
        return json.load(f)


def store_cache(cache_dir: Path, date: str, closes: dict[str, float]) -> None:
    p = cache_path(cache_dir, date)
    tmp = p.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        json.dump(closes, f, separators=(",", ":"))
    tmp.replace(p)


def fetch_grouped(client: MassiveClient, date: str) -> dict[str, float]:
    """All stock closes for `date`. Empty dict == no trading session that day."""
    data = client.get(
        f"/v2/aggs/grouped/locale/us/market/stocks/{date}",
        {"adjusted": "true"},
    )
    closes: dict[str, float] = {}
    for row in data.get("results", []) or []:
        t = row.get("T")
        c = row.get("c")
        if t and isinstance(c, (int, float)) and c > 0:
            closes[t] = float(c)
    return closes


def candidate_dates(start: dt.date, end: dt.date):
    """Weekdays in [start, end]; weekends are never trading days."""
    d = start
    one = dt.timedelta(days=1)
    while d <= end:
        if d.weekday() < 5:  # Mon-Fri
            yield d
        d += one


def gather_sessions(
    client: MassiveClient,
    cache_dir: Path,
    start: dt.date,
    end: dt.date,
    workers: int,
) -> dict[str, dict[str, float]]:
    """Return {date: closes} for every cached/fetched weekday (incl. empty holidays)."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    result: dict[str, dict[str, float]] = {}
    to_fetch: list[str] = []

    for d in candidate_dates(start, end):
        ds = d.isoformat()
        cached = load_cached(cache_dir, ds)
        if cached is not None:
            result[ds] = cached
        else:
            to_fetch.append(ds)

    if to_fetch:
        print(f"Fetching {len(to_fetch)} uncached weekdays "
              f"(cache hits: {len(result)})...")

        def task(ds: str):
            closes = fetch_grouped(client, ds)
            store_cache(cache_dir, ds, closes)  # cache empty days too (holidays)
            return ds, closes

        done = 0
        with cf.ThreadPoolExecutor(max_workers=workers) as ex:
            for ds, closes in ex.map(task, to_fetch):
                result[ds] = closes
                done += 1
                if done % 25 == 0 or done == len(to_fetch):
                    print(f"  ...{done}/{len(to_fetch)}")
    else:
        print(f"All {len(result)} weekdays served from cache.")

    return result


# --------------------------------------------------------------------------- #
# Metric
# --------------------------------------------------------------------------- #
def compute_series(
    sessions: dict[str, dict[str, float]],
    universe: set[str],
    lookback: int,
    threshold: float,
) -> list[dict]:
    # Trading days = dates that actually have closes, ascending.
    trading_days = sorted(d for d, closes in sessions.items() if closes)
    out: list[dict] = []
    factor = 1.0 + threshold
    for i in range(lookback, len(trading_days)):
        d_now = trading_days[i]
        d_past = trading_days[i - lookback]
        cur = sessions[d_now]
        past = sessions[d_past]
        count = 0
        for sym in universe:
            c_now = cur.get(sym)
            if c_now is None:
                continue
            c_past = past.get(sym)
            if c_past is None or c_past <= 0:
                continue
            if c_now >= c_past * factor:
                count += 1
        out.append({"date": d_now, "value": count})
    return out


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser(description="US 5-day up-20% market-breadth series.")
    ap.add_argument("--out", default="breadth_5d_up20.json")
    ap.add_argument("--cache-dir", default="cache")
    ap.add_argument("--backfill-years", type=float, default=2.0)
    ap.add_argument("--lookback", type=int, default=5, help="trading-session offset")
    ap.add_argument("--threshold", type=float, default=0.20, help="min fractional gain")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument(
        "--common-stock-only",
        action="store_true",
        default=os.environ.get("COMMON_STOCK_ONLY", "").lower() in ("1", "true", "yes"),
        help="restrict universe to type==CS (default False)",
    )
    ap.add_argument("--tc2000", metavar="YYYY-MM-DD=VALUE",
                    help="known TC2000 value to compare against for tuning")
    args = ap.parse_args()

    client = MassiveClient(os.environ.get("MASSIVE_API_KEY", ""))

    now_et = dt.datetime.now(ET)
    today_et = now_et.date()
    # Most recent date whose session is final (EOD settled). Never emit a
    # still-forming current day.
    if now_et.hour >= SESSION_FINAL_HOUR_ET:
        end = today_et
    else:
        end = today_et - dt.timedelta(days=1)

    # Window start: backfill span plus a small buffer so the earliest emitted
    # date already has its lookback-th prior session available.
    span_days = int(args.backfill_years * 365.25)
    series_start = today_et - dt.timedelta(days=span_days)
    fetch_start = series_start - dt.timedelta(days=20)  # buffer for the offset

    universe = build_universe(client, args.common_stock_only)
    sessions = gather_sessions(client, Path(args.cache_dir), fetch_start, end, args.workers)
    series = compute_series(sessions, universe, args.lookback, args.threshold)

    # Trim to the requested backfill window (earlier dates were only buffer).
    series = [pt for pt in series if pt["date"] >= series_start.isoformat()]
    series.sort(key=lambda p: p["date"])

    Path(args.out).write_text(json.dumps(series, indent=2) + "\n", encoding="utf-8")
    print(f"\nWrote {len(series)} points to {args.out} "
          f"(range {series[0]['date'] if series else 'n/a'} .. "
          f"{series[-1]['date'] if series else 'n/a'}).")

    # ----- validation -----
    tail = series[-10:]
    print("\nLast 10 values:")
    for pt in tail:
        print(f"  {pt['date']}  {pt['value']}")
    if series:
        vals = [p["value"] for p in series]
        print(f"\nStats: min={min(vals)} max={max(vals)} "
              f"mean={sum(vals)/len(vals):.1f} n={len(vals)}")
        if max(vals) > 1000:
            print("WARNING: magnitude looks too high (>1000); check the universe filter.")

    if args.tc2000:
        date_s, _, val_s = args.tc2000.partition("=")
        want = int(val_s)
        got = next((p["value"] for p in series if p["date"] == date_s), None)
        if got is None:
            print(f"\nTC2000 check: {date_s} not in series (not a trading day in window?).")
        else:
            diff = got - want
            print(f"\nTC2000 check {date_s}: ours={got} tc2000={want} "
                  f"diff={diff:+d} ({abs(diff)/want*100:.1f}% off)" if want else
                  f"\nTC2000 check {date_s}: ours={got} tc2000={want} diff={diff:+d}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
