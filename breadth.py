#!/usr/bin/env python3
"""
Market-breadth pipeline: Stockbee "US Comm Stks 5 Day up 20%" (Pradeep Bonde).

For each US trading day D, count common stocks listed on the configured venues
where ALL of the following hold:

  1. close(D) / close(D - 5 trading sessions) - 1 >= 0.20   (5-session +20% move)
  2. close(D) >= 5                                            ($5 price floor)
  3. min( vol(D-1), vol(D-2), vol(D-3) ) > 100000             (TC2000 minv3.1)

All bars are split/dividend-adjusted (adjusted=true), so volume and price are on
a consistent basis. Days lacking 5 prior closes / 3 prior volumes are skipped.

Data source: Massive.com REST API (Polygon-compatible).
  - Universe:   GET /v3/reference/tickers  (active=true AND active=false ->
                survivorship-free: delisted names are classified too)
  - Daily bars: GET /v2/aggs/grouped/locale/us/market/stocks/{date}
  - Trading calendar derived empirically (a date is a session iff grouped
    returns rows). Point-in-time candidate set per day = symbols present in that
    day's bars (so delisted names are counted on the days they traded).

Output: breadth_5d_up20.json -> [{"date":"YYYY-MM-DD","value":<int>}, ...] asc.

The API key is read from MASSIVE_API_KEY. It is never printed or persisted.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import datetime as dt
import gzip
import json
import os
import statistics
import sys
import threading
import time
from collections import Counter
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

API_BASE = "https://api.massive.com"
ET = ZoneInfo("America/New_York")

# ============================ UNIVERSE CONFIG ============================== #
# Listing-venue whitelist: venue label -> set of `primary_exchange` MIC codes
# Massive uses for that venue. Confirmed empirically by build_universe()'s
# diagnostics (do not assume codes).
EXCHANGE_MICS = {
    "NYSE":          {"XNYS"},
    "NASDAQ":        {"XNAS", "XNGS", "XNMS", "XNCM"},  # all NASDAQ tiers
    "NYSE American": {"XASE"},   # AMEX
    "NYSE Arca":     {"ARCX"},   # mostly ETFs -> ~0 CS (kept anyway)
    "Cboe BZX":      {"BATS"},   # mostly ETFs -> ~0 CS (kept anyway)
}
# Security-type whitelist: keep ONLY these Massive `type` codes ("CS" = common).
TYPE_WHITELIST = {"CS"}
ALLOWED_MICS = {mic for mics in EXCHANGE_MICS.values() for mic in mics}

# ========================== QUALIFIER THRESHOLDS =========================== #
# Stockbee "US Comm Stks 5 Day up 20%". Tune here.
MOVE_LOOKBACK = 5         # trading sessions for the % move (D vs D-5)
MOVE_THRESHOLD = 0.20     # close(D)/close(D-5) - 1 >= this
PRICE_FLOOR = 5.0         # close(D) >= this ($)
VOL_LOOKBACK = 3          # use volumes of D-1 .. D-VOL_LOOKBACK
MIN_VOLUME = 100_000      # min(volume over that window) must be STRICTLY > this

# ============================== HISTORY ==================================== #
# Earliest date the subscription serves grouped data (probed 2026-06: the plan
# is a rolling ~10y window; 2016-06-17 was the earliest 200, 2016-06-16 -> 403).
# Pre-coverage dates return HTTP 403 and are skipped gracefully.
START_DATE = "2016-06-17"

# Session considered final after this ET hour (EOD settle); current forming day
# is never emitted.
SESSION_FINAL_HOUR_ET = 20


class RateLimitError(RuntimeError):
    """Raised when the API rate/quota limit blocks progress (resume by re-running)."""


# --------------------------------------------------------------------------- #
# HTTP client (thread-safe, retry/backoff)
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
        s = getattr(self._local, "session", None)
        if s is None:
            s = requests.Session()
            s.headers.update({"Authorization": f"Bearer {self._key}"})
            self._local.session = s
        return s

    def get(self, path: str, params: dict | None = None,
            allow_forbidden: bool = False) -> dict | None:
        """GET with retry/backoff. Returns parsed JSON, or None on 403 when
        allow_forbidden (used to detect pre-coverage dates). Raises
        RateLimitError if 429 persists past retries."""
        url = path if path.startswith("http") else f"{API_BASE}{path}"
        backoff = 1.0
        for attempt in range(1, self.max_retries + 1):
            try:
                r = self._session.get(url, params=params, timeout=90)
            except requests.RequestException:
                if attempt == self.max_retries:
                    raise
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            if r.status_code == 200:
                return r.json()
            if r.status_code == 403 and allow_forbidden:
                return None
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                sleep_s = float(ra) if ra and ra.isdigit() else backoff
                if attempt == self.max_retries:
                    raise RateLimitError(f"429 rate/quota limit on {path}")
                time.sleep(sleep_s)
                backoff = min(backoff * 2, 60)
                continue
            if 500 <= r.status_code < 600:
                if attempt == self.max_retries:
                    r.raise_for_status()
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
                continue
            raise SystemExit(f"HTTP {r.status_code} for {path}: {r.text[:300]}")
        raise SystemExit(f"Exhausted retries for {path}")


# --------------------------------------------------------------------------- #
# Universe (survivorship-free: active + delisted)
# --------------------------------------------------------------------------- #
def _paginate_tickers(client: MassiveClient, active: str) -> list[tuple]:
    rows: list[tuple] = []
    params = {"market": "stocks", "active": active, "limit": 1000,
              "order": "asc", "sort": "ticker"}
    next_url = None
    while True:
        data = client.get(next_url or "/v3/reference/tickers",
                          None if next_url else params)
        for r in data.get("results", []):
            if r.get("market") != "stocks":
                continue
            rows.append((r.get("ticker"), r.get("type"), r.get("primary_exchange")))
        next_url = data.get("next_url")
        if not next_url:
            break
    return rows


def build_universe(client: MassiveClient) -> set[str]:
    """Common stocks (TYPE_WHITELIST) on EXCHANGE_MICS, from the union of active
    and delisted reference data (so delisted names are not dropped)."""
    active_rows = _paginate_tickers(client, "true")
    delisted_rows = _paginate_tickers(client, "false")

    def cs_set(rows):
        return {tk for tk, t, e in rows
                if tk and t in TYPE_WHITELIST and e in ALLOWED_MICS}

    active_cs = cs_set(active_rows)
    delisted_cs = cs_set(delisted_rows)
    universe = active_cs | delisted_cs

    # diagnostics
    all_rows = active_rows + delisted_rows
    type_counts = Counter(t for _, t, _ in all_rows)
    print(f"\n=== Universe (survivorship-free) ===")
    print(f"  active equities pulled  : {len(active_rows)}")
    print(f"  delisted equities pulled: {len(delisted_rows)}")
    print(f"  CS on target venues (active)  : {len(active_cs)}")
    print(f"  CS on target venues (delisted): {len(delisted_cs)} "
          f"(+{len(universe) - len(active_cs)} not in active -> survivorship recovered)")
    print(f"  combined CS universe          : {len(universe)}")

    kept_rows = [(tk, e) for tk, t, e in all_rows
                 if t in TYPE_WHITELIST and e in ALLOWED_MICS]
    per_mic = Counter(e for _, e in kept_rows)
    print("  per-venue (union, with dup tickers across active/delisted):")
    for venue, mics in EXCHANGE_MICS.items():
        tot = sum(per_mic.get(m, 0) for m in mics)
        print(f"    {venue:<14} {tot}")
    print(f"  pre-filter type breakdown (union): "
          f"{dict(type_counts.most_common(8))}")
    if "CS" not in type_counts:
        print("  WARNING: no 'CS' type returned; adjust TYPE_WHITELIST.")
    return universe


# --------------------------------------------------------------------------- #
# Grouped daily bars cache  ({ticker: [close, volume]}; {} == holiday)
# --------------------------------------------------------------------------- #
def cache_path(cache_dir: Path, date: str) -> Path:
    return cache_dir / f"grouped-{date}.json.gz"


def load_cached(cache_dir: Path, date: str) -> dict[str, list] | None:
    p = cache_path(cache_dir, date)
    if not p.exists():
        return None
    try:
        with gzip.open(p, "rt", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if not data:
        return {}  # cached holiday
    # v2 format stores [close, volume]; reject legacy close-only (float) files.
    sample = next(iter(data.values()))
    if not isinstance(sample, list):
        return None  # legacy v1 -> force refetch to capture volume
    return data


def store_cache(cache_dir: Path, date: str, bars: dict[str, list]) -> None:
    p = cache_path(cache_dir, date)
    tmp = p.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        json.dump(bars, f, separators=(",", ":"))
    tmp.replace(p)


# sentinel for "date is before the plan's coverage window" (HTTP 403)
BEFORE_COVERAGE = object()


def fetch_grouped(client: MassiveClient, date: str):
    data = client.get(f"/v2/aggs/grouped/locale/us/market/stocks/{date}",
                      {"adjusted": "true"}, allow_forbidden=True)
    if data is None:
        return BEFORE_COVERAGE
    bars: dict[str, list] = {}
    for row in data.get("results", []) or []:
        t = row.get("T")
        c = row.get("c")
        v = row.get("v")
        if t and isinstance(c, (int, float)) and c > 0:
            bars[t] = [float(c), float(v) if isinstance(v, (int, float)) else 0.0]
    return bars


def candidate_dates(start: dt.date, end: dt.date):
    d, one = start, dt.timedelta(days=1)
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += one


def gather_sessions(client: MassiveClient, cache_dir: Path, start: dt.date,
                    end: dt.date, workers: int) -> dict[str, dict[str, list]]:
    """Return {date: bars} for cached/fetched weekdays. Resumable: each day is
    cached on success, so a re-run after a rate-limit skips completed days."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    result: dict[str, dict[str, list]] = {}
    to_fetch: list[str] = []
    for d in candidate_dates(start, end):
        ds = d.isoformat()
        cached = load_cached(cache_dir, ds)
        if cached is not None:
            result[ds] = cached
        else:
            to_fetch.append(ds)

    total_target = len(result) + len(to_fetch)
    if not to_fetch:
        print(f"All {len(result)} weekdays served from cache.")
        write_checkpoint(cache_dir, total_target, len(result), 0, end, complete=True)
        return result

    print(f"Backfill: {len(to_fetch)} weekdays to fetch "
          f"(cache hits: {len(result)}; target {total_target}).")
    done = 0
    rate_limited = False

    def task(ds: str):
        bars = fetch_grouped(client, ds)
        if bars is BEFORE_COVERAGE:
            return ds, None  # pre-coverage: skip, don't cache
        store_cache(cache_dir, ds, bars)
        return ds, bars

    try:
        with cf.ThreadPoolExecutor(max_workers=workers) as ex:
            for ds, bars in ex.map(task, to_fetch):
                if bars is not None:
                    result[ds] = bars
                done += 1
                if done % 50 == 0 or done == len(to_fetch):
                    print(f"  ...{done}/{len(to_fetch)} fetched")
                    write_checkpoint(cache_dir, total_target, len(result),
                                     len(to_fetch) - done, end, complete=False)
    except RateLimitError as e:
        rate_limited = True
        print(f"\nRATE/QUOTA LIMIT: {e}")

    write_checkpoint(cache_dir, total_target, len(result),
                     len(to_fetch) - done, end, complete=not rate_limited)
    if rate_limited:
        raise SystemExit(
            "Backfill paused by rate/quota limit. Progress is cached; "
            "re-run to resume (completed days are skipped). Output not rewritten."
        )
    return result


def write_checkpoint(cache_dir: Path, target: int, completed: int,
                     remaining: int, end: dt.date, complete: bool) -> None:
    cp = cache_dir / "checkpoint.json"
    cp.write_text(json.dumps({
        "target_weekdays": target,
        "completed_weekdays": completed,
        "remaining_weekdays": remaining,
        "through_date": end.isoformat(),
        "complete": complete,
    }, indent=2), encoding="utf-8")


# --------------------------------------------------------------------------- #
# Metric
# --------------------------------------------------------------------------- #
class Cfg:
    def __init__(self, move_lookback, move_threshold, price_floor,
                 vol_lookback, min_volume):
        self.move_lookback = move_lookback
        self.move_factor = 1.0 + move_threshold
        self.price_floor = price_floor
        self.vol_lookback = vol_lookback
        self.min_volume = min_volume


def count_day(i, days, sessions, universe, cfg) -> tuple[int, int, int]:
    """Return (pre, after_price, after_volume) counts for trading_days[i].
    pre = +20% move only (old qualifier); after_volume = full Stockbee count."""
    now = sessions[days[i]]
    past = sessions[days[i - cfg.move_lookback]]
    vol_days = [sessions[days[i - k]] for k in range(1, cfg.vol_lookback + 1)]
    pre = after_price = after_vol = 0
    for sym, rec in now.items():
        if sym not in universe:
            continue
        c_now = rec[0]
        p = past.get(sym)
        if p is None or p[0] <= 0:
            continue
        if c_now < p[0] * cfg.move_factor:        # condition 1: +20% over 5 sessions
            continue
        pre += 1
        if c_now < cfg.price_floor:               # condition 2: >= $5
            continue
        after_price += 1
        vmin = None                               # condition 3: min vol(D-1..D-3) > 100k
        ok = True
        for vd in vol_days:
            r = vd.get(sym)
            if r is None:
                ok = False
                break
            vmin = r[1] if vmin is None else min(vmin, r[1])
        if not ok or vmin is None or vmin <= cfg.min_volume:
            continue
        after_vol += 1
    return pre, after_price, after_vol


def compute_series(sessions, universe, cfg):
    days = sorted(d for d, bars in sessions.items() if bars)
    start_i = max(cfg.move_lookback, cfg.vol_lookback)
    out = []
    for i in range(start_i, len(days)):
        _, _, post = count_day(i, days, sessions, universe, cfg)
        out.append({"date": days[i], "value": post})
    return out, days


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser(description="Stockbee US 5-day up-20% breadth series.")
    ap.add_argument("--out", default="breadth_5d_up20.json")
    ap.add_argument("--cache-dir", default="cache")
    ap.add_argument("--start-date", default=START_DATE)
    ap.add_argument("--lookback", type=int, default=MOVE_LOOKBACK)
    ap.add_argument("--threshold", type=float, default=MOVE_THRESHOLD)
    ap.add_argument("--price-floor", type=float, default=PRICE_FLOOR)
    ap.add_argument("--vol-lookback", type=int, default=VOL_LOOKBACK)
    ap.add_argument("--min-volume", type=float, default=MIN_VOLUME)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--tc2000", metavar="YYYY-MM-DD=VALUE",
                    help="known TC2000/Stockbee value to compare for tuning")
    args = ap.parse_args()

    cfg = Cfg(args.lookback, args.threshold, args.price_floor,
              args.vol_lookback, args.min_volume)
    client = MassiveClient(os.environ.get("MASSIVE_API_KEY", ""))

    now_et = dt.datetime.now(ET)
    today_et = now_et.date()
    end = today_et if now_et.hour >= SESSION_FINAL_HOUR_ET else today_et - dt.timedelta(days=1)

    series_start = dt.date.fromisoformat(args.start_date)
    # buffer so the earliest emitted date has its lookback prior sessions
    fetch_start = series_start - dt.timedelta(days=20)

    universe = build_universe(client)
    sessions = gather_sessions(client, Path(args.cache_dir), fetch_start, end, args.workers)
    series, days = compute_series(sessions, universe, cfg)
    series = [pt for pt in series if pt["date"] >= series_start.isoformat()]
    series.sort(key=lambda p: p["date"])

    Path(args.out).write_text(json.dumps(series, indent=2) + "\n", encoding="utf-8")

    # ---------------- validation report ----------------
    print(f"\nWrote {len(series)} points to {args.out}")
    if not series:
        print("No points produced."); return 1
    vals = [p["value"] for p in series]
    print(f"Range: {series[0]['date']} .. {series[-1]['date']}")
    print(f"Stats: count={len(vals)} min={min(vals)} mean={sum(vals)/len(vals):.1f} "
          f"median={statistics.median(vals):.1f} max={max(vals)}")
    print("\nLast 10 values:")
    for pt in series[-10:]:
        print(f"  {pt['date']}  {pt['value']}")

    # pre vs post filter on sample dates (how many $5 + volume floors remove)
    date_to_i = {d: i for i, d in enumerate(days)}
    n = len(series)
    sample = [series[-1]["date"], series[-2]["date"],
              series[n // 2]["date"], series[n // 4]["date"]]
    print("\nPre/post-filter breakdown (move-only -> +$5 floor -> +vol floor = value):")
    for ds in sample:
        i = date_to_i[ds]
        pre, ap_, post = count_day(i, days, sessions, universe, cfg)
        print(f"  {ds}: move>=20%={pre:<5} after $>={int(cfg.price_floor)}={ap_:<5} "
              f"after minVol>{int(cfg.min_volume)}={post}")

    # Judge by the typical level, not the single extreme: a 10-year window
    # includes the 2020 COVID rebound, a real breadth thrust that legitimately
    # spikes into the hundreds. The filters are wrong only if the TYPICAL
    # reading is high.
    srt = sorted(vals)
    med = statistics.median(vals)
    p95 = srt[min(len(srt) - 1, int(len(srt) * 0.95))]
    print(f"\nMagnitude check: median={med:.0f} p95={p95} max={max(vals)} "
          f"(share<=60: {100*sum(v<=60 for v in vals)/len(vals):.0f}%)")
    print("  " + ("OK: typical readings are tens (Stockbee range); extremes are "
                  "real thrusts (e.g. the 2020 COVID rebound)."
                  if med <= 80 else
                  "WARNING: typical readings too high; check the $5/volume filters."))

    if args.tc2000:
        ds, _, vs = args.tc2000.partition("=")
        got = next((p["value"] for p in series if p["date"] == ds), None)
        if got is None:
            print(f"\nTC2000 check: {ds} not in series.")
        else:
            want = int(vs)
            print(f"\nTC2000 check {ds}: ours={got} tc2000={want} diff={got - want:+d}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
