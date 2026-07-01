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

Also produces the bearish mirror "US Comm Stks 5 Day Down 20%" in the same pass
(same universe/bars/calendar), using close(D)/close(D-5) < 0.80.

Output (both sorted ascending, schema [{"date":"YYYY-MM-DD","value":<int>}]):
  - breadth_5d_up20.json     (close(D)/close(D-5) >= 1.20)
  - breadth_5d_down20.json   (close(D)/close(D-5) <  0.80)

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
# Stockbee "US Comm Stks 5 Day up 20%" (and the bearish mirror). Tune here.
MOVE_LOOKBACK = 5         # trading sessions for the % move (D vs D-5)
MOVE_THRESHOLD = 0.20     # UP series:   close(D)/close(D-5) - 1 >= this  (>= 1.20x)
DOWN_THRESHOLD = 0.20     # DOWN series: close(D)/close(D-5) - 1 <  -this (<  0.80x)
PRICE_FLOOR = 5.0         # close(D) >= this ($)
VOL_LOOKBACK = 3          # use volumes of D-1 .. D-VOL_LOOKBACK
MIN_VOLUME = 100_000      # min(volume over that window) must be STRICTLY > this

# ============================== HISTORY ==================================== #
# Earliest date the subscription serves grouped data (probed 2026-06: the plan
# is a rolling ~10y window; 2016-06-17 was the earliest 200, 2016-06-16 -> 403).
# Pre-coverage dates return HTTP 403 and are skipped gracefully.
START_DATE = "2016-06-17"

# A trading session is treated as final (EOD data settled) only after this ET
# hour, so the still-forming current day is never emitted. 18:00 ET = 2 hours
# after the 16:00 close, which is enough for Massive's EOD grouped data to be
# available. The scheduled "after close" run is timed to fire after this hour;
# the early-morning safety-net run sees the prior day as already-final.
SESSION_FINAL_HOUR_ET = 18

# ---------------------- PRE-CLOSE PREVIEW CONFIG --------------------------- #
# The optional "preview" mode writes a PROVISIONAL value for today ~1h before the
# close, using each stock's current intraday price (from the full-market
# snapshot) as today's price. Everything else (close(D-5), the D-1..D-3 volumes,
# the universe, the qualifier) is identical to the finalizer -- it reuses the
# very same count_day(), so the two runs cannot drift.
SNAPSHOT_PATH = "/v2/snapshot/locale/us/markets/stocks/tickers"  # all US tickers, 1 call
MARKETSTATUS_NOW = "/v1/marketstatus/now"
MARKETSTATUS_UPCOMING = "/v1/marketstatus/upcoming"
REGULAR_CLOSE_ET = dt.time(16, 0)     # normal US equities close (4:00pm ET)
# Valid preview window: only compute+write when "now" is within this many
# minutes before today's ACTUAL close (half-day aware). Targets ~1h before,
# widened to tolerate GitHub's cron drift; outside it the run exits quietly.
PREVIEW_WINDOW_MIN = 30
PREVIEW_WINDOW_MAX = 90
STATUS_FILE = "status.json"


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
                try:
                    return r.json()
                except ValueError:
                    # Truncated/corrupt body (transient during large pulls) ->
                    # retry with backoff rather than crashing the whole run.
                    if attempt == self.max_retries:
                        raise
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 30)
                    continue
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
    def __init__(self, move_lookback, move_threshold, down_threshold,
                 price_floor, vol_lookback, min_volume):
        self.move_lookback = move_lookback
        self.move_factor = 1.0 + move_threshold   # UP:   ratio >= this
        self.down_factor = 1.0 - down_threshold   # DOWN: ratio <  this
        self.price_floor = price_floor
        self.vol_lookback = vol_lookback
        self.min_volume = min_volume


def count_day(i, days, sessions, universe, cfg) -> dict:
    """Count up- and down-qualifiers for trading_days[i] in a single pass over
    the day's bars. The $5 price floor and the min-volume floor are shared; only
    the 5-session move condition differs (>= 1.20x up vs < 0.80x down).

    Returns {"up", "down", "up_pre", "up_price"} where up_pre/up_price are the
    move-only and move+price intermediate counts (for the up breakdown report)."""
    now = sessions[days[i]]
    past = sessions[days[i - cfg.move_lookback]]
    vol_days = [sessions[days[i - k]] for k in range(1, cfg.vol_lookback + 1)]
    up = down = up_pre = up_price = 0
    for sym, rec in now.items():
        if sym not in universe:
            continue
        c_now = rec[0]
        p = past.get(sym)
        if p is None or p[0] <= 0:
            continue
        c_past = p[0]                                 # condition 1: the 5-session move.
        is_up = c_now >= c_past * cfg.move_factor     #   up:   >= 1.20x (multiplication
        is_down = c_now < c_past * cfg.down_factor    #   down: <  0.80x  form, matches the
        #                                               original up series exactly)
        if is_up:
            up_pre += 1
        if not (is_up or is_down):
            continue
        if c_now < cfg.price_floor:                   # condition 2: close >= $5
            continue
        if is_up:
            up_price += 1
        vmin = None                                   # condition 3: min vol(D-1..D-3) > 100k
        ok = True
        for vd in vol_days:
            r = vd.get(sym)
            if r is None:
                ok = False
                break
            vmin = r[1] if vmin is None else min(vmin, r[1])
        if not ok or vmin is None or vmin <= cfg.min_volume:
            continue
        if is_up:
            up += 1
        else:
            down += 1
    return {"up": up, "down": down, "up_pre": up_pre, "up_price": up_price}


def compute_series(sessions, universe, cfg):
    """Compute both the up and down series in one pass over the cached bars.
    Returns (up_series, down_series, days)."""
    days = sorted(d for d, bars in sessions.items() if bars)
    start_i = max(cfg.move_lookback, cfg.vol_lookback)
    up_out, down_out = [], []
    for i in range(start_i, len(days)):
        c = count_day(i, days, sessions, universe, cfg)
        up_out.append({"date": days[i], "value": c["up"]})
        down_out.append({"date": days[i], "value": c["down"]})
    return up_out, down_out, days


# --------------------------------------------------------------------------- #
# Pre-close preview (provisional "today so far") + status tracking
# --------------------------------------------------------------------------- #
def fetch_snapshot(client: MassiveClient) -> dict[str, float]:
    """Current intraday price per ticker from the full-market snapshot (one call).
    Price preference: last trade -> last minute close -> day close-so-far."""
    data = client.get(SNAPSHOT_PATH, {"include_otc": "false"})
    out: dict[str, float] = {}
    for t in data.get("tickers") or []:
        sym = t.get("ticker")
        if not sym:
            continue
        price = None
        for obj, field in (("lastTrade", "p"), ("min", "c"), ("day", "c")):
            v = (t.get(obj) or {}).get(field)
            if isinstance(v, (int, float)) and v > 0:
                price = float(v)
                break
        if price is not None:
            out[sym] = price
    return out


def _parse_et(iso: str) -> dt.datetime:
    return dt.datetime.fromisoformat(iso).astimezone(ET)


def _today_close_et(client: MassiveClient, today: dt.date) -> dt.datetime:
    """Today's actual close in ET (16:00 normally; 13:00-ish on early-close
    half-days, read from the calendar). Defaults to the regular close."""
    close = dt.datetime.combine(today, REGULAR_CLOSE_ET, tzinfo=ET)
    try:
        data = client.get(MARKETSTATUS_UPCOMING)
    except Exception:
        return close
    rows = data if isinstance(data, list) else (data.get("results") or [])
    for e in rows:
        if (e.get("date") == today.isoformat()
                and str(e.get("status", "")).lower() == "early-close"
                and e.get("exchange") in ("NYSE", "NASDAQ")):
            raw = e.get("close")
            if raw:
                try:
                    return _parse_et(str(raw).replace("Z", "+00:00"))
                except ValueError:
                    pass
    return close


def preview_gate(client: MassiveClient):
    """Return (close_et, now_et, today, reason). close_et is None when NOT a
    valid preview moment (market not open, or outside the pre-close window)."""
    ns = client.get(MARKETSTATUS_NOW)
    market = str(ns.get("market") or "").lower()
    server = ns.get("serverTime")
    now_et = _parse_et(server) if server else dt.datetime.now(ET)
    today = now_et.date()
    if market != "open":
        return None, now_et, today, f"market is '{market or 'unknown'}' (not a regular open session)"
    close_et = _today_close_et(client, today)
    mins = (close_et - now_et).total_seconds() / 60.0
    if not (PREVIEW_WINDOW_MIN <= mins <= PREVIEW_WINDOW_MAX):
        return None, now_et, today, (f"{mins:.0f} min before close {close_et:%H:%M} ET "
                                     f"(need {PREVIEW_WINDOW_MIN}-{PREVIEW_WINDOW_MAX})")
    return close_et, now_et, today, f"OK: {mins:.0f} min before close {close_et:%H:%M} ET"


def load_status(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def write_status(path: str, date_str: str, state: str,
                 up_value=None, down_value=None) -> dict:
    obj = {
        "date": date_str,
        "state": state,  # "provisional" (pre-close preview) or "final" (settled)
        "updated_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if up_value is not None:
        obj["up_value"] = up_value
    if down_value is not None:
        obj["down_value"] = down_value
    Path(path).write_text(json.dumps(obj, indent=2) + "\n", encoding="utf-8")
    return obj


def upsert_row(path: str, date_str: str, value: int) -> None:
    """Set/replace ONLY today's {date,value} row; never touch any other date."""
    p = Path(path)
    arr = json.loads(p.read_text(encoding="utf-8")) if p.exists() else []
    m = {x["date"]: x["value"] for x in arr}
    m[date_str] = value
    out = [{"date": d, "value": m[d]} for d in sorted(m)]
    p.write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")


def run_preview(args, cfg, client) -> int:
    """Compute and (only inside a valid window) write today's PROVISIONAL value,
    reusing the exact finalizer math. Only today's row is ever touched."""
    if args.force_preview:
        now_et = dt.datetime.now(ET)
        today = now_et.date()
        close_et = None
        print("FORCED preview: time gate bypassed (testing only; will not commit).")
    else:
        close_et, now_et, today, reason = preview_gate(client)
        print(f"Preview gate: {reason}")
        if close_et is None:
            print("Not a valid preview window -> exiting quietly without writing.")
            return 0
    today_iso = today.isoformat()

    # Safety: never overwrite a value already marked FINAL for today (e.g. if the
    # preview fires late, after the finalizer already settled today).
    st = load_status(args.status_out)
    if st.get("date") == today_iso and st.get("state") == "final":
        print(f"{today_iso} is already FINAL in {args.status_out}; preview will not overwrite.")
        return 0

    # Settled bars through the PRIOR session (self-healing; reuses the cache).
    series_start = dt.date.fromisoformat(args.start_date)
    fetch_start = series_start - dt.timedelta(days=20)
    universe = build_universe(client)
    settled = gather_sessions(client, Path(args.cache_dir), fetch_start,
                              today - dt.timedelta(days=1), args.workers)
    settled.pop(today_iso, None)  # today is NOT settled

    # Live snapshot -> synthetic "today" session (price only; today's volume is
    # never used -- the volume floor uses the settled D-1..D-3 bars).
    snap = fetch_snapshot(client)
    today_bars = {sym: [price, 0.0] for sym, price in snap.items() if price > 0}
    if not today_bars:
        raise SystemExit("Snapshot returned no usable intraday prices; aborting preview.")

    sessions = dict(settled)
    sessions[today_iso] = today_bars
    days = sorted(d for d, bars in sessions.items() if bars)
    i = days.index(today_iso)
    need = max(cfg.move_lookback, cfg.vol_lookback)
    if i < need:
        raise SystemExit(f"Not enough settled history before {today_iso} "
                         f"(have {i}, need {need}).")

    # THE SAME count_day used by the finalizer -- no drift possible.
    res = count_day(i, days, sessions, universe, cfg)
    up_val, down_val = res["up"], res["down"]

    print(f"\nPROVISIONAL (today so far, {today_iso}, snapshot @ current prices):")
    print(f"  UP   (5 Day up 20%)   = {up_val}")
    print(f"  DOWN (5 Day down 20%) = {down_val}")

    if args.dry_run or args.force_preview:
        why = "dry-run" if args.dry_run else "forced/outside a confirmed window"
        print(f"\n{why.upper()}: printed only -- no files written, no history changed.")
        return 0

    upsert_row(args.out, today_iso, up_val)
    upsert_row(args.down_out, today_iso, down_val)
    write_status(args.status_out, today_iso, "provisional", up_val, down_val)
    print(f"\nWrote PROVISIONAL {today_iso}: {args.out}, {args.down_out}; status -> provisional.")
    return 0


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser(description="Stockbee US 5-day up-20% breadth series.")
    ap.add_argument("--out", default="breadth_5d_up20.json", help="up-series output")
    ap.add_argument("--down-out", default="breadth_5d_down20.json",
                    help="down-series output")
    ap.add_argument("--cache-dir", default="cache")
    ap.add_argument("--start-date", default=START_DATE)
    ap.add_argument("--lookback", type=int, default=MOVE_LOOKBACK)
    ap.add_argument("--threshold", type=float, default=MOVE_THRESHOLD)
    ap.add_argument("--down-threshold", type=float, default=DOWN_THRESHOLD)
    ap.add_argument("--price-floor", type=float, default=PRICE_FLOOR)
    ap.add_argument("--vol-lookback", type=int, default=VOL_LOOKBACK)
    ap.add_argument("--min-volume", type=float, default=MIN_VOLUME)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--tc2000", metavar="YYYY-MM-DD=VALUE",
                    help="known TC2000/Stockbee value to compare for tuning")
    ap.add_argument("--mode", choices=("finalize", "preview"), default="finalize",
                    help="finalize = settled after-close run (default); "
                         "preview = provisional pre-close 'today so far' run")
    ap.add_argument("--status-out", default=STATUS_FILE,
                    help="provisional/final tracking file")
    ap.add_argument("--force-preview", action="store_true",
                    help="preview mode: bypass the time gate (testing; never commits)")
    ap.add_argument("--dry-run", action="store_true",
                    help="compute and print only; write nothing")
    args = ap.parse_args()

    cfg = Cfg(args.lookback, args.threshold, args.down_threshold,
              args.price_floor, args.vol_lookback, args.min_volume)
    client = MassiveClient(os.environ.get("MASSIVE_API_KEY", ""))

    if args.mode == "preview":
        return run_preview(args, cfg, client)

    now_et = dt.datetime.now(ET)
    today_et = now_et.date()
    end = today_et if now_et.hour >= SESSION_FINAL_HOUR_ET else today_et - dt.timedelta(days=1)

    series_start = dt.date.fromisoformat(args.start_date)
    # buffer so the earliest emitted date has its lookback prior sessions
    fetch_start = series_start - dt.timedelta(days=20)

    universe = build_universe(client)
    sessions = gather_sessions(client, Path(args.cache_dir), fetch_start, end, args.workers)
    up_series, down_series, days = compute_series(sessions, universe, cfg)

    def trim(s):
        s = [pt for pt in s if pt["date"] >= series_start.isoformat()]
        s.sort(key=lambda p: p["date"])
        return s

    up_series = trim(up_series)
    down_series = trim(down_series)

    if not up_series:
        print("No points produced."); return 1

    # Self-healing + history-preserving merge. The series is rebuilt from all
    # cached bars up to the most recent FINAL session Massive has, so missing
    # days (e.g. after a skipped scheduled run) are appended automatically --
    # never fabricated, since a date only appears if grouped-daily returned rows.
    #
    # We MERGE with the already-published file rather than overwriting: newly
    # computed dates win for their range, but any earlier dates already on disk
    # are kept. This keeps the 10-year start sticky -- the API serves a rolling
    # ~10y window, so the oldest day eventually returns 403 and would otherwise
    # drop off the front of a fresh rebuild. We only ever append/refresh.
    def load_existing(path):
        p = Path(path)
        if not p.exists():
            return {}
        try:
            return {x["date"]: x["value"]
                    for x in json.loads(p.read_text(encoding="utf-8"))}
        except (json.JSONDecodeError, OSError, KeyError, TypeError):
            return {}

    def merge_write(path, computed):
        existing = load_existing(path)
        prev_last = max(existing) if existing else None
        merged = dict(existing)                 # preserve all prior history
        for pt in computed:                     # computed wins for its range
            merged[pt["date"]] = pt["value"]
        out = [{"date": d, "value": merged[d]} for d in sorted(merged)]
        Path(path).write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")
        return out, prev_last

    up_series, prev_last = merge_write(args.out, up_series)
    down_series, _ = merge_write(args.down_out, down_series)

    new_last = up_series[-1]["date"]
    appended = [p["date"] for p in up_series if prev_last is None or p["date"] > prev_last]
    print(f"\nSelf-heal: previous last date={prev_last or 'n/a'} -> new last date={new_last} "
          f"({len(appended)} day(s) appended{': ' + ', '.join(appended) if appended else ''}).")

    # The finalizer's value is the settled, authoritative close: mark it FINAL so
    # a late-firing preview never overwrites it.
    write_status(args.status_out, new_last, "final",
                 up_series[-1]["value"], down_series[-1]["value"])
    print(f"status -> final ({new_last}).")

    def stats(series, name):
        vals = [p["value"] for p in series]
        srt = sorted(vals)
        med = statistics.median(vals)
        p95 = srt[min(len(srt) - 1, int(len(srt) * 0.95))]
        print(f"\n=== {name} ({len(series)} pts, "
              f"{series[0]['date']} .. {series[-1]['date']}) ===")
        print(f"Stats: count={len(vals)} min={min(vals)} mean={sum(vals)/len(vals):.1f} "
              f"median={med:.1f} p95={p95} max={max(vals)}")
        print("Last 10:")
        for pt in series[-10:]:
            print(f"  {pt['date']}  {pt['value']}")
        return vals

    print(f"\nWrote {len(up_series)} pts -> {args.out}")
    print(f"Wrote {len(down_series)} pts -> {args.down_out}")
    assert len(up_series) == len(down_series), "up/down length mismatch"
    assert [p["date"] for p in up_series] == [p["date"] for p in down_series], \
        "up/down date mismatch"

    up_vals = stats(up_series, "UP   (5 Day up 20%)")

    # up pre/post-filter breakdown on sample dates (kept from before)
    date_to_i = {d: i for i, d in enumerate(days)}
    n = len(up_series)
    sample = [up_series[-1]["date"], up_series[n // 2]["date"], up_series[n // 4]["date"]]
    print("\nUP pre/post-filter (move-only -> +$5 -> +vol = value):")
    for ds in sample:
        c = count_day(date_to_i[ds], days, sessions, universe, cfg)
        print(f"  {ds}: move>=20%={c['up_pre']:<5} after $>={int(cfg.price_floor)}="
              f"{c['up_price']:<5} after minVol>{int(cfg.min_volume)}={c['up']}")

    down_vals = stats(down_series, "DOWN (5 Day down 20%)")

    # down-series sanity: largest readings should land on known sell-offs.
    top = sorted(down_series, key=lambda p: -p["value"])[:5]
    print("\nDOWN top-5 readings (should cluster on real sell-offs):")
    for pt in top:
        print(f"  {pt['date']}  {pt['value']}")
    med_d = statistics.median(down_vals)
    print(f"\nDown magnitude: median={med_d:.0f} max={max(down_vals)} "
          + ("OK: near zero in calm uptrends, spikes on declines."
             if med_d <= 80 else "WARNING: typical too high; check filters."))

    if args.tc2000:
        ds, _, vs = args.tc2000.partition("=")
        got = next((p["value"] for p in up_series if p["date"] == ds), None)
        if got is None:
            print(f"\nTC2000 check: {ds} not in series.")
        else:
            want = int(vs)
            print(f"\nTC2000 check {ds}: ours={got} tc2000={want} diff={got - want:+d}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
