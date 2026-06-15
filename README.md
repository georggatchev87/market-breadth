# US 5-Day Up-20% Market Breadth (Stockbee)

Replicates Pradeep Bonde's Stockbee **"US Comm Stks 5 Day up 20%"** breadth
indicator (the TC2000 scan) as a daily series.

For each US trading day **D**, it counts common stocks listed on the configured
venues where **all three** conditions hold:

1. `close(D) / close(D − 5 trading sessions) − 1 ≥ 0.20`  (5-session +20% move)
2. `close(D) ≥ 5`  ($5 price floor)
3. `min( vol(D−1), vol(D−2), vol(D−3) ) > 100000`  (TC2000 `minv3.1`; the three
   sessions *before* D, excluding D)

Output: [`breadth_5d_up20.json`](breadth_5d_up20.json) — an array sorted ascending:

```json
[{"date": "2016-06-24", "value": 12}, ...]
```

## How it works

- **Data source:** [Massive.com](https://massive.com) REST API (Polygon-compatible).
- **Bars:** `GET /v2/aggs/grouped/locale/us/market/stocks/{date}` with
  `adjusted=true` — split/dividend-adjusted OHLCV, so neither the % move nor the
  volume test is distorted by splits. One call per trading day, cached on disk.
- **Universe (survivorship-free):** `GET /v3/reference/tickers` pulled for both
  `active=true` and `active=false`, so **delisted** names are classified too.
  Kept if `type == "CS"` (common stock) and `primary_exchange` is one of the
  configured venues. Per-day candidates come from the symbols actually present
  in that day's bars, so delisted names are counted on the days they traded.
- **Trading calendar:** derived empirically — a date is a session iff the
  grouped endpoint returns rows. The D−5 and D−1..D−3 offsets use this real
  trading-day sequence, not calendar days.
- **History:** backfills to the earliest date the subscription serves
  (a rolling ~10-year window; pre-coverage dates return HTTP 403 and are
  skipped). The backfill is **resumable** — every day is cached on success
  (cache *is* the checkpoint, plus `cache/checkpoint.json`), so a run halted by
  a rate/quota limit continues on the next run without re-pulling.
- **EOD only:** the still-forming current day is never emitted (a session is
  treated as final after 20:00 ET).

## Configuration (top of `breadth.py`)

```python
EXCHANGE_MICS   # venue label -> primary_exchange MIC set (NYSE/NASDAQ/AMEX/Arca/Cboe)
TYPE_WHITELIST  # {"CS"}  -> common stock only
MOVE_LOOKBACK   = 5        # sessions for the % move
MOVE_THRESHOLD  = 0.20     # +20%
PRICE_FLOOR     = 5.0      # $5
VOL_LOOKBACK    = 3        # D-1..D-3
MIN_VOLUME      = 100_000  # min volume over that window must be strictly greater
START_DATE      = "2016-06-17"  # earliest covered date (probed)
```

All are also overridable via CLI flags (`--threshold`, `--price-floor`,
`--min-volume`, `--start-date`, `--tc2000 YYYY-MM-DD=VALUE`, ...).

## Running locally

```bash
pip install -r requirements.txt   # on Windows behind TLS inspection also: pip-system-certs
export MASSIVE_API_KEY=...          # never commit this
python breadth.py                   # full ~10y backfill on first run; incremental after
```

The grouped-daily cache (`cache/`) is git-ignored and rebuilt from the API;
historical bars are immutable, so later runs only fetch new sessions.

## Publishing

`.github/workflows/publish.yml` runs after the US close (21:15 UTC, Mon–Fri)
plus manual dispatch. It reads `MASSIVE_API_KEY` from a repo secret, regenerates
the series, and commits the JSON if it changed. The cache is persisted between
runs via `actions/cache`.

### Setup
1. Add the secret: **Settings → Secrets and variables → Actions → `MASSIVE_API_KEY`**.
2. Actions needs write permission (declared via `permissions: contents: write`).

## Notes

- **Magnitude:** typically tens (median ≈ 24, ~84% of days ≤ 60). Real breadth
  thrusts spike higher — the 2020 COVID rebound reached ~960; 2017 (calm year)
  peaked at 35.
- **Survivorship:** delisted-inclusive classification removes the main bias.
  Classification uses each symbol's reference attributes (type/exchange), which
  are stable over time; rare as-of-date attribute changes or ticker reuse are
  not tracked.
