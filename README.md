# US 5-Day Up-20% Market Breadth

Replicates TC2000's **"US Comm Stks 5 Day up 20%"** indicator as a daily series.

For each US trading day **D**, it counts NYSE/NASDAQ common stocks whose
`close(D) / close(D − 5 trading sessions) − 1 ≥ 0.20` (i.e. up at least 20%
over the prior 5 trading sessions).

Output: [`breadth_5d_up20.json`](breadth_5d_up20.json) — an array sorted ascending:

```json
[{"date": "2024-06-14", "value": 63}, ...]
```

## How it works

- **Data source:** [Massive.com](https://massive.com) REST API (Polygon-compatible).
- **Universe:** `GET /v3/reference/tickers` with `market=stocks`, `active=true`,
  filtered to primary exchange MIC `XNYS` (NYSE) or `XNAS` (NASDAQ — all tiers
  collapse to this; `XNGS/XNMS/XNCM` also accepted defensively). NYSE American
  (`XASE`), Arca (`ARCX`), Cboe (`BATS`), IEX, and OTC are excluded.
  With `COMMON_STOCK_ONLY=true` (the default for publishing), the universe is
  further restricted to `type == "CS"`.
- **Daily bars:** `GET /v2/aggs/grouped/locale/us/market/stocks/{date}`
  (`adjusted=true`) — one call per trading day, cached on disk under `cache/`.
- **Trading calendar:** derived empirically — a date is a trading day iff the
  grouped endpoint returns rows for it. The 5-session offset uses this actual
  trading-day sequence, not calendar days.
- **EOD only:** the still-forming current day is never emitted (a session is
  treated as final after 20:00 ET).

## Running locally

```bash
pip install -r requirements.txt
export MASSIVE_API_KEY=...        # never commit this
python breadth.py                  # backfills ~2 years on first run
python breadth.py --common-stock-only   # "Comm Stks" replication (recommended)
```

Useful flags: `--backfill-years`, `--threshold` (default `0.20`),
`--lookback` (default `5`), `--common-stock-only`, `--workers`,
`--tc2000 YYYY-MM-DD=VALUE` (compare one known TC2000 value for tuning).

The grouped-daily cache (`cache/`) is git-ignored and rebuilt from the API;
historical days never change, so subsequent runs only fetch new trading days.

## Publishing

`.github/workflows/publish.yml` runs after the US close (21:15 UTC, Mon–Fri)
and on manual dispatch. It reads the API key from the repo secret
`MASSIVE_API_KEY`, regenerates the series, and commits the JSON if it changed.
The grouped-daily cache is persisted between runs via `actions/cache`.

### Setup

1. Create the repo on GitHub and push this directory.
2. Add the secret: **Settings → Secrets and variables → Actions →
   `MASSIVE_API_KEY`**.
3. Ensure Actions has write permission (Settings → Actions → General →
   Workflow permissions → *Read and write*). The workflow also declares
   `permissions: contents: write`.

## Notes / caveats

- The universe is the **currently active** NYSE/NASDAQ list applied across
  history, so names delisted since are not counted on historical dates
  (mild survivorship bias). `/v3/reference/tickers` supports a point-in-time
  `date` param if exact historical membership is later required.
- Closes are split-adjusted (`adjusted=true`), so 5-session returns are not
  distorted by splits.
