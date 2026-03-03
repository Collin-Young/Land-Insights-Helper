## Land Insights automation helper

This repo contains a fully automated Playwright workflow that logs into https://app.landinsights.co/, applies county + acreage filters, purchases parcel exports, downloads the CSVs, and iterates across as many counties as you need (single run, Texas-wide, or nationwide lists).

### Quick start

1. Install dependencies and browsers (already done once, repeat if needed):
   ```bash
   npm install
   npx playwright install chromium
   ```
2. Create a `.env` file (never commit it) with your unlimited account credentials and default filter inputs:
   ```
   LANDINSIGHTS_EMAIL=aaron@fulleroak.com
   LANDINSIGHTS_PASSWORD=DdILked5ibid9OBThrur
   LANDINSIGHTS_COUNTY=Cleveland County, OK
   LANDINSIGHTS_ACRES_FROM=0
   LANDINSIGHTS_ACRES_TO=10000
   ```
3. Run the automation for the county defined in `.env`:
   ```bash
   npm run login:trace
   ```
   - The browser opens in headed mode so you can monitor progress (add `--headless` to keep it hidden).
   - When exports finish, the CSV files appear under `artifacts/downloads/` with descriptive names—no HAR/trace files are produced anymore.
4. Optional flags:
   - `npm run login:trace -- --auto --hold-ms=30000 --headless` keeps the session for 30 s then exits in headless mode.
   - `--slow=0` removes the default 50 ms action delay.

### Sample multiple counties

- `npm run counties:sample` launches `scripts/batchCounties.mjs`, which picks three random counties from `data/texas-counties.json` and runs `loginTrace` once per county (reusing your existing `.env` credentials).
- Adjustments:
  - `--count=5` changes how many unique counties are sampled (defaults to 3).
  - `--all` iterates through every county in the specified state list (useful for running the entire state of Texas end-to-end).
  - `--keep-going` continues to the next county even if a prior run fails.
  - `--county-file=relative/path.json` overrides the source list (falls back to `data/<state>-counties.json`, where `<state>` defaults to `texas`).
  - Any other flags (for example `--auto --headless --hold-ms=5000`) are passed through to `loginTrace`.
- Example: `npm run counties:sample -- --count=3 --keep-going --auto --hold-ms=15000`.

### Download outputs

- Each automated export clicks through the Purchase and Download dialogs, captures the CSV, and stores it under `artifacts/downloads/`.
- Files are renamed to `county-state-batch#-acreStart-acreEnd-parcelCount.csv`, for example `ector-county-tx-batch-1-0-10000-87450.csv`.
- Existing files are preserved; duplicates receive a numeric suffix.
- HAR/trace artifacts are no longer generated—only the CSV exports remain.

### Running full states or the entire USA

- To sweep all of Texas, run `npm run counties:sample -- --all --state="Texas" --keep-going --auto --headless`.
- For other states or nationwide runs, drop a JSON array of `["County Name, ST", ...]` into `data/` (for example `data/us-counties.json`) and reference it via `--county-file=data/us-counties.json --all --keep-going`.
- Long-running batches can be resumed by rerunning the command; completed counties will still be reprocessed unless you comment them out of the JSON list.

### Next steps

- Re-run the script while performing the exact export workflow (county selection, filters, download). The resulting HAR/trace will reveal the JSON payloads and headers needed for fully headless automation.
- Promote the login flow into a Prefect/Temporal task once the endpoints are mapped, then add job orchestration and storage described in our automation plan.
- Override the filter environment variables (county + acreage range) per run or inject them via a higher-level orchestrator to sweep through all counties.
