# 2026-08-12 — verification pass

## Live-data verification (cross-repo confusion resolved)

The `financial-data-pipeline\TASKS.md` tracked an item claiming the 8/4 keys
(USDA_AMS/USDA_NASS/EIA/KROGER) were "NOT in this clone, so 5 pipelines SKIP".
That finding was checked against the wrong repo — those pipelines live here in
`consumer-goods-price-pipeline`, and this repo's `.env` has all the keys.

Verified live data present (fresh 2026-08-11 incremental pulls, all files exist):

- `kroger_products` — 3,523 rows / 1,223 products / 4 store regions across the
  20260804 + 20260811 files (Certification env, api-ce.kroger.com).
- `usda_ams_retail` + `usda_ams_wholesale` — 20260811 files present.
- `usda_prices_received` + `usda_prices_paid` — 20260811 files present.
- `EIA_API_KEY` present in `.env`.

## Only Best Buy remains SKIPping

`BESTBUY_API_KEY` is genuinely missing (signup rejects free/.edu email — see
TODO.md "Deferred"). Pipeline code is built and fully wired; activation is one
`.env` line once a non-free-provider email is available.

## Task tracker

- Corrected the stale item in `financial-data-pipeline\TASKS.md` (was "re-add the
  8/4 API keys to .env") — now reflects that only Best Buy is open. Session note
  appended to that repo's SESSION_NOTES.md too.
