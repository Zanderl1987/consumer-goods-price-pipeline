# TODO — 2026-08-04

Source research for consumer-goods price data is complete (see
`docs/SOURCES.md`, verified 2026-08-03/04). Every keyless source from the
original backlog is now built.

## Done

- [x] **StatCan retail-prices pipeline** — `statcan_retail_prices`. Built,
      wired, live-verified 2026-08-04 (12,955 rows, 110 items, 13 geographies).
- [x] **WFP HDX food-prices pipeline** — `wfp_food_prices`. Built 2026-08-04.
      **Correction found during build:** the dataset SOURCES.md pointed at
      (`wfp-food-prices`) is a dead snapshot frozen at 2021-08 — replaced by
      `global-wfp-food-prices` (verified live via HDX's own metadata), which
      is what the pipeline actually reads. See docs/SOURCES.md.
- [x] **Re-point Open Food Facts at Open Prices** — `openfoodfacts_prices`
      now reads the Hugging Face `openfoodfacts/open-prices` Parquet snapshot
      (285k real barcode + category price observations). **Correction found
      during build:** the "3 gzipped JSONL dumps" SOURCES.md assumed don't
      exist — prices.openfoodfacts.org has no bulk export endpoint at all
      (checked every route under `/api/v1/`). ODbL, attribution: Open Food
      Facts contributors.
- [x] **Kroger products pipeline** — `kroger_pipeline.py` built 2026-08-04
      (OAuth2 client-credentials, 5 tracked ZIPs, 20 search terms). Fully
      wired into run_all.py/query.py/curated.py/validate.py/tests.
      **Activated and live-verified 2026-08-04**: 1,759 rows, 1,061
      products, 4/5 ZIPs (real Kroger/Ralphs/King Soopers product+price
      data). **Correction found live:** a self-serve app registers in the
      Certification environment, which authenticates against
      `api-ce.kroger.com`, NOT `api.kroger.com` (Production 401s
      "invalid credentials" for a Certification app's credentials — set
      `KROGER_ENV=production` in `.env` if a Production-tier app is ever
      approved). Also: Certification store data is only partially
      seeded — the Denver ZIP's "nearby store" from the Locations API
      404s on every product search; not fatal, just fewer rows for that
      ZIP.
- [x] **Eurostat HICP pipeline** — `eurostat_hicp` (renamed from the
      originally-reserved `eurostat_hpcp` typo). Built 2026-08-04,
      live-verified: 1,352 rows (incremental), 43 geos, all-items + food.
- [x] **OECD CPI pipeline** — `oecd_cpi`. Built 2026-08-04, live-verified:
      1,645 rows (incremental), 47 countries. New sdmx.oecd.org host.
- [x] **FAO prices pipeline** — `fao_food_prices` + `fao_meat_prices`. Built
      2026-08-04, live-verified: 32,597 food-CPI rows (241 areas) + 1,074
      meat producer-price rows (95 areas, 26 items). **Correction found
      during build:** natural key needed `area` added (originally-reserved
      key was item-only, which would have collided across countries).
- [x] **World Bank Pink Sheet pipeline** — `worldbank_pinksheet`. Built
      2026-08-04, live-verified: 1,494 rows (incremental), 68 commodities.
      Resolves the current rotating-hash download URL from the landing page
      HTML each run rather than hardcoding it.
- [x] **IMF PCPS pipeline** — `imf_commodities`. Built 2026-08-04,
      live-verified: 1,452 rows (incremental), 39 indicators. **Correction
      found during build:** COUNTRY code is `G001`, not the usual SDMX
      "world" code `W00` — found by reading back real dimension values from
      a wildcard query.
- [x] **CMS drug pricing pipeline** — `cms_drug_pricing`. Built 2026-08-04,
      live-verified: 61,405 rows, 3,508 drugs, 5 years. **Correction found
      during build:** the dataset has no NDC column (aggregated to
      brand/generic/manufacturer instead) — natural key corrected from the
      originally-reserved NDC-based one. Also surfaced a repo-wide gotcha:
      the domain column had to be named `spending_year`, not `year` — a
      literal `year` column collides with `write_partitioned`'s Hive
      `year=YYYY/` partition and silently gets overwritten on read-back.
      Documented in CLAUDE.md gotchas.
- [x] **Best Buy products pipeline** — `bestbuy_products`. Built 2026-08-04
      (keyword search across 15 terms, `format=json`). Fully wired. **SKIPs
      cleanly at runtime, DEFERRED** — Best Buy's developer signup rejects
      free/.edu email addresses ("Sorry. Free email and .edu addresses are
      not allowed at this time"), and this account doesn't have a
      registerable business domain email. Code is ready; activate whenever
      a qualifying email is available — see "Deferred" section below.

## Open (needs a free key registered + added to `.env`)

- [x] ~~Register KROGER_CLIENT_ID/SECRET~~ — done 2026-08-04, live-verified.
- [x] ~~Register EIA_API_KEY~~ — found in an existing local credentials doc
      2026-08-04, live-verified: 468 gas-retail + 280 gas-spot + 310
      electricity + 355 natgas rows. **This was the pipeline's first-ever
      live run** (never had a key before) and it surfaced two real bugs in
      `eia_energy_prices_pipeline.py` — the electricity route's `data[]`
      param doesn't accept "value" (only revenue/sales/price/customers),
      and the natgas route has no `stateid` column at all (facets are
      duoarea/product/process/series). Both fixed; `validate.py`'s SCHEMAS
      for both tables also corrected — they'd assumed a `value` column
      neither table actually produces (`cents_per_kwh` / `price` instead).
- [x] ~~Register FRED_API_KEY~~ — found in the same doc, live-verified:
      1,280 + 118 rows. Worked first try, no code changes needed.
- [x] ~~Register USDA_NASS_API_KEY~~ — user registered a real NASS key
      2026-08-04 (a "USDA API KEY" found earlier in a local credentials doc
      turned out to be for FoodData Central, unrelated). Live-verified: 4,101
      prices_received rows (19/19 commodities) + 923 prices_paid rows (6/6).
      **This was the pipeline's first-ever live run** and surfaced real
      bugs in `usda_nass_prices_pipeline.py`: a single sector/group filter
      couldn't cover a commodity list spanning livestock/dairy/poultry/
      crops/fruit-veg (fixed: filter on `statisticcat_desc="PRICE RECEIVED"`
      instead, drop sector/group); two PRICES_PAID commodity names
      ("ANIMAL DRUGS", "BABY CHICKS") don't exist in NASS's taxonomy at all
      (swapped for real ones); date was built from `year` alone, collapsing
      up to 12 monthly observations per commodity/year into one row — once
      curated.py deduped on (commodity, date) this destroyed 99%+ of the
      data (35,210→42 rows) before the fix (now uses the real month via
      `begin_code`, natural key gained `description`).
- [x] ~~Register USDA_AMS_API_KEY~~ — user registered via eAuth 2026-08-04.
      Live-verified: 2,749 retail rows + 41,288 wholesale rows in ~72s.
      **This pipeline's first-ever live run timed out at 900s with zero
      output** — turned out to be three stacked bugs in
      `usda_ams_pipeline.py`: (1) wrong endpoint path (`{base}/{slug}` 404s;
      `{base}/reports/{slug}` alone returns narrative text, not prices — the
      per-commodity data needs `{base}/reports/{slug}/Report Details`
      explicitly); (2) `FVWV`, the originally-assumed national terminal
      report slug, doesn't exist at all — there's no single national
      terminal report, only ~30 per-city fruit/veg report pairs (~half
      "Discontinued", including Dallas and San Francisco — swapped for 6
      active US cities); (3) the real cause of the 900s timeout — AMS's
      `date_start`/`date_end` params are silently **ignored** on the
      Report Details endpoint (confirmed live: a 2020 date window returned
      2026 data), so every request was pulling ~100k largely-unfiltered
      historical rows at 90-120s each. Switched to the `lastDays` param
      (v3.1 only, found via `/services/help`), which filters correctly and
      cut request time to ~5s. Also: `curated.py`'s natural key had to be
      dropped entirely for these two tables — AMS terminal reports
      legitimately publish multiple simultaneous vendor quotes that share
      every field the API exposes but have different prices, so full-row
      dedup (only removes exact re-fetched duplicates) is the correct,
      non-data-losing behavior here.

**All 21 keyed/keyless pipelines that can run without a business-domain
email are now live.** Only Best Buy remains SKIPping (deferred, see below).

## Deferred

- [ ] **Best Buy** (`bestbuy_products`, `BESTBUY_API_KEY`) — 2026-08-04:
      developer.bestbuy.com's signup form rejects free-provider and .edu
      email addresses ("Sorry. Free email and .edu addresses are not
      allowed at this time"). Not worked around with a fake/misrepresented
      email — that's misleading Best Buy's own eligibility check on
      purpose, not something to do even for a low-stakes hobby key. Revisit
      if/when a real non-free-provider domain email becomes available, or
      if Best Buy opens an individual-developer path. Pipeline code is
      already built and fully wired — activation is a one-line `.env` add
      whenever a key exists.

## Not built (deprioritized / needs more than a free key)

- [ ] eBay Browse API (`ebay_listings`) — needs OAuth2 user token + a
      separate Buy-API license request on top of the App ID.
- [ ] Hospital price transparency (`hospital_prices`) — no central CMS API;
      would need an aggregator (Turquoise Health research dataset or
      PriceTransparency.io, 60 req/min).

## Explicitly NOT building (verified dead ends)

Numbeo (paid), Walmart/Amazon/Target/HD/Lowe's/Costco/IKEA (no free tier),
KBB/JD Power/CarGurus/Carvana/TrueCar (paid/ToS-hostile), GasBuddy (no API),
Census retail trade (sales $ only).

## Known gaps / verification needed

- `usda_ams_pipeline.py` FVWV wholesale-terminal slug unverified (FVWRETAIL
  confirmed). Needs a live run with a real USDA_AMS_API_KEY.
- IMF PCPS indicator list (`imf_commodities_pipeline.py` INDICATORS) covers
  ~39 consumer-relevant codes out of 136 total in the CL_PCPS_INDICATOR
  codelist — expand if a specific commodity is needed later.
