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
      cleanly at runtime** — no `BESTBUY_API_KEY` configured yet; register
      free instant key at developer.bestbuy.com/apis to activate.

## Open (needs a free key registered + added to `.env`)

- [x] ~~Register KROGER_CLIENT_ID/SECRET~~ — done 2026-08-04, live-verified.
- [ ] Register `BESTBUY_API_KEY` at developer.bestbuy.com/apis, add to
      `.env` to activate `bestbuy_products_pipeline.py`.
- [ ] Same for the existing keyed Stage 1 pipelines still SKIPping:
      `USDA_AMS_API_KEY`, `USDA_NASS_API_KEY`, `EIA_API_KEY`, `FRED_API_KEY`.

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
