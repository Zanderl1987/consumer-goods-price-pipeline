# TODO — 2026-08-04

Source research for consumer-goods price data is complete (see
`docs/SOURCES.md`, verified 2026-08-03). These are the agreed next builds.

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

## In Progress / Next Builds (pending user prompt)

- [ ] **Best Buy products pipeline** — new table (`bestbuy_products`), free
      instant key (BESTBUY_API_KEY), electronics prices incl. sale/clearance.
      Add wiring + tests.

## Optional / Planned (all keyless, cheap to add)

- [ ] Eurostat HICP indices (`eurostat_hpcp`) — indices only; PRC_AVG is dead.
- [ ] OECD CPI (`oecd_cpi`).
- [ ] FAO FFPI + FAOSTAT CP (`fao_food_prices`, `fao_meat_prices`) — CC BY-NC-SA.
- [ ] World Bank Pink Sheet (`worldbank_pinksheet`) — rotating URL hash.
- [ ] IMF PCPS (`imf_commodities`) — 10 req/5s.
- [ ] CMS drug pricing (`cms_drug_pricing`) — program prices, not retail.

## Explicitly NOT building (verified dead ends)

Numbeo (paid), Walmart/Amazon/Target/HD/Lowe's/Costco/IKEA (no free tier),
KBB/JD Power/CarGurus/Carvana/TrueCar (paid/ToS-hostile), GasBuddy (no API),
Census retail trade (sales $ only).

## Known gaps / verification needed

- `usda_ams_pipeline.py` FVWV wholesale-terminal slug unverified (FVWRETAIL
  confirmed). Needs a live run with a real USDA_AMS_API_KEY.
- No live keys configured yet in `.env` — Stage-1 gov pipelines SKIP until
  keys are added.
