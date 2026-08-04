# TODO — 2026-08-03

Source research for consumer-goods price data is complete (see
`docs/SOURCES.md`, verified 2026-08-03). These are the agreed next builds,
awaiting the user's go-ahead.

## In Progress / Next Builds (pending user prompt)

- [ ] **StatCan retail-prices pipeline** — `statcan_retail_prices`. Keyless,
      absolute CAD retail price levels (table 18-10-0245-01/02: milk, bread,
      ground beef, eggs, produce). Top candidate. Wire CATALOG `statcan_retail_prices`
      (row already exists), SCHEMAS, KEYS, run_all spec (keyless), tests, docs.
- [ ] **WFP HDX food-prices pipeline** — new table (`wfp_food_prices`), keyless,
      weekly, 76 countries / 1,500+ markets via
      https://data.humdata.org/dataset/wfp-food-prices. Add to CATALOG/SCHEMAS/
      KEYS/EXPECTED_TABLES/docs.
- [ ] **Best Buy products pipeline** — new table (`bestbuy_products`), free
      instant key (BESTBUY_API_KEY), electronics prices incl. sale/clearance.
      Add wiring + tests.
- [ ] **Re-point Open Food Facts at Open Prices** — `openfoodfacts_prices` should
      read the Open Prices JSONL dumps (barcode-level price observations), not
      the sparse price fields on the main product DB. Confirm ODbL attribution.

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
