# Pipeline catalog

Every registered pipeline, what it pulls, which table(s) it lands in, and
which env keys it needs. `requires_env` in `run_all.py` is the source of
truth; missing keys make `run_all.py` SKIP the pipeline cleanly.

Legend: `keyless` = no key needed; `free key` = requires registration for a
free API key.

## Stage 1 — government statistics (free keys, or keyless)

| Pipeline | Table(s) | Covers | Key |
|---|---|---|---|
| `bls_cpi_pipeline.py` | `bls_cpi`, `bls_ppi` | CPI (consumer price index, all items + food subgroups), PPI | `BLS_API_KEY` — keyless v1 fallback |
| `bls_avg_prices_pipeline.py` | `bls_avg_prices` | ~60 BLS APU average prices — eggs, milk, bread, ground beef, chicken, coffee, gasoline, etc. | `BLS_API_KEY` — keyless v1 fallback |
| `usda_ams_pipeline.py` | `usda_ams_wholesale`, `usda_ams_retail` | AMS MARS market news — wholesale terminal prices, weekly retail produce (incl. avocados). API base URL + auth verified live 2026-08-03 (MARS v1.2, HTTP Basic key); FVWRETAIL confirmed, FVWV terminal slug pending live check | `USDA_AMS_API_KEY` |
| `usda_nass_prices_pipeline.py` | `usda_prices_received`, `usda_prices_paid` | NASS QuickStats — farm prices received (commodities) and paid (inputs) | `USDA_NASS_API_KEY` |
| `eia_energy_prices_pipeline.py` | `eia_gas_retail`, `eia_gas_spot`, `eia_electricity_price`, `eia_natgas_price` | Retail gasoline/diesel, spot prices, electricity price, natural gas citygate | `EIA_API_KEY` |
| `fred_consumer_prices_pipeline.py` | `fred_consumer_prices`, `fred_used_cars` | FRED consumer series — used-car prices, tires, housing, retail aggregates | `FRED_API_KEY` |
| `statcan_retail_prices_pipeline.py` | `statcan_retail_prices` | Statistics Canada retail prices — absolute CAD prices, milk to household goods | keyless |
| `wfp_food_prices_pipeline.py` | `wfp_food_prices` | WFP global food prices — 98 countries, per-market retail/wholesale. Reads `global-wfp-food-prices` on HDX, NOT the deprecated `wfp-food-prices` slug (frozen at 2021-08) | keyless |

## Stage 2 — retail / e-commerce / crowdsourced

| Pipeline | Table(s) | Covers | Key |
|---|---|---|---|
| `openfoodfacts_pipeline.py` | `openfoodfacts_prices` | Open Prices — real barcode/category price observations from receipts + price tags, via the Hugging Face Parquet snapshot (re-pointed 2026-08-04; the old version scraped the sparse main product DB) | keyless |

## Stage 3 — planned

Not yet implemented (registry entries reserved in `run_all.py` as a PLANNED
comment block; CATALOG rows, SCHEMAS, KEYS already wired in `query.py`,
`validate.py`, `curated.py` so adding the pipeline file is additive).

| Pipeline | Table(s) | Covers | Key |
|---|---|---|---|
| `eurostat_hpcp_pipeline.py` | `eurostat_hpcp` | EU Harmonised Index of Consumer Prices | keyless |
| `oecd_cpi_pipeline.py` | `oecd_cpi` | OECD CPI (member countries) | keyless |
| `fao_prices_pipeline.py` | `fao_food_prices`, `fao_meat_prices` | FAO food price indices, meat prices | keyless |
| `worldbank_pinksheet_pipeline.py` | `worldbank_pinksheet` | World Bank "Pink Sheet" commodity prices | keyless |
| `imf_commodities_pipeline.py` | `imf_commodities` | IMF primary commodity prices | keyless |
| `cms_drug_pricing_pipeline.py` | `cms_drug_pricing` | CMS drug pricing datasets | keyless |
| `ebay_pipeline.py` | `ebay_listings` | eBay Browse API — used goods, electronics | `EBAY_APP_ID` |
| `walmart_pipeline.py` | `walmart_products` | Walmart Product/Affiliate API | `WALMART_API_KEY` |
| `kroger_pipeline.py` | `kroger_products` | Kroger product API (groceries, household) | `KROGER_CLIENT_ID`/`SECRET` |
| `numbeo_pipeline.py` | (TBD) | Cost-of-living price data | keyless |
| `hospital_prices_pipeline.py` | `hospital_prices` | Hospital price transparency files | keyless |
| `bestbuy_products_pipeline.py` | (TBD: `bestbuy_products`) | Best Buy electronics prices | `BESTBUY_API_KEY` |

## Validation coverage

Every table in `query.py` CATALOG has a `validate.SCHEMAS` entry (enforced by
`tests/test_pipelines.py::test_validate_schemas_cover_catalog`) and a natural
key in `curated.KEYS` (or falls back to full-row dedup). Range checks default
to WARNING until proven stable — see `docs/ARCHITECTURE.md` and `CLAUDE.md`.
