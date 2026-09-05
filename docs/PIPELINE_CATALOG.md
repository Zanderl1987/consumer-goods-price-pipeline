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
| `eurostat_hicp_pipeline.py` | `eurostat_hicp` | Eurostat Harmonised Index of Consumer Prices — EU/EFTA, all-items + food COICOP groups. Indices only (PRC_AVG absolute-price dataset is dead) | keyless |
| `oecd_cpi_pipeline.py` | `oecd_cpi` | OECD consumer price indices, ~38-47 economies, all-items + food. New sdmx.oecd.org host (old stats.oecd.org is dead) | keyless |
| `fao_prices_pipeline.py` | `fao_food_prices`, `fao_meat_prices` | FAO national food-CPI (per-country monthly) + meat/livestock producer prices (per-country annual, USD/tonne). CC BY-NC-SA 3.0 IGO | keyless |
| `worldbank_pinksheet_pipeline.py` | `worldbank_pinksheet` | World Bank Pink Sheet — ~70 global commodity benchmark prices, nominal USD since 1960. Resolves the current release's rotating-hash download URL live from the landing page each run | keyless |
| `imf_commodities_pipeline.py` | `imf_commodities` | IMF PCPS — ~40 tracked commodity benchmark prices (index + USD unit-price), monthly since the 1980s-90s | keyless |
| `cms_drug_pricing_pipeline.py` | `cms_drug_pricing` | CMS Medicare Part D spending by drug, brand/generic/manufacturer x year. Program reimbursement, not retail cash price | keyless |
| `noaa_seafood_landings_pipeline.py` | `noaa_seafood_landings` | NOAA FOSS commercial landings — ex-vessel (dockside) seafood prices, per species/state/region/year, 1950-present. `price_per_lb` derived from dollars/pounds; no direct price field. Old NEFSC market-news page is dead, replaced by the live `apps-st.fisheries.noaa.gov/ods/foss` REST API (found live 2026-08-14) | keyless |
| `ers_specialty_crops_pipeline.py` | `ers_fruit_nut_prices`, `ers_veg_prices`, `ers_fruit_nut_trade` (--with-trade), `ers_veg_trade` (--with-trade) | USDA ERS specialty crops - monthly CPI/PPI price indexes + average retail prices (grapes, oranges, avocados, potatoes ...), optional import/export trade CSVs behind --with-trade. Both price URLs served byte-identical files live 2026-08-24 - see pipeline docstring anomaly note | keyless |
| `fews_net_pipeline.py` | `fews_net_food_prices` | FEWS NET market prices - per-country fetch of the website export renderer (`country`-scoped, `fields=body`, full 63-col schema). Country codes auto-discovered from `market.json` metadata; empty countries skipped. Default window 10y, `--backfill` for full history. Raw renderers + whole-dataset query are broken upstream; see SOURCES.md | keyless |

## Stage 2 — retail / e-commerce / crowdsourced

| Pipeline | Table(s) | Covers | Key |
|---|---|---|---|
| `openfoodfacts_pipeline.py` | `openfoodfacts_prices` | Open Prices — real barcode/category price observations from receipts + price tags, via the Hugging Face Parquet snapshot (re-pointed 2026-08-04; the old version scraped the sparse main product DB) | keyless |
| `kroger_pipeline.py` | `kroger_products` | Kroger grocery catalog prices, ZIP-localized to 5 tracked regions (price only returns with a store-scoped `filter.locationId`). Certification-environment app — authenticates at `api-ce.kroger.com`, not `api.kroger.com` | `KROGER_CLIENT_ID`/`SECRET` |
| `kroger_catalog_pipeline.py` | `kroger_catalog` | Kroger broad-term paginated sweep per store (~46 terms walked via `filter.start` to exhaustion) with channel availability flags. Verified live 2026-08-25: term-less listing rejected (PRODUCT-2016) and fulfillment pricing unsupported on Certification, so coverage is term-bounded and channels are booleans only; dead/test locationIds probed and skipped at run time | `KROGER_CLIENT_ID`/`SECRET` |
| `bestbuy_products_pipeline.py` | `bestbuy_products` | Best Buy electronics/appliance prices incl. sale/clearance, ~15 tracked search terms | `BESTBUY_API_KEY` |

## Stage 3 — planned

Not yet implemented (registry entries reserved in `run_all.py` as a PLANNED
comment block; CATALOG rows, SCHEMAS, KEYS already wired in `query.py`,
`validate.py`, `curated.py` so adding the pipeline file is additive).

| Pipeline | Table(s) | Covers | Key |
|---|---|---|---|
| `ebay_pipeline.py` | `ebay_listings` | eBay Browse API — used goods, electronics | `EBAY_APP_ID` |
| `walmart_pipeline.py` | `walmart_products` | Walmart Product/Affiliate API — **no general free tier, rejected** | `WALMART_API_KEY` |
| `numbeo_pipeline.py` | (TBD) | Cost-of-living price data — **paid API, rejected** | keyless |
| `hospital_prices_pipeline.py` | `hospital_prices` | Hospital price transparency files (via aggregator) — **spiked 2026-08-14, NO-GO on both live candidates, see `docs/SOURCES.md`** | keyless |

## Validation coverage

Every table in `query.py` CATALOG has a `validate.SCHEMAS` entry (enforced by
`tests/test_pipelines.py::test_validate_schemas_cover_catalog`) and a natural
key in `curated.KEYS` (or falls back to full-row dedup). Range checks default
to WARNING until proven stable — see `docs/ARCHITECTURE.md` and `CLAUDE.md`.
