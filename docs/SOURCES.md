# Data source index

Research-backed index of free consumer-goods price sources: what each covers,
whether it needs a key, where to register, how often it updates, and the
verified rate limits. This is the "everything from tires to avocados" map.

**Status: RESEARCHED AND VERIFIED 2026-08-03.** Every row below was checked
via live API probes, official docs, and current registration/ToS pages. The
three research passes covered (1) US government, (2) retail/e-commerce
developer tiers, (3) international statistics + crowdsourced. Corrections to
earlier assumptions are flagged inline.

## Conventions

- **Keyless** = no API key or registration needed.
- **Free key** = requires registration for a free key (instant unless noted).
- **Free tier** = a permanent free developer tier exists (may need approval).
- **Cadence** = how often data updates; this sets the pipeline fetch schedule.

---

## US government

| Source | Access | Coverage | Cadence | Limits / gotchas |
|---|---|---|---|---|
| **BLS CPI + Average Prices (APU)** | Free key (v1 keyless fallback) — register at https://data.bls.gov/registrationEngine/ | CPI indexes (thousands of series) + APU dollar average prices for ~90 items: eggs, milk, bread, ground beef, chicken, rice, produce, coffee, gasoline, diesel, electricity, nat gas. Series prefix `APU0000...`, since 1980 | Monthly (CPI release mid-month) | v1 keyless: 25 queries/day/IP, 10 yr window. v2 keyed: 500/day, 50 series/request, 20 yr window. Bulk FTP blocked (403). **PIPELINE: bls_cpi, bls_avg_prices (built)** |
| **USDA AMS MARS** | Free key — account at https://mymarketnews.ams.usda.gov/ (key in "My Profile") | Wholesale terminal-market fruit/veg per city; **FVWRETAIL** weekly retail advertised produce from 400+ retailers (incl. avocados) | Daily/weekly reports | Base URL is now **https://marsapi.ams.usda.gov/services/v1.2** (the old `mymarketnews.../api/v1/data/reports` is dead). Auth = HTTP Basic (key, empty pw). 100,000 records/req; **180-day date window/req** (paginate backfills). No published quota; blocks high-frequency polling. **PIPELINE: usda_ams (built, API updated to v1.2)** |
| **USDA NASS QuickStats** | Free key — request at https://quickstats.nass.usda.gov/api/ | Farmgate "prices received" (crops/livestock) + "prices paid" (~450 ag inputs). Producer, not consumer, prices | Monthly / annual | >100 calls/5 min = 429. 50,000 records/query. Keys die silently (re-request). Big backfills: bulk files at nass.usda.gov/datasets. **PIPELINE: usda_nass_prices (built)** |
| **EIA Open Data v2** | Free key (DEMO_KEY keyless works for testing) — https://www.eia.gov/opendata/register.php | Weekly retail gasoline/diesel (US + state), electricity price ($/kWh), natural gas residential. API v1 dead since Nov 2022 | Weekly (gas Wed.) / monthly | 5,000 rows/request cap -> paginate with offset/length. Bulk files keyless. **PIPELINE: eia_energy (built)** |
| **FRED** | Free key — https://fredaccount.stlouisfed.org/apikeys | 800k+ series: used-car CPI, headline CPI, PPI tires, retail sales. Auth via `Authorization: Bearer` header | Monthly (CPI release day) | ~120 req/min. **PIPELINE: fred_consumer (built)** |
| **Census Bureau retail trade** | Free key (required for every query since May 2026) — https://api.census.gov/data/key_signup.html | Monthly retail **sales $** by NAICS. **No prices at all** | Monthly | Dead end for a price pipeline (sales value, not prices). **REJECTED** |
| **NOAA Fisheries (FOSS) Commercial Landings** | Keyless — `https://apps-st.fisheries.noaa.gov/ods/foss/landings/` (Oracle ORDS REST) | Ex-vessel (dockside) seafood prices — species x state x region x year, 1950-present, ~159k commercial rows. `price_per_lb` = dollars/pounds (no direct price field) | Annual | **Live-verified 2026-08-14.** The old NEFSC "Boston/NY Market News" page (nefsc.noaa.gov/read/socialsci/marketNews.php) is dead (301s to a generic region page; its InPort metadata also flags internal-network access constraints). Found the real replacement via InPort item 10574's `ords/foss/metadata-catalog` link, which redirects to the current `apps-st.fisheries.noaa.gov` host. `q={"collection":"Commercial"}` param filters out null-priced MRIP recreational rows; the ORDS WAF (Akamai) 403s any `$` character in the query string, so operator filters like `$gte` don't work — paginate the full table instead (16 pages at limit=10000, a few seconds, no key/quota). ~0.8% of rows are `species="WITHHELD FOR CONFIDENTIALITY"` (NOAA small-cell suppression) — same non-key-able multi-row collision pattern as USDA AMS; left out of `curated.py` KEYS. **PIPELINE: noaa_seafood_landings (built 2026-08-14)** |

## International statistics

| Source | Access | Coverage | Cadence | Limits / gotchas |
|---|---|---|---|---|
| **Eurostat** | Keyless — https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/... | HICP indices (`prc_hicp_midx`), all-items + food COICOP groups, 43 geos (EU + EFTA + accession + UK/US). **Indices only** | Monthly (~mid-month) | **PRC_AVG "average consumer prices" dataset no longer exists (404 verified 2026-08-03)** — no absolute euro prices. JSON-stat 2.0 flat-value cube; `unit=I15` (2015=100) is the current base. **PIPELINE: eurostat_hicp (built 2026-08-04)** |
| **OECD** | Keyless — https://sdmx.oecd.org/public/rest/... | Dataflow `OECD.SDD.TPS,DSD_PRICES@DF_PRICES_ALL,1.0` — CPI index level (all-items + food), ~38-47 economies incl. G7/G20 aggregates | Monthly | Migrated off stats.oecd.org to sdmx.oecd.org (~2024). Key is 8 dot-separated dims: `REF_AREA.FREQ.METHODOLOGY.MEASURE.UNIT_MEASURE.EXPENDITURE.ADJUSTMENT.TRANSFORMATION` — level index = `.M.N.CPI.IX._T.N._Z`; `format=csv` keeps responses small. **PIPELINE: oecd_cpi (built 2026-08-04)** |
| **Statistics Canada** | Keyless — https://www150.statcan.gc.ca/ (SDMX) | **Absolute retail prices in CAD** — table 18-10-0245-01/02: milk, bread, ground beef, eggs, butter, produce, per-province. CPI 18-10-0004-01. Gasoline 18-10-0002. Scanner-data since Jan 2024. **Best free source of absolute grocery price levels** | Monthly (~22nd) | 25 req/s/IP (50/s server-wide); API locked 00:00-08:30 ET. Bulk sync via Delta File. **PIPELINE: statcan_retail_prices (built)** |
| **FAO** | Keyless bulk CSV ZIPs — https://bulks-faostat.fao.org/production/{ConsumerPriceIndices,Prices}_E_All_Data.zip | Two separate FAOSTAT domains feed two tables: **CP** domain = per-country monthly national food/general CPI (2000-present); **PP** (Producer Prices) domain filtered to "Meat of..." items, Element="Producer Price (USD/tonne)" = per-country annual meat producer prices (1991-present). FFPI (the single global headline index) has **no API** — HTML/Excel export only on fao.org/worldfoodsituation; the FAOSTAT CP domain's per-country granularity fits this repo's schema better anyway | Monthly (CP) / Annual (PP) | **License CC BY-NC-SA 3.0 IGO** (non-commercial, share-alike). Both bulk ZIPs are wide format (Y{year} columns), melted to long on ingest. **PIPELINE: fao_prices -> fao_food_prices + fao_meat_prices (built 2026-08-04)** |
| **World Bank Pink Sheet** | Keyless — https://www.worldbank.org/en/research/commodity-markets | ~70 commodity spot/benchmark prices (energy, metals, ag), nominal USD, monthly since 1960. Wholesale, not retail | Monthly (~1st) | Download URLs carry a rotating per-release hash (`thedocs.worldbank.org/.../CMO-Historical-Data-Monthly.xlsx`) — pipeline scrapes the current link out of the landing page HTML every run rather than hardcoding it. "Monthly Prices" sheet, 2 header rows (commodity, unit). **PIPELINE: worldbank_pinksheet (built 2026-08-04)** |
| **IMF PCPS** | Keyless — https://api.imf.org/external/sdmx/3.0/... | ~40 tracked primary commodity benchmark prices (aggregate indices + single-commodity USD unit prices), 1980s-1990s-present. Wholesale | Monthly | Dataflow `IMF.RES,PCPS,~`. **Correction (verified live 2026-08-04): COUNTRY code is `G001`, NOT the usual SDMX "world" code `W00`** — found by reading back the actual dimension values from a full-wildcard query, since neither the generic SDMX docs nor a blog walkthrough for a different dataflow got this right. Key = `COUNTRY.INDICATOR.DATA_TRANSFORMATION.FREQUENCY`; `startPeriod`/`endPeriod` params are silently ignored (always returns full history — filter client-side). **PIPELINE: imf_commodities (built 2026-08-04)** |
| **WFP Global Food Prices (HDX)** | Keyless — https://data.humdata.org/dataset/global-wfp-food-prices | Retail/wholesale food prices, 98 countries, per-market, per-year CSVs 1990-present. **Correction (verified live 2026-08-04): the dataset literally named `wfp-food-prices` is a dead snapshot frozen at 2021-08** — its own HDX metadata says it was replaced by `global-wfp-food-prices`, which is the actively-updated one (latest resource modified 2026-08-03) | Weekly | Strongest free updateable international retail-food feed. **PIPELINE: wfp_food_prices (built)** |

## Retail / e-commerce (free developer tiers)

| Source | Access | Coverage | Cadence | Limits / gotchas |
|---|---|---|---|---|
| **Best Buy Products API** | Free key, instant — https://developer.bestbuy.com/apis | 1M+ electronics products, current + historical, price + sale/clearance, availability by store/ZIP | Near-real-time | Base `api.bestbuy.com/v1/products`, apiKey query param, parenthesized filter syntax e.g. `(search=laptop)`, `format=json`, `page`/`pageSize` (max 100). **PIPELINE: bestbuy_products (built 2026-08-04, code verified against docs; SKIPs cleanly — no live key configured yet)** |
| **eBay Buy Browse API** | Free App ID + **additional license** — https://developer.ebay.com/api-docs/buy/browse/ | Marketplace listings, keyword/category search, price filters, deals. Prices are point-in-time seller asks | On-demand | 5,000 calls/day. Needs OAuth2 user token + Buy-API license request on top of App ID. **PLANNED: ebay_listings** |
| **Kroger Products API** | Free — https://developer.kroger.com/ | Grocery catalog; **price + aisle only returned with `filter.locationId`** (ZIP-localized). Locations API for store lookup | On-demand | ~10,000 product calls/day, ~1,600 location calls/day. OAuth2 client-credentials. **Correction (verified live 2026-08-04): a self-serve app registers in the Certification environment, which authenticates at `api-ce.kroger.com`** — `api.kroger.com` (Production) 401s "invalid credentials" for a Certification app's client ID/secret; Production is likely gated behind Kroger partner approval. Certification store data is also only partially seeded — one of 5 tracked ZIPs' nearest store 404s on every product search despite the Locations API returning it as real. Community client: github.com/CupOfOwls/kroger-api. **PIPELINE: kroger_products (built + activated 2026-08-04, live data: 1,759 rows, 1,061 products, 4/5 ZIPs)** |
| **Etsy Open API v3** | Free key — https://developer.etsy.com/ | Listing prices (maker/resale, skewed vs mainstream retail) | On-demand | "v3 Limited Access" for new apps. OAuth required. Low priority. |
| **Walmart** | **No general free tier** — affiliate/Impact Radius approval gate | Item prices for affiliate-eligible items only | - | **REJECTED** for a hobby pipeline. |
| **Amazon** | **Not viable** — Creators API (ex-PA-API 5.0, retired 2026-05-15) requires 10 qualifying sales in 30 days | Product offers | - | **REJECTED**. |
| **Target / Home Depot / Lowe's / Costco / IKEA / grocery chains** | No official APIs | - | - | **REJECTED** — scraping only (RedSky internal API = ToS risk). |

## Crowdsourced / healthcare / housing

| Source | Access | Coverage | Cadence | Limits / gotchas |
|---|---|---|---|---|
| **Open Prices** | Keyless read — full-snapshot Parquet dump at https://huggingface.co/datasets/openfoodfacts/open-prices (`prices.parquet`) | Real barcode (`type=PRODUCT`) AND loose/no-barcode (`type=CATEGORY` — produce like avocados/bananas) price observations from receipt/price-tag photo proofs: price, currency, date, OSM shop location. ~285k rows, updated daily. **Correction (verified live 2026-08-04): prices.openfoodfacts.org has no bulk dump/export endpoint** (checked every route under `/api/v1/`) and the "3 gzipped JSONL dumps" a prior pass assumed don't exist — the real bulk source is the Hugging Face Parquet snapshot | Daily | Use the HF dump for bulk, never the live paginated API. License ODbL. **PIPELINE: openfoodfacts (re-pointed 2026-08-04)** |
| **CMS drug pricing** | Keyless — https://data.cms.gov/data-api/v1/ | Medicare Part D spending by drug. **Program/payer amounts, not retail prices** | Annual | **Correction (verified live 2026-08-04): the "Medicare Part D Spending by Drug" dataset (uuid `7e0b4365-...`) has NO NDC column** — it's aggregated to brand-name x generic-name x manufacturer, not NDC-level (the original SOURCES.md assumption). 5,000 rows/page hard cap, paginate via `offset`. Rebates excluded; small cells suppressed. **PIPELINE: cms_drug_pricing (built 2026-08-04)** |
| **Hospital price transparency** | No central CMS API; aggregators below | Negotiated/cash rates per CPT per hospital | Varies | **Spiked 2026-08-14, NO-GO on both live aggregator candidates — see Rejected table below.** Turquoise Health's free research dataset still requires a manual DUA request (not probed). **REJECTED for now: hospital_prices** |
| **Zillow Research** | Keyless CSVs — https://www.zillow.com/research/data/ | ZORI (rent), ZHVI (home value), inventory, median prices, national->ZIP | Monthly (16th) | **History is restated every release** — archive a vintage copy monthly. ZTRAX restricted. **Optional add** |
| **Redfin Data Center** | Keyless — https://www.redfin.com/news/data-center/ | ~40 market metrics, RHPI, national->neighborhood | Weekly (Thu) + monthly | Aggregates only. Revisions normal. **Optional add** |

## Rejected / dead ends

| Source | Why rejected |
|---|---|
| Numbeo | API **paid-only** ($260/mo basic); academic tier "temporarily unavailable"; ToS explicitly bans scraping |
| Kelley Blue Book (InfoDriver) | Paid B2B contract only; no free tier |
| JD Power Values | From $945/1,000 lookups; no free tier |
| CarGurus / Carvana / TrueCar | No public API; ToS bans scraping + aggressive anti-bot |
| GasBuddy | No official API (only unofficial GraphQL scrape at ToS risk). Use EIA weekly gasoline instead |
| Census retail trade | Sales dollars only, no prices; now key-gated |
| Walmart / Amazon / Target / HD / Lowe's / Costco / IKEA | No viable free tier (approval gates or no API at all) |
| Kaggle mirrors (WFP, Blinkit, Zenodo) | Static/stale snapshots; use the underlying HDX/API instead |
| GlobalPetrolPrices | Paid subscription, only a 2-week free trial, no lasting free tier. Redundant with EIA/StatCan/Eurostat anyway (checked 2026-08-14) |
| Grocery-price scraper APIs (Apify "Grocery Prices", RapidAPI "Grocery API", FoodDataScrape) | Paid scraping-as-a-service, ToS risk against the underlying retailers, not real free sources (checked 2026-08-14) |
| USDA ERS Food Price Outlook | Excel-download only, no API; forecasts not actuals; redundant with BLS CPI food already in this repo (checked 2026-08-14) |
| ONS UK API (`api.beta.ons.gov.uk`) | Real keyless REST API, but the only price-relevant dataset (`cpih01`) is index-only and UK-only, redundant with existing `eurostat_hicp` UK series (checked 2026-08-14) |
| DoltHub `dolthub/transparency-in-pricing` (hospital rates) | Live keyless SQL API, but operationally infeasible: 794M-row `rate` table, any `GROUP BY`/aggregate query times out ("context deadline exceeded") even scoped to one hospital, and OFFSET pagination on a single hospital's rows took 29s per 5k-row page (would take days across ~8k hospitals). Metadata frozen since 2023-08 (bounty program ended); DoltHub's own blog says use this over the older, DoltHub-recommends-against `v3` — but neither is a viable live source (checked 2026-08-14) |
| PriceTransparency.io (`/api/hpt/rates`) | Live, keyless, well-shaped API (153 CMS-designated shoppable codes, ~36k rows/code nationally, 60 req/min, snapshot_date ~90-day recrawl) — otherwise a strong candidate, but its ToS Section 5(a) bars "systematically extract[ing] the Service's output in volumes inconsistent with your plan" and Section 5(b)/(c)-adjacent language bars redistributing exports "as a competing data product." A full curated-table pull (the whole point of this repo's query layer) is exactly what those clauses target under a free/anonymous plan. Would need to ask them directly for a bulk/redistribution-permitted tier before building against it (checked 2026-08-14) |

## Recommendations

**Status as of 2026-08-04: every keyless source in this file is built.**
BLS, USDA AMS/NASS, EIA, FRED, StatCan, WFP, Open Prices (Stage 1/2 core);
Eurostat HICP, OECD CPI, FAO (food + meat), World Bank Pink Sheet, IMF PCPS,
CMS drug pricing (Stage 1/3 "cheap to add" indices/wholesale sources).
Kroger and Best Buy are also built and wired but SKIP at runtime until a
free key is registered and added to `.env`.

**Still open:** register `BESTBUY_API_KEY` (developer.bestbuy.com/apis) to
activate `bestbuy_products`. Kroger is now live (2026-08-04). eBay (needs a
Buy-API license on top of the App ID) and a hospital-price aggregator
(Turquoise Health / PriceTransparency.io) remain unbuilt.

**Never build:** Numbeo, KBB, JD Power, CarGurus/Carvana/TrueCar, GasBuddy,
Walmart, Amazon, Target, HD, Lowe's, Costco, IKEA.
