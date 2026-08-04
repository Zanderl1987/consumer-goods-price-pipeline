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

## International statistics

| Source | Access | Coverage | Cadence | Limits / gotchas |
|---|---|---|---|---|
| **Eurostat** | Keyless — https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/... | HICP indices (`prc_hicp_midx`, `prc_hicp_manr`, etc.), 500+ COICOP sub-indices, 27 EU + EFTA. **Indices only** | Monthly (~mid-month) | **PRC_AVG "average consumer prices" dataset no longer exists (404 verified 2026-08-03)** — no absolute euro prices. HICP rebased 2015->2025=100 Jan 2026. Big datasets (21.9M cells) must be filtered. **PLANNED: eurostat_hpcp** |
| **OECD** | Keyless — https://sdmx.oecd.org/public/rest/... | CPI dataflows, COICOP sub-indices, ~38 economies. Indices only | Monthly | Migrated off stats.oecd.org to sdmx.oecd.org (~2024). Dataflow IDs change; list catalog first. **PLANNED: oecd_cpi** |
| **Statistics Canada** | Keyless — https://www150.statcan.gc.ca/ (SDMX) | **Absolute retail prices in CAD** — table 18-10-0245-01/02: milk, bread, ground beef, eggs, butter, produce, per-province. CPI 18-10-0004-01. Gasoline 18-10-0002. Scanner-data since Jan 2024. **Best free source of absolute grocery price levels** | Monthly (~22nd) | 25 req/s/IP (50/s server-wide); API locked 00:00-08:30 ET. Bulk sync via Delta File. **PLANNED: statcan_retail_prices (top candidate)** |
| **FAO** | Keyless — https://bulks-faostat.fao.org/ , FFPI at fao.org/worldfoodsituation | FAO Food Price Index (5 groups, 1961-), FAOSTAT Consumer Price Indices (domain `CP`), Producer Prices (`APP`) | Monthly | **License CC BY-NC-SA 3.0 IGO** (non-commercial, share-alike). FPMA (true national retail prices) has NO API — HTML/CSV export only. **PLANNED: fao_prices** |
| **World Bank Pink Sheet** | Keyless — https://www.worldbank.org/en/research/commodity-markets | ~70 commodity spot/benchmark prices (energy, metals, ag). Wholesale, not retail | Monthly (~1st) | Download URLs carry a rotating month-key hash (PyPI `worldbank-commodities` helper exists). **PLANNED: worldbank_pinksheet** |
| **IMF PCPS** | Keyless — https://api.imf.org/external/sdmx/3.0/... | 100+ primary commodity benchmark prices, 1980-. Wholesale | Monthly | 10 req/5s/IP. Legacy endpoints deprecated; use api.imf.org SDMX 3.0. **PLANNED: imf_commodities** |
| **WFP Global Food Prices (HDX)** | Keyless — https://data.humdata.org/dataset/global-wfp-food-prices | Retail/wholesale food prices, 98 countries, per-market, per-year CSVs 1990-present. **Correction (verified live 2026-08-04): the dataset literally named `wfp-food-prices` is a dead snapshot frozen at 2021-08** — its own HDX metadata says it was replaced by `global-wfp-food-prices`, which is the actively-updated one (latest resource modified 2026-08-03) | Weekly | Strongest free updateable international retail-food feed. **PIPELINE: wfp_food_prices (built)** |

## Retail / e-commerce (free developer tiers)

| Source | Access | Coverage | Cadence | Limits / gotchas |
|---|---|---|---|---|
| **Best Buy Products API** | Free key, instant — https://developer.bestbuy.com/apis | 1M+ electronics products, current + historical, price + sale/clearance, availability by store/ZIP | Near-real-time | 5 req/s, ~50,000 req/day. **Top retail pick. NEW: recommend adding** |
| **eBay Buy Browse API** | Free App ID + **additional license** — https://developer.ebay.com/api-docs/buy/browse/ | Marketplace listings, keyword/category search, price filters, deals. Prices are point-in-time seller asks | On-demand | 5,000 calls/day. Needs OAuth2 user token + Buy-API license request on top of App ID. **PLANNED: ebay_listings** |
| **Kroger Products API** | Free — https://developer.kroger.com/ | Grocery catalog; **price + aisle only returned with `filter.locationId`** (ZIP-localized). Locations API for store lookup | On-demand | ~10,000 product calls/day, ~1,600 location calls/day. OAuth2 client-credentials. Community client: github.com/CupOfOwls/kroger-api. **PLANNED: kroger_products** |
| **Etsy Open API v3** | Free key — https://developer.etsy.com/ | Listing prices (maker/resale, skewed vs mainstream retail) | On-demand | "v3 Limited Access" for new apps. OAuth required. Low priority. |
| **Walmart** | **No general free tier** — affiliate/Impact Radius approval gate | Item prices for affiliate-eligible items only | - | **REJECTED** for a hobby pipeline. |
| **Amazon** | **Not viable** — Creators API (ex-PA-API 5.0, retired 2026-05-15) requires 10 qualifying sales in 30 days | Product offers | - | **REJECTED**. |
| **Target / Home Depot / Lowe's / Costco / IKEA / grocery chains** | No official APIs | - | - | **REJECTED** — scraping only (RedSky internal API = ToS risk). |

## Crowdsourced / healthcare / housing

| Source | Access | Coverage | Cadence | Limits / gotchas |
|---|---|---|---|---|
| **Open Food Facts + Open Prices** | Keyless read — https://world.openfoodfacts.org/api/v3/ , dumps at world.openfoodfacts.org/data, Open Prices at prices.openfoodfacts.org | Product/barcode DB (~4M). **Main DB price fields are thin for US products.** The real price data is **Open Prices**: timestamped barcode-level price observations (retailer, location, photo proof) as 3 gzipped JSONL dumps | Daily dumps | Use the dumps for bulk, never the live API (1 call ~ 1 scan). License ODbL. **PIPELINE: openfoodfacts (built; align to Open Prices for real price data)** |
| **CMS drug pricing** | Keyless — https://data.cms.gov/data-api/v1/ | Medicare Part D spending by drug (NDC-level), Part B, Medicaid. **Program/payer amounts, not retail prices** | Monthly/quarterly | No published limit. Rebates excluded; small cells suppressed. **PLANNED: cms_drug_pricing** |
| **Hospital price transparency** | No central CMS API; aggregators below | Negotiated/cash rates per CPT per hospital | Varies | **Turquoise Health** free research dataset (non-commercial DUA, 14 shoppable services) — https://turquoise.health/researchers/request_dataset. **PriceTransparency.io** public API, 60 req/min/IP — https://pricetransparency.io/docs/api. v3 MRF standard live since 2026-04-01. **PLANNED: hospital_prices (via aggregator)** |
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

## Recommendations

**Core (build next):** Statistics Canada 18-10-0245 (absolute grocery price
levels, keyless — the single best "avocados to eggs" retail-price source),
WFP HDX food prices (keyless, weekly, 76 countries), Best Buy (free instant
key electronics), and re-point Open Food Facts at the Open Prices dumps for
real barcode-level price history.

**Keep as built:** BLS (APU + CPI), EIA v2, FRED, USDA AMS (MARS v1.2) as the
US government core; NASS only if farmgate prices matter.

**Leave planned-but-not-priority:** Eurostat HICP (indices only now that
PRC_AVG is dead), OECD, FAO, World Bank Pink Sheet, IMF, CMS — all keyless,
cheap to add, but indices/wholesale rather than consumer retail.

**Never build:** Numbeo, KBB, JD Power, CarGurus/Carvana/TrueCar, GasBuddy,
Walmart, Amazon, Target, HD, Lowe's, Costco, IKEA.
