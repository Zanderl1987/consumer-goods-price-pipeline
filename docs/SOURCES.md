# Data source index

Research-backed index of free consumer-goods price sources: what each one
covers, whether it needs a key, where to register, and how often it updates.
This is the "everything from tires to avocados" map — used to decide what
gets a pipeline and what gets rejected.

**Status: UNDER RESEARCH — this table is being filled via web research.**
Seed entries are from domain knowledge and the reference financial pipeline;
each row is being verified (live API checks where possible) before its
pipeline is promoted from PLANNED.

## Conventions

- **Free tier** = a permanent free option exists (may need registration).
- **Keyless** = no API key needed.
- **Coverage** = what consumer-goods prices it actually carries.
- **Cadence** = how often data updates, which sets the fetch schedule.

## Shortlist (strong candidates)

| Source | Coverage | Auth | Cadence | Notes |
|---|---|---|---|---|
| BLS CPI + Average Prices (APU) | Groceries, energy, durable goods, rent | free key (keyless v1 fallback) | monthly | Seed pipeline written |
| BLS PPI | Producer-level prices feeding retail | free key | monthly | Seed pipeline written |
| USDA AMS Market News | Wholesale terminal + retail fruit/veg, avocados | free key | daily/weekly | SCAFFOLD, endpoints unverified |
| USDA NASS QuickStats | Farm prices received/paid | free key | monthly/seasonal | Seed pipeline written |
| EIA Open Data | Gasoline, diesel, electricity, natural gas | free key | weekly | Seed pipeline written |
| FRED | Used cars, tires, consumer aggregates | free key | monthly | Seed pipeline written |
| Open Food Facts | Crowdsourced grocery product prices | keyless | continuous | Seed pipeline written |
| Eurostat HICP | EU consumer price indices | keyless | monthly | Planned |
| OECD CPI | OECD country CPI | keyless | monthly | Planned |
| StatCan retail prices | Canadian retail prices | keyless | monthly | Planned |
| FAO food/meat prices | Global food price indices + meat | keyless | monthly | Planned |
| World Bank Pink Sheet | Commodity price series | keyless | monthly | Planned |
| IMF primary commodities | Commodity prices | keyless | monthly | Planned |
| CMS drug pricing | Prescription drug prices | keyless | quarterly | Planned |
| Hospital price transparency | Hospital/surgery prices | keyless | annual | Planned |

## Retail/e-commerce tier (free tiers, more friction)

| Source | Coverage | Auth | Cadence | Notes |
|---|---|---|---|---|
| eBay Browse API | Used/refurb goods, electronics, auto parts | free App ID | on-demand | Planned |
| Walmart Product API | Groceries, household, electronics | free tier | on-demand | Planned |
| Kroger API | Grocery prices by zip | free tier | on-demand | Planned |
| Numbeo | Cost-of-living, grocery, transport | keyless scrape | continuous | TOS-sensitive, low priority |

## Rejected / dead ends

| Source | Why rejected |
|---|---|
| (to be filled from research) | |

## Open questions

- (to be filled from research)
