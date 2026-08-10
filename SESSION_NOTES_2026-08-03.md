# Session Notes — 2026-08-03

**Branch:** master
**Session model:** big-pickle (opencode)
**Repo:** consumer-goods-price-pipeline (new, private, owner Zanderl1987)

## What happened

User asked to build a consumer-goods price pipeline ("tires to avocados")
mirroring `financial-data-pipeline`'s architecture, then to research free
sources and build a source table. Done in two phases.

## Phase 1 — repo creation + scaffold

- Created private repo `Zanderl1987/consumer-goods-price-pipeline`
  (default branch `master`) via `gh repo create`.
- Studied reference architecture from a shallow clone of
  `financial-data-pipeline` (and `freight-rail-data-pipeline`).
- Wrote the full scaffold to `C:\Users\zande\PycharmProjects\consumer-goods-price-pipeline`:
  - Infra: `storage_utils.py`, `logging_utils.py`, `query.py` (26-table DuckDB
    CATALOG + views), `curated.py` (natural-key dedup), `validate.py`
    (SCHEMAS + severity checks), `run_all.py` (PipelineSpec registry, stage
    filter, key-aware SKIP, post-run validate + curated compaction).
  - Seed pipelines (Stage 1 gov/keyless + Stage 2): `bls_cpi`, `bls_avg_prices`
    (~60 APU series), `usda_ams`, `usda_nass_prices`, `eia_energy`,
    `fred_consumer`, `openfoodfacts`.
  - Docs: README, CLAUDE, docs/ARCHITECTURE.md, docs/PIPELINE_CATALOG.md,
    docs/SOURCES.md (initial), .env.example, .gitignore, requirements.txt.
  - Tests: `tests/` — catalog/glob/path guards, pipeline-registry guards,
    storage partitioning, curated dedup, validation, logging. 52 tests.
- **Bug found + fixed during verification:** `query.py`'s module docstring
  embedded a `q.sql("""` example whose `"""` terminated the docstring early
  (IndentationError). Fixed by rewriting the example with string
  concatenation. The reference repo's own docs warn ASCII/encoding matters —
  keep docstring examples free of literal `"""`.
- `git init`, committed, pushed to origin/master. 52/52 tests green.

## Phase 2 — source research (3 parallel web passes, verified 2026-08-03)

Three general-purpose research agents verified free price sources via live
API probes + docs. Full detail in `docs/SOURCES.md`.

### Corrections to prior assumptions (matter for code)

- **USDA AMS API moved.** Old base `https://mymarketnews.ams.usda.gov/api/v1/
  data/reports` is dead. Correct: `https://marsapi.ams.usda.gov/services/v1.2`,
  HTTP Basic auth (username=key, empty password), 100k records/req, 180-day
  date window/req. Migrated `usda_ams_pipeline.py` to MARS v1.2 (slug
  endpoints, windowed backfill, Real User-Agent). FVWRETAIL verified
  (incl. avocados); FVWV wholesale terminal slug still needs a live check.
- **Eurostat PRC_AVG ("average consumer prices") dataset no longer exists**
  (404 verified). Eurostat HICP is indices-only. Absolute euro prices are gone.
- **USDA NASS keys die silently** (401) — re-request when needed.

### New high-value finds (next pipeline builds)

1. **Statistics Canada 18-10-0245-01/02** — keyless absolute retail price
   levels in CAD (milk, bread, ground beef, eggs, produce), scanner-data
   quality. Single best "eggs to avocados" retail-price source. 25 req/s/IP;
   API locked 00:00-08:30 ET.
2. **WFP Global Food Prices (HDX)** — keyless, weekly, 76 countries,
   1,500+ markets. Best updateable international retail-food feed.
3. **Best Buy Products API** — free instant key, 1M+ electronics products,
   current + historical prices, 5 req/s, ~50k req/day.
4. **Open Prices (Open Food Facts)** — real barcode-level price observations
   as JSONL dumps (ODbL). Re-point `openfoodfacts_pipeline.py` at Open Prices;
   the main OFF product DB has thin US price fields.

### Dead ends confirmed (do not build)

Numbeo (paid-only, ToS bans scraping), Walmart (affiliate gate), Amazon
(Creators API needs 10 sales/30 days), Target/HD/Lowe's/Costco/IKEA/groceries
(no API), KBB/JD Power/CarGurus/Carvana/TrueCar (paid or ToS-hostile),
GasBuddy (no official API), Census (sales $ only, not prices).

## Repo state at end of session

- 2 commits pushed to origin/master. Working tree clean. 52/52 tests passing.
- `usda_ams_pipeline.py` migrated to MARS v1.2; docs updated to reflect it.
- `docs/SOURCES.md` is the verified source index with a recommendations section.
- TODO.md created listing the agreed next builds; task list in session.

## Open work (next session, awaiting user go-ahead)

- Build StatCan retail-prices pipeline (top candidate).
- Build WFP HDX food-price pipeline.
- Build Best Buy products pipeline.
- Re-point Open Food Facts pipeline at Open Prices dumps.
- Optionally: Eurostat HICP (indices), OECD, FAO, World Bank, IMF, CMS — all
  keyless, planned rows already wired in CATALOG/SCHEMAS/KEYS.
