# Olist Mercury - Master Plan (Brief)

Version 1.0 - 2026-09-21 - Owner: Line
Design detail: `docs/ARCHITECTURE.md`. Original spec: `docs/source/PROJECT_SPEC_v0.md`. Second analysis: `docs/source/ASTRA_ANALYSIS.md`.

## 1. Mission

Build a commerce intelligence project that shows **senior data analyst work on top of a sound data platform**: one developer, one 16 GB laptop, public, and defensible line by line. It serves Line's data analyst portfolio and a college DA course.

The analyst side: a business frame with named stakeholders, a KPI tree, a SQL star schema, ten case-study memos with statistics, a recommendation and an impact estimate in each, and coded dashboards.
The platform side: Olist data (about 100,000 orders, 2016-2018) flows through five data products - **Raw, Conformed, Analytical, Feature, Serving** - to an API, a web application, and a real-time replay. A source adapter isolates Olist, so a Chat Order, POS, or CRM adapter can use the same pipeline later.

The project answers four levels of question: what happened, why, what will happen, and what is happening now.

## 2. Locked decisions (Line, 2026-09-21)

| # | Decision |
|---|---|
| 1 | **No JVM in any processing code.** Python (Polars) does Raw, quality, Conformed, Feature, ML, and streaming. Spark is removed. |
| 2 | **The Analytical layer is SQL: dbt Core + DuckDB.** A star schema and marts with tests, documentation, and lineage. |
| 3 | **Balanced analyst and platform project.** Every phase ships an analyst deliverable next to the engineering one. |
| 4 | **Dashboards are coded** in the web application, as in the original spec. No BI tool. |
| 5 | **TypeScript for the API and the whole frontend**: Hono + Zod API, Next.js web, one pnpm workspace. FastAPI is removed. |
| 6 | **Kafka stays as the broker** (a container, `stream` profile only). Python produces and processes; the TypeScript API consumes for the WebSocket. |
| 7 | **CLI first, Dagster in Phase 7.** Airflow is removed. |
| 8 | **Public repository + Vercel demo.** The web application has a `live` mode and a `snapshot` mode. Vercel runs the snapshot mode with a recorded real-time replay. |
| 9 | **ClickHouse is the serving store.** PostgreSQL enters with MLflow in Phase 4. |
| 10 | **UI direction: operations console.** shadcn/ui (Radix + Tailwind), no bought admin template. Detail: architecture 9.2. |
| 11 | **No calendar dates and no time pressure.** Progress is measured by phase exit tests. Effort numbers are estimates for ordering, not promises. |
| 13 | **Docker first.** Every tool runs in a container, including the Python toolchain, dbt, and the TypeScript workspace. A user needs only Git and Docker. Versions are pinned by image digest and lock files; all configuration is in the repository. Detail: architecture 10.3. |
| 14 | **Free software only.** Every component is open source and self-hosted. Conditions and check dates are in `docs/LICENSE_REGISTER.md`. |
| 12 | **Non-commercial.** The dataset is CC BY-NC-SA 4.0 and Vercel Hobby is non-commercial. Raw data never enters git. |

Python and TypeScript never call each other. They meet at ClickHouse tables, Kafka topics, and generated schemas.

## 3. What changed from spec v0

### 3.1 Corrections

| Spec v0 | Problem | Correction |
|---|---|---|
| S16-18 retention, cohorts, K-Means on frequency | About 97% of customers buy one time (to verify). Cohorts go to zero; frequency is constant. | Feasibility gate. The low repeat rate is the finding. Segment on value, recency, freight, delay, review. |
| S26 product association rules | About 90% of orders hold one item (to verify) | Category-level baskets, per order and per customer. "No strong rules" is a valid result. |
| S29 "next hour" forecast | About 5 orders per hour | Daily primary, weekly secondary. Trim the sparse 2016 start and 2018 tail. |
| S31 delay regression only | The estimated date is padded; most orders arrive early | Two targets: `is_late` and `actual_delivery_days`. Expect a low R2 and say so. |
| S32 satisfaction features | Uses delivery delay with no prediction time | Prediction time = after delivery, before the review. Written contract per model. |
| S13 event: random UUID, float money | A replay doubles totals; floats lose cents | Deterministic `event_id` (UUIDv5); integer minor units + currency |
| S38 one topic per event type | No order guarantee across topics | One topic per aggregate, keyed by `order_id` |
| S35-36 simulator | Promises events the source never recorded (`payment.created` has no timestamp) | Evidence matrix: observed, inferred, synthetic, unavailable. Events reveal only fields known at their time. |
| S48 "Realtime Store" | Never defined | ClickHouse `rt` tables (history) + Kafka metric topics (push). No Redis. |
| S34 MLflow Staging / Production | Deprecated since MLflow 2.9 | Aliases `@challenger` and `@champion` |
| S11 quality checks | Checks without consequences | Each rule is FAIL, QUARANTINE, or WARN. Serving keeps the last good version. |
| "Revenue" everywhere | No definition; it is marketplace volume, not income | Metric dictionary. Headline = `gmv_items`, labelled "Item sales (GMV)". |
| S78 against S84 | Two phase orders | Vertical slice first (S84) |
| S49 against S50 | 10 menu items, 7 page specs | One inventory of 9 pages |
| S60 against S67 | `data/contracts` sits inside the git-ignored lake; loose top-level folders | `contracts/` at the root; one Python package `src/mercury`; one pnpm workspace |
| S39, S51, S59 Spark and Airflow | Against the spec's own rule "no technology without a problem"; about 7 GB RAM together (estimate) | Removed (decisions 1 and 4) |
| S77 "docker compose up starts all" | Conflicts with profiles and the RAM budget | `docker compose up` starts `core` only |
| S5, S69 licensing | Dataset license not mentioned | License register; download script; non-commercial notice |
| Intro text | ChatGPT reference marks and `utm_source` links | Removed in all new documents |

### 3.2 Enhancements (ranked by value against effort)

1. **Evidence card on every chart**: metric id, grain, coverage, row count, caveats. Blocks unsupported claims.
2. **Feasibility gates** measured in Phase 0. A module ships the full path or the honest fallback.
3. **Shared feature code + parity test**: one Python function serves batch and stream. Removes train/serve skew.
4. **Run manifest + atomic publication**: reproducible runs, real lineage, no half-loaded dashboard.
5. **End-to-end typed contract**: Pydantic -> JSON Schema -> Zod -> web types, with a drift check in CI.
6. **Snapshot mode**: a free public demo with no backend and no attack surface.
7. **Deterministic demo command**: `make demo` builds everything from the raw files. No manual steps in a defense.
8. **Replay controls and fault injection**: pause, seek, speed, duplicates, late events, spikes. Makes event-time behaviour visible and testable.
9. **Synthetic POS adapter fixture**: proves that the canonical model is not Olist-shaped.
10. **Resource benchmark report**: measured throughput, RAM, and latency behind every CV claim.

### 3.3 The analyst track (added after Line's DA requirement)

Spec v0 started from tables and tools. A senior analyst starts from a decision. The full practice is in architecture 8.4; in brief:

- **Business frame:** the analytics team of the Olist marketplace, with four stakeholders: marketplace lead, logistics operations, seller success, customer experience.
- **KPI tree:** delivered GMV = active customers x orders per customer x average order value, with experience drivers (on-time share, promise accuracy, review score) and supply drivers (active sellers, concentration).
- **Star schema in SQL (dbt):** `fct_orders`, `fct_order_items`, `fct_payments`, `fct_reviews`, and customer, product, seller, category, geography, and date dimensions. Tested and documented.
- **Ten memos (M0-M9),** each with: question and stakeholder, method, findings with evidence, recommendation, estimated impact in BRL, limits, next step. Topics: state of the marketplace; cost of a late delivery in review score; over-padded delivery promise; seller concentration and scorecard; category portfolio and Black Friday; installments and order value; why customers do not return; segments with actions; forecast for planning plus an A/B test design; drivers of late delivery with a risk-based intervention.
- **Statistical standards:** effect sizes and confidence intervals, rank-based tests for ordinal review scores, multiple-comparison control, strata checks for Simpson's paradox, practical against statistical significance, minimum-sample rules, "associated with" and not "causes".
- **Reproducible and reviewed:** each case has its SQL, a notebook, and a chart-data export; a review checklist gates publication; an analysis log keeps the dead ends.
- **Published in the product:** an Insights page renders the memos with live charts and evidence cards.

## 4. Phases

Each phase ends with a working, demonstrable state. "Done when" is the exit test.

| Phase | Engineering deliverable | Analyst deliverable | Done when |
|---|---|---|---|
| **P0 Foundation and profiling** | Repository, `worker` image and Compose file, `.env.example`, uv + pnpm, lint, types, tests, CI that runs inside the same image, download script, raw ingest | Business frame, KPI tree, EDA and profile report, memo template, review checklist, statistical standards | The profile report gives every number marked "to verify" in the architecture file. The feasibility gates are frozen. The time-zone assumption has a decision. |
| **P1 Vertical slice** | orders + items + payments: Raw -> quality -> Conformed -> dbt core models -> ClickHouse -> TS API -> Overview page -> snapshot export | `fct_orders`, `fct_order_items`, `dim_date`, `dim_customer` with tests and docs; KPI tree on the Overview page; **memo M0** | `gmv_items` reconciles with the raw files. `dbt build` is green. A rerun changes nothing. A clean clone runs with documented commands. **Line deploys to Vercel.** |
| **P2 Analytical breadth** | Full star schema and marts; Customers, Products, Sellers and Logistics, Satisfaction, Data Quality, Insights pages | **Memos M1-M5**; dbt documentation and lineage graph published | Relationship and cardinality tests pass for every model. Every metric on screen has a dictionary entry and an evidence card. Each memo passes the review checklist and matches a fresh rerun. |
| **P3 Data mining** | RFM, segmentation, association rules, batch anomaly detection | **Memos M6-M7** (why customers do not return; segments with sized actions) | Each module shows its gate result. Segments are stable across seeds. Anomaly detection finds injected spikes at a stated alert rate. |
| **P4 Forecasting + MLflow** | Feature sets, baselines, LightGBM / XGBoost, rolling-origin backtest, registry aliases, Forecasting and Models pages | **Memo M8**: forecast error in business terms, and the A/B test design with power analysis | A clean run reproduces the holdout metrics. The champion beats the seasonal-naive baseline, or the baseline is the champion and the page says so. |
| | **---- v1.0 cut line: public release, README, executive summary, slide deck, demo video, CV entry ----** | | |
| **P5 Delivery and satisfaction models** | Prediction-time contracts, as-of features, leakage tests, batch predictions in Serving | **Memo M9**: drivers of late delivery, cost and benefit by risk threshold | A leakage test fails when a post-event field enters a feature set. Seen and unseen sellers are reported separately. |
| **P6 Real time** | Event contracts, simulator, Kafka, Python processor, `rt` tables, WebSocket, Real-Time page, recorded replay for Vercel | Operations view: anomaly triage notes that say what an analyst checks first when an alert fires | With injected duplicates, late events, and one processor crash, the window totals equal the batch totals for the same period. The `stream` profile stays inside the RAM budget (measured). |
| **P7 Orchestration and MLOps** | Dagster assets and schedules (calls `dbt build`), promotion flow, retrain job | - | One Dagster run rebuilds Raw -> Serving. The asset graph matches the lineage section. A promotion records its reason. |
| **P8 Observability and hardening** | Prometheus, freshness and lag metrics, recovery tests, POS fixture adapter, benchmark report, security pass | - | A clean-machine run passes. The POS fixture flows to the dashboard with no downstream change. The benchmark report exists. |
| **P9 Optional** | Portuguese review NLP, Marketing Funnel module | Review-topic memo; seller acquisition funnel memo | Gate passed; results checked against a manual sample |

Structured logs and tests start in P0, not in P8.

Effort in focused days (estimate, to re-measure after P1; the analyst work is included): P0 3-4, P1 5-7, P2 9-12, P3 6-8, P4 7-9 (v1.0 total 30-40), P5 6-8, P6 8-10, P7 3-4, P8 5-6, P9 5-7. P6 can move in front of P5 when an early real-time demo matters more; only the live prediction card depends on P5.

## 4.1 Team handoff after the MVP (Line, 2026-09-21)

One owner builds the MVP (P0-P4). The team joins after v1.0. Reason: the seams between modules (contracts, star schema, API types) must be proven by one mind before several people build against them. A split before that point multiplies rework.

Workstreams after v1.0. Each one owns disjoint paths, so two people never edit the same files:

| Workstream | Owns | Meets the others at |
|---|---|---|
| Data pipeline | `src/mercury/{ingest,quality,conform,serving}`, `contracts/` | Conformed Parquet schemas, serving table schemas |
| Analytics SQL | `dbt/` | dbt sources (input), mart schemas (output), `contracts/metrics.yaml` |
| Analysis and memos | `analysis/`, memo content in `apps/web` | Star schema (read only), chart-data JSON |
| ML | `src/mercury/{features,mining,ml}` | `contracts/features.yaml`, prediction tables |
| API | `apps/api`, `packages/contracts` | Serving tables (read only), Zod schemas |
| Web | `apps/web` | Zod schemas, the `DataSource` interface |
| Platform (P6-P8) | `src/mercury/streaming`, `orchestration/`, `infra/` | Event contracts, Compose profiles |

What v1.0 must contain so that a team can start in one day:

- `CONTRIBUTING.md`: the Docker commands, branch and pull-request rules, commit style, the review checklist for code and the one for memos.
- `AGENTS.md` at the repository root: the same rules for AI coding agents, with the path ownership above.
- `CODEOWNERS`, a pull-request template, issue labels per workstream, and branch protection on `main` (Line sets it on GitHub).
- A change to anything in `contracts/` or `packages/contracts` needs a review from both sides of that seam.
- Decision records in `docs/decisions/`, so a new person learns why, not only what.
- A starter backlog: P5-P9 cut into issues, each with files, an exit test, and a size.
- Green CI and the clean-clone test: a new person runs the whole system with Git and Docker only.

## 5. Working method

- Supervisor (Claude) owns specs, architecture, review, and judgment on UI and copy. The Codex worker builds. Each phase splits into task packets with exact files and a VALIDATE block.
- Non-trivial logic is test-first. Fixtures are synthetic; no Olist rows enter git or CI.
- Before code that touches a library API (ClickHouse, Polars, Pandera, Hono, Next.js, MLflow, the stream library), check the current documentation. Do not write versions from memory.
- Git: no commit and no push without Line's order. The Vercel deploy is Line's action.

## 6. Risks

| Risk | Effect | Control |
|---|---|---|
| The data fails several gates | Thin mining pages | Fallback paths are planned deliverables, not failures (architecture 8.1) |
| Scope: 9 phases, one developer | The project stalls before release | v1.0 cut line after P4; every phase ships alone |
| RAM limit | The `stream` profile does not fit | Memory caps, profiles, measurement in P6; Kafka-free fallback recorded (architecture 13) |
| Feature skew between batch and stream | Wrong live predictions | One feature library + parity test |
| License | Take-down or misuse | No raw data in git; attribution; non-commercial; Line confirms the license page in P0 |
| Two languages | Contract drift between Python and TypeScript | Generated schemas + drift check in CI + row-parse contract test |
| Stream library health (Quix Streams) | Rework in P6 | Verify at P6 start; plain consumer fallback behind one interface |

## 7. CV wording (use only what is built)

For a data analyst role, lead with the analysis:

> Framed and answered ten business questions on a 100,000-order marketplace dataset: quantified the review-score cost of late delivery, measured over-padding of the delivery promise by region, built a seller scorecard, and explained a low repeat-purchase rate. Each memo gives an effect size with a confidence interval, a recommendation, and an impact estimate.
> Modeled the data as a tested and documented star schema in SQL (dbt, DuckDB), with a metric dictionary and a KPI tree. Designed an A/B test with power analysis. Built demand forecasts that are measured against seasonal baselines.
> Built the dashboards and an insights site in TypeScript (Next.js) over ClickHouse, with a definition and evidence card on every chart.

For an analytics-engineer or data-engineer role, add:

> Built batch and real-time pipelines in Python (Polars), Apache Kafka, and ClickHouse with data contracts, quality gates with quarantine, idempotent event replay, leakage-tested point-in-time features, MLflow champion / challenger aliases, Dagster orchestration, and CI with contract-drift checks.

Spark and Airflow no longer appear. The defense of that choice is one sentence: "I measured the data size (the P0 profile gives the number) and chose tools that fit it."

## 8. Open items for Line

1. Confirm the dataset license on the Kaggle page, and that publishing derived aggregates with attribution is acceptable to you.
2. Code license for the public repository (MIT or Apache-2.0).
3. The time-zone assumption (`America/Sao_Paulo`) after the P0 profile.
4. Kafka against the Kafka-free fallback: decide with the P6 RAM measurement, not now.
5. When the college course publishes a rubric or a required report format, send it. Each rubric line then maps to a memo, a model, or a page.

## 9. Next step

Phase 0 has two parts. The worker builds the first: repository skeleton (uv + pnpm workspace, lint, types, tests, CI), `scripts/download_olist.py`, raw ingest with metadata columns, and the profile report that measures every "to verify" number. The supervisor writes the second with Line: the business frame, the KPI tree, the memo template, the review checklist, and the statistical standards as project documents.
