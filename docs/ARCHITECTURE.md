# Olist Mercury - Architecture

Version 1.0 - 2026-09-21 - Owner: Line - Status: baseline for Phase 0

Inputs: `docs/source/PROJECT_SPEC_v0.md` (spec v0), the supervisor analysis, the independent Astra analysis (`docs/source/ASTRA_ANALYSIS.md`), and Line's four decisions of 2026-09-21.
The brief plan is in `docs/MASTER_PLAN.md`. When the two files differ, this file wins for design and the master plan wins for order of work.

Labels in this file: **(estimate)** = not measured. **(to verify)** = a fact that Phase 0 must check against the data or a primary source.

---

## 1. Purpose

Olist Mercury is a commerce intelligence platform for one developer and one 16 GB laptop.
It converts the Olist historical data into five data products, serves them through an API and a web application, and replays the history as an event stream for a real-time path.
All data processing code is Python. No JVM process runs any processing code. The API and the web application are TypeScript.
A source adapter isolates Olist. A later adapter (Chat Order, POS, CRM) can feed the same downstream pipeline.

## 2. Decisions that shape the design

| # | Decision | Reason |
|---|---|---|
| D1 | Processing has no JVM: Polars (Python) for Raw, Conformed, and Feature transforms; DuckDB as the SQL engine (under dbt, see D10, and for checks and exports). No Spark. | About 1.5 M rows in total (to verify). Spark solves no problem at this size and costs 3-4 GB RAM. One language gives one feature code path for batch and stream. |
| D2 | Apache Kafka stays as the event broker. It is infrastructure in a container, in the `stream` profile only. The stream processor is Python. | Line selected this option. Kafka gives replay, consumer groups, and per-key order, and it is the interface a real Chat Order producer will use. Astra's stricter reading (no Kafka, SQLite journal) is recorded in section 13 as the fallback. |
| D3 | Orchestration: one Python CLI first (`mercury run <stage>`), Dagster in Phase 7. No Airflow. | The CLI keeps the inner loop fast. Dagster assets map 1:1 to the five data products and give lineage. Airflow needs 3-4 GB (estimate) and adds no capability here. |
| D4 | Public GitHub repository and a public web demo on Vercel. | Vercel hosts only the Next.js application. The design therefore has two data modes: `live` and `snapshot` (section 9.3). |
| D5 | ClickHouse is the only serving store. PostgreSQL enters with MLflow in Phase 4. | The API needs a store that accepts concurrent stream writes and analytical reads. DuckDB is single-writer and in-process, so it cannot do this. PostgreSQL has no workload before MLflow and Dagster need a metadata database. |
| D8 | Language boundary (Line, 2026-09-21): Python owns all data work (ingest to publish, ML, simulator, stream processor). TypeScript owns the API and the whole frontend. FastAPI is removed. | The API only reads ClickHouse and Kafka topics, so it needs no Python. One language from the API contract to the browser removes a type-translation layer. The boundary between the two languages is data, not function calls: ClickHouse tables, Kafka topics, and generated schemas (section 9.1). |
| D9 | Balanced analyst and platform project (Line, 2026-09-21). The project serves a data analyst portfolio and a college DA course. Every phase ships an analyst deliverable (a memo, a model, a dashboard view) next to the engineering deliverable. Dashboards stay coded in the web application; no BI tool. | A DA reviewer judges the question, the SQL, the statistics, the insight, and the recommendation. The platform alone does not show these. Section 8.4 defines the analysis practice. |
| D10 | The Analytical layer is SQL: dbt Core with the DuckDB adapter builds a star schema and marts from Conformed Parquet. This narrows D1: Python (Polars) does Raw, quality, Conformed, Feature, ML, and streaming; SQL does Analytical. | SQL is the first skill of an analyst, and dbt is the usual analytics-engineering tool: models, tests, documentation, and a lineage graph in one place. dbt Core is Apache 2.0 (getdbt.com, checked 2026-09-21). It is a Python package and adds no service. |
| D6 | Vertical slice first. | Spec v0 S78 and S84 disagree. S84 is correct: prove Raw -> Serving -> API -> Web on one metric, then widen. |
| D7 | Every analytic promise has a feasibility gate. "Insufficient evidence" is a valid, published result. | Olist cannot support several promises of spec v0 (section 8.1). An honest negative finding is defensible. A forced chart is not. |

## 3. System context

```text
                        BATCH PATH (Python, host or worker container)

 Kaggle CSV --> [Ingest] --> RAW --> [Quality] --> [Olist Adapter] --> CONFORMED
                                        |                                  |
                                   quarantine                +-------------+-------------+
                                                             v                           v
                                                        ANALYTICAL                    FEATURE
                                                   (SQL: dbt + DuckDB,              (Python,
                                                    star schema, marts)           point-in-time)
                                                      |            |                     |
                                          case-study memos         |              [Train + Evaluate]
                                                             |                           |
                                                             |                     MLflow registry
                                                             |                     (@champion alias)
                                                             v                           |
                                                     [Publish, atomic] <-----------------+
                                                             |                  batch predictions
                                                             v
                                                   SERVING (ClickHouse)
                                                             |
             REAL-TIME PATH                                  |
                                                             |
 CONFORMED --> [Event Simulator] --> Kafka --> [Stream Processor, Python] --+--> ClickHouse rt_* tables
 (or a real producer later)            |              |                     |
                                       |              +--> mercury.dlq.v1   +--> mercury.metrics.v1
                                       |                                    +--> mercury.predictions.v1
                                       |                                                 |
                                       +------------------------------------------------+
                                                             |
                                                             v
          ---------------- Python above this line / TypeScript below ----------------
                                         API, TypeScript  (REST: ClickHouse, WebSocket: Kafka topics)
                                                             |
                                       +---------------------+---------------------+
                                       v                                           v
                              Next.js, mode = live                     Next.js, mode = snapshot
                              (laptop or Linux server)                 (Vercel, static JSON + recorded replay)
```

## 4. The five data products

| Product | Content | Storage | Write rule |
|---|---|---|---|
| Raw | Source files as received, plus `_source`, `_source_file`, `_file_sha256`, `_ingestion_timestamp`, `_batch_id`, `_schema_version` | Parquet under `data/raw/olist/batch_id=.../`. All columns stay as source text. | Immutable. Append a new batch. Never edit. |
| Conformed | Canonical entities with typed columns and stable keys | Parquet under `data/conformed/<entity>/` | Rebuilt from Raw. Deterministic. |
| Analytical | Star schema (facts and dimensions), marts, and time series | Parquet under `data/analytical/<model>/`, written by dbt (section 5.4) | Rebuilt from Conformed by `dbt build`. SQL only. |
| Feature | Point-in-time feature sets with a registry entry for each feature | Parquet under `data/feature/<set>/version=N/` | Versioned. A new definition gives a new version. |
| Serving | Tables shaped for API responses. No large joins at request time. | ClickHouse database `serving`, plus `rt` for the real-time path | Atomic publication (section 5.3). |

The five names are logical products, not five databases. `data/` is ignored by git. Contracts live in `contracts/`, not in `data/` (spec v0 S60 and S67 collided).

## 5. Batch path

### 5.1 Stages

```text
ingest -> quality -> conform -> analytical -> feature -> train -> publish -> export-snapshot
```

Each stage is a CLI command and, from Phase 7, a Dagster asset group. Each stage reads only the product before it.

### 5.2 Data quality policy

Spec v0 listed the checks but not the consequences. Each rule has one disposition.

| Disposition | Meaning | Examples |
|---|---|---|
| FAIL | The run stops. Serving keeps the last good version. | Missing file, missing required column, type that cannot be parsed, broken primary key in `orders`, orphan `order_items.order_id`, money reconciliation outside tolerance |
| QUARANTINE | The row moves to `data/quarantine/<entity>/` with a reason code. The run continues. | Negative price, `review_score` outside 1-5, timestamp that cannot be parsed |
| WARN | The row stays. The count goes to the quality report. | Null product category, lifecycle timestamps out of order, missing delivery date on a non-delivered order, category without translation |

Validation uses Pandera schemas on Polars frames. Quality results are a dataset (`analytical/data_quality_results`) and feed the Data Quality page.

Known source traps that the rules must name (all counts to verify in Phase 0):

- `review_id` is not unique, and one order can have more than one review. The Conformed grain is (`review_id`, `order_id`).
- `geolocation` has many rows per ZIP prefix. Conformed `location` holds one row per prefix: median latitude and longitude, with the row count. Do not drop duplicates at random.
- `payments` has several rows per order (`payment_sequential`). This is valid. Aggregate to order grain before any join to items.
- Items, payments, and reviews all multiply rows when joined directly. Every mart joins at order grain. A join-cardinality test protects each mart.
- Some products have a null category, and some categories have no translation. Map them to `unknown` and to the Portuguese name.
- `customer_id` is per order. `customer_unique_id` is the person.
- Timestamps have no time zone (section 6.3).

### 5.3 Run manifest and atomic publication

Every run writes `data/_runs/<run_id>/manifest.json`: input file hashes, git revision, lock-file hash, configuration, time cutoffs, row counts per stage, quality summary, and the versions below. The manifest is the lineage record.

```text
dataset_version   olist_2018_v1         (hash of the raw files)
schema_version    per contract file     (contracts/*.json)
pipeline_version  git revision
feature_version   per feature set
model_version     MLflow model version + alias
```

Publication to ClickHouse is atomic: load into `serving.<table>__new`, run reconciliation checks, then `EXCHANGE TABLES`. On failure the old table stays. A rerun with the same inputs gives byte-equal Parquet and equal Serving totals. A test proves this.

### 5.4 Analytical layer: dbt and the star schema

```text
Conformed Parquet --(dbt sources)--> staging views --> facts + dimensions --> marts --(external Parquet)--> publish to ClickHouse
```

- dbt Core with the DuckDB adapter. Conformed Parquet files are dbt sources. Models materialize as external Parquet under `data/analytical/`. The `publish` stage loads them into ClickHouse. DuckDB runs in-process; there is no server.
- The `analytical` CLI stage is `dbt build` (run + test). From Phase 7, Dagster calls the same command.
- Layers: `stg_` (rename and cast only), `fct_` and `dim_` (the star schema), `mart_` (one table per dashboard view or memo).

| Model | Grain |
|---|---|
| `fct_orders` | one row per order: status, lifecycle timestamps, delay, item count, `gmv_items`, freight, payment total, review score (first review), single-seller flag |
| `fct_order_items` | one row per order line |
| `fct_payments` | one row per payment row |
| `fct_reviews` | one row per (review, order) |
| `dim_customer` | one row per person (`customer_unique_id`), with first and last order dates |
| `dim_product`, `dim_seller`, `dim_category` | one row per entity |
| `dim_geo` | one row per ZIP prefix, with state and coordinates |
| `dim_date` | one row per day, with week, month, quarter, holiday and event flags (Black Friday 2017) |

- Every model has a description, column descriptions, and tests: `unique` and `not_null` on keys, `relationships` between facts and dimensions, accepted values, and singular tests for money reconciliation and join cardinality.
- Metrics are defined one time. `contracts/metrics.yaml` stays the source; dbt marts implement it, and a singular test compares each mart total with the metric definition query.
- `dbt docs generate` gives the data dictionary and the lineage graph. Both are portfolio artifacts and are linked from the web application.
- Python feature code does not read marts for model inputs. Point-in-time features come from Conformed data (section 8.2), because a mart holds full-history aggregates and would leak.
- To verify in Phase 1: the DuckDB adapter release that supports the current dbt Core major version.

## 6. Canonical model and contracts

### 6.1 Entities and grain

| Entity | Grain (one row per) | Key |
|---|---|---|
| customer | person | `customer_id` = `olist:<customer_unique_id>` |
| order | order | `order_id` = `olist:<order_id>`; holds `source_customer_key` (the per-order Olist id) |
| order_item | order line | (`order_id`, `line_number`) |
| payment | payment row | (`order_id`, `payment_sequence`) |
| review | review of an order | (`review_id`, `order_id`) |
| product, seller, category | entity | namespaced id |
| location | ZIP prefix | `zip_prefix` |

All identifiers carry a source namespace (`olist:`, later `chat:`, `pos:`). Two sources can then never collide.

Multi-seller orders: a review and a delivery outcome belong to the order. Seller marts attribute them only for single-seller orders and report the excluded share.

### 6.2 Money

Money is an integer in minor units plus an ISO currency: `amount_minor = 10000`, `currency = "BRL"`. No float carries money in any contract, event, or Serving table. Spec v0 S13 used `100.00` as a float.

### 6.3 Time

- Olist timestamps carry no zone. The adapter assumes `America/Sao_Paulo` (to verify) and converts to UTC. Brazil used daylight saving time in 2016-2018; `zoneinfo` handles it. Ambiguous local times use the first occurrence; the count goes to the quality report.
- Conformed keeps three fields: the original text, the UTC instant, and the assumed zone.
- The reference date for recency is the day after the last purchase in the data. It is never "today".

### 6.4 Metric dictionary (version 1)

Spec v0 used "revenue" without a definition. `contracts/metrics.yaml` is the single source; the API and every chart cite a metric id.

| Metric id | Definition |
|---|---|
| `gmv_items` | Sum of `order_item.price` for eligible orders. This is the headline number. The label on screen is "Item sales (GMV)", not "Revenue": it is marketplace volume, not Olist income. |
| `freight_total` | Sum of `order_item.freight_value` for eligible orders |
| `gross_order_value` | `gmv_items + freight_total` |
| `payment_total` | Sum of `payment.payment_value`. Used for reconciliation against `gross_order_value` only. |
| eligible order | `order_status` not in (`canceled`, `unavailable`) |
| `delivery_delay_days` | Actual delivery date minus estimated delivery date, in calendar days. Delivered orders only. |
| delivery class | On time: delay <= 0. Late: 1 to 7 days. Very late: more than 7 days. |
| satisfaction class | Negative: score 1-2. Neutral: 3. Positive: 4-5. |
| repeat customer | A customer with 2 or more eligible orders |

### 6.5 Event envelope (version 1)

```json
{
  "event_id": "uuid5(namespace, source | event_type | source_key | revision)",
  "event_type": "order.created",
  "event_version": 1,
  "event_time": "2017-11-24T13:05:11Z",
  "source": "olist",
  "source_key": "olist:<order_id>",
  "source_time_text": "2017-11-24 11:05:11",
  "evidence": "observed",
  "replay_run_id": "r-20260921-01",
  "emitted_at": "2026-09-21T10:30:00Z",
  "payload": { "order_id": "...", "customer_id": "...", "items": [], "total_minor": 10000, "currency": "BRL" }
}
```

Rules:

- `event_id` is deterministic. A retry or a second replay gives the same id. Sinks deduplicate on it. Spec v0 used a random UUID, so a replay would have doubled every total.
- The contract source is a Pydantic model in `mercury.contracts`. JSON Schema files in `contracts/events/` are generated from it and checked in. The TypeScript types and Zod validators in `packages/contracts` are generated from the same JSON Schema. CI fails when any of the three differ.
- Evolution: a new optional field is compatible. Any other change needs a new `event_version`, a new topic suffix, and an adapter test.
- Reveal rule: an event carries only the fields known at its `event_time`. `order.created` never carries a delivery date. This rule makes the replay honest and blocks leakage into real-time features.

### 6.6 Source-to-event evidence matrix

| Event | Olist source | Evidence |
|---|---|---|
| `order.created` | `order_purchase_timestamp` | observed |
| `order.approved` | `order_approved_at` | observed |
| `order.shipped` | `order_delivered_carrier_date` | inferred (carrier handoff is a proxy for shipment) |
| `order.delivered` | `order_delivered_customer_date` | observed |
| `payment.created` | The payments table has no timestamp | synthetic: uses the approval time, else the purchase time |
| `review.created` | `review_creation_date` (date precision) | observed |
| `order.canceled` | Final status only; no transition time | unavailable: not emitted in version 1 |

The real-time page shows the evidence label. Counts built on inferred or synthetic events carry a mark.

## 7. Real-time path

### 7.1 Topics

Spec v0 S38 used one topic per event type. Kafka orders messages only inside one partition, so `created` and `delivered` of one order could arrive out of order. Version 1 uses one topic per aggregate, keyed by `order_id`.

| Topic | Key | Content |
|---|---|---|
| `mercury.order-events.v1` | `order_id` | `order.*` and `payment.*` |
| `mercury.review-events.v1` | `order_id` | `review.created` |
| `mercury.metrics.v1` | `window_start` + metric | Window snapshots from the processor |
| `mercury.predictions.v1` | `order_id` | Prediction events |
| `mercury.dlq.v1` | source key | Invalid events: original bytes, reason code, processor version |

### 7.2 Event simulator

- Reads Conformed data and emits envelope events in event-time order.
- A replay clock maps history to wall time with a speed factor (for example 1 day = 1 minute). Speed changes the schedule only; `event_time` stays historical.
- Each replay has a `replay_run_id`. Controls: start date, end date, speed, pause, seek.
- Fault injection flags: duplicates, late events, malformed events, a volume spike. The recovery tests and the anomaly evaluation use them.

### 7.3 Stream processor (Python)

```text
consume -> validate (Pydantic) -> deduplicate (event_id) -> conform -> enrich (lookup tables)
        -> event-time windows -> anomaly score -> inference -> produce + sink
```

- Library: Quix Streams is the first candidate (Kafka-native, pure Python API, windows and state). Its license and maintenance status are to verify at the start of Phase 6. The fallback is a plain `confluent-kafka` consumer with project-owned windows. The processor sits behind a small interface, so the choice does not leak.
- Windows use `event_time`, a watermark, and an allowed lateness. Processing time would turn the replay speed into false spikes.
- Window output is an absolute snapshot per (`replay_run_id`, `window_start`, metric), never a delta. A retry overwrites; it does not add.
- Delivery semantics: at-least-once from Kafka, plus idempotent sinks. Together these give effectively-once totals. A test with injected duplicates and a processor crash proves it.
- Features: the processor imports the same `mercury.features` functions as the batch path. A parity test feeds the same orders through both paths and compares the vectors.
- Inference: the processor loads `models:/<name>@champion` at start. When a required feature is missing or an entity is unseen, the prediction event has `status = "abstained"` with a reason. It never invents a value.
- Anomaly score: robust z-score (median and MAD) of the window count against the same hour-of-week baseline from the batch path.

### 7.4 Sinks and the "Realtime Store"

Spec v0 S48 named a "Realtime Store" and never defined it. The definition is:

- **History:** ClickHouse database `rt`. Tables use `ReplacingMergeTree` keyed by `event_id` (events) or by (`replay_run_id`, `window_start`, `metric`) (snapshots). Queries select the latest version explicitly (`FINAL` or `argMax`). Background merges must not decide correctness.
- **Push:** the API WebSocket (TypeScript) consumes `mercury.metrics.v1` and `mercury.predictions.v1` directly and fans out to browsers through bounded queues. On connect, the client first gets a backfill from `rt`.
- No Redis and no extra store.
- Isolation: `rt` tables hold replay data only, tagged by `replay_run_id`. Batch Serving tables never union with them. This prevents double counting of the same history.

## 8. Analytics, mining, and ML

### 8.1 Feasibility gates

Phase 0 measures each number. The module then takes the full path or the fallback path. Both paths are valid deliverables.

| Module (spec v0) | Expected data reality (to verify) | Gate | Fallback when the gate fails |
|---|---|---|---|
| Retention and cohorts (S18) | About 3% of customers buy more than once | Repeat customers >= 5% | Publish the repeat rate as the finding. Show time-to-second-order for the repeat group. Unobserved cohort months stay blank, not 0%. |
| RFM and K-Means (S16-17) | Frequency is almost constant | Frequency variance is material | Segment on monetary, recency, freight share, delay, review, category breadth. Compare clusters against plain RFM groups. Check stability across seeds and time. |
| Association rules (S26) | About 90% of orders hold one item | Multi-item baskets >= 5,000 | Category-level baskets, per order and per customer. Report support honestly. "No strong rules" is a result. |
| Hourly forecast (S29) | About 5 orders per hour on average | Median hourly count >= 20 | Daily is primary, weekly is secondary. The "next hour" horizon is removed. |
| Category demand (S29) | Most categories are sparse per day | Category has >= 20 orders per day (median) | Forecast the top categories only; name the list in an eligible-series manifest. |
| NLP (S25) | Review text is Portuguese; many reviews have no text | Reviews with text >= 30,000 | Portuguese stop words and stemmer (to choose in Phase 9). Score-derived labels are weak supervision; check a manual sample. |

Time-series hygiene: build a coverage calendar. Trim the sparse start in 2016 and the tail in late 2018. Missing coverage is not zero demand. Mark Black Friday 2017 as a known event.

### 8.2 Prediction-time contracts

| Model | Prediction time | Allowed inputs | Target | Baselines | Metrics |
|---|---|---|---|---|---|
| Daily orders and GMV forecast | After the daily cutoff | Lags and shifted rolling means, calendar fields, known events | Next 1 to 7 days | Naive, seasonal naive, moving average | MAE, RMSE, sMAPE, MASE per horizon; rolling-origin backtest |
| Delivery | Order creation | Order, item, product, seller and customer location, distance from ZIP prefix, calendar, seller history **as of** order time | `is_late` (class) and `actual_delivery_days` (regression) | Class prior; median days; linear model | PR-AUC, recall on late orders, calibration; MAE, RMSE |
| Satisfaction | After delivery, before the review | Delivery inputs plus actual delay and delivery time | Negative, neutral, positive | Majority class; logistic regression | Macro-F1, negative-class precision and recall, PR-AUC |

Rules for all models:

- Chronological split: train, validation, and a final untouched holdout. No random split.
- Labels must mature: an order enters training only when its outcome date is before the cutoff.
- Rows of one order stay in one fold.
- Aggregates (seller history, customer history) are as-of aggregates. A full-history Customer 360 column never enters a feature set.
- Report seen and unseen sellers and customers separately.
- The estimated delivery date is padded, so most orders arrive early (to verify). Expect a low R2 on delay days. `is_late` is the useful product.
- A baseline can win. The report then says so, and the baseline is the champion.
- Model output is a prediction, not a causal claim.

### 8.3 Feature registry and MLflow

- `contracts/features.yaml` holds one entry per feature: name, type, definition, source, calculation, availability time, version.
- MLflow tracks parameters, metrics, the dataset, feature and code versions, and the artifact with its fitted preprocessing.
- Registry stages (Staging, Production) of spec v0 S34 are deprecated since MLflow 2.9 (checked 2026-09-21). Mercury uses aliases: `@challenger` after validation, `@champion` after it beats the current champion and the baseline on the holdout. Promotion is a CLI command with a recorded reason.
- MLflow runs in the `ml` profile with PostgreSQL as its backend store and a local artifact directory. It does not run all the time.

### 8.4 Analysis practice (the analyst track)

This section sets how analysis is done, in the way a senior analyst works at a large company. The engineering exists to make this work fast, correct, and repeatable.

**Business frame.** The analyst team of the Olist marketplace serves four stakeholders: the marketplace lead (growth), logistics operations (delivery promise), seller success (seller quality), and customer experience (satisfaction). Every analysis names its stakeholder and the decision it supports.

**KPI tree.** One north-star metric and its drivers. Every memo and every dashboard view hangs on one branch.

```text
Delivered GMV (gmv_items, delivered orders)
  = active customers x orders per customer x average order value
      |                   |                        |
      new customers       repeat rate              items per order, price mix,
      by state, channel   time to second order     installments, category mix
  experience drivers: on-time share, delivery days, promise accuracy, review score
  supply drivers:     active sellers, seller concentration, category coverage
```

**Case studies.** Each one is a memo with a fixed structure: question and stakeholder, context, data and method, findings with evidence, recommendation, estimated impact in BRL with the assumptions, limits, next step. One page of text first; the detail follows.

| # | Question | Main method | Phase |
|---|---|---|---|
| M0 | State of the marketplace: size, growth, mix, data coverage | Descriptive, KPI tree baseline | P1 |
| M1 | What does a late delivery cost in review score? | Dose-response curve by delay days, ordinal tests, stratification by category and state, regression with controls. Observed relation, not cause. | P2 |
| M2 | Is the promised delivery date over-padded, and where? | Promise error distribution by route and seller state; calibration; scenario for a tighter promise | P2 |
| M3 | Seller concentration and a seller scorecard | Pareto and Gini, scorecard with minimum-volume rules and shrinkage for small sellers | P2 |
| M4 | Category portfolio and the Black Friday effect | Growth against share, price bands, seasonality, event lift against a counterfactual baseline | P2 |
| M5 | Installments and order value | Distribution comparison, confounding by category and price | P2 |
| M6 | Why do customers not return? | Repeat rate, time to second order (Kaplan-Meier), first-order experience against return | P3 |
| M7 | Segments and an action per segment | RFM and clusters, with a sizing of each action | P3 |
| M8 | Forecast for planning, and an A/B test design for a tighter delivery promise | Forecast error in business terms; power analysis, sample size, primary and guardrail metrics, decision rule | P4 |
| M9 | Drivers of late delivery and a risk-based intervention | Model interpretation (permutation importance, partial dependence), cost and benefit by risk threshold | P5 |

**Statistical standards.**

- Report an effect size and a confidence interval, not only a p-value. Use the bootstrap when the distribution is unknown.
- Review scores are ordinal. Use rank-based tests (Mann-Whitney, Kruskal-Wallis) or ordinal models, not a t-test on the mean alone.
- Control multiple comparisons when many segments are tested (Benjamini-Hochberg).
- Check each aggregate finding inside the main strata (category, state, period). Report a reversal (Simpson's paradox) when it occurs.
- Separate statistical from practical significance. With 100,000 orders, tiny effects pass a test; state the size in business units.
- Minimum sample rules for rankings (sellers, products). Small groups are shrunk or excluded, and the rule is on the page.
- Words: "is associated with", not "causes", unless a design supports it. Missing reviews and undelivered orders are selection effects; name them.

**Reproducibility and review.**

- Each case lives in `analysis/<id>_<name>/`: SQL queries against the star schema, one notebook, and a script that writes the chart data as JSON. A rerun gives the same numbers.
- The memo is an MDX file in the web application (Insights page). Its charts use the same components and the same evidence cards as the dashboards, fed by the exported JSON.
- Review checklist before a memo is published: question answered, metric ids cited, grain and filters stated, sample sizes shown, interval given, strata checked, limits written, recommendation sized, numbers match a fresh rerun.
- An analysis log (`docs/analysis_log.md`) records each question, the date, the result, and dead ends. Dead ends are evidence of method.

## 9. Serving and application

### 9.1 API (TypeScript: Hono + Zod on Node)

- A separate long-running service in `apps/api`, not Next.js route handlers. The WebSocket and the Kafka consumer need a process that stays alive, and the API must also run without Vercel.
- Reads only `serving` and `rt` tables through the official ClickHouse JavaScript client. No analytical join at request time. Queries are parameterized; no string-built SQL.
- Groups: `/api/v1/analytics`, `/customers`, `/products`, `/sellers`, `/logistics`, `/satisfaction`, `/forecast`, `/predictions`, `/anomalies`, `/realtime` (REST plus `/realtime/ws`), `/models`, `/quality`, `/meta/metrics`.
- Zod schemas define every request and response. The OpenAPI document is generated from them. Stable error body (RFC 7807). Pagination on list routes.
- The web application imports the same Zod schemas and inferred types from `packages/contracts`. The frontend therefore cannot disagree with the API contract, and no code generation step sits between them.
- The API runs no model and imports no Python. Predictions reach it in two ways only: batch predictions in Serving tables, and stream predictions on `mercury.predictions.v1`. On-demand inference (a `POST /predict` route) is out of scope for version 1; it would need a small Python inference service behind the API.

**The language boundary.** Python and TypeScript never call each other. They share three things:

| Shared thing | Owner | How the other side stays in step |
|---|---|---|
| Serving and `rt` table schemas | Python (`mercury.serving` DDL) | A generated `contracts/serving/*.json` describes each table; TypeScript row types are generated from it; a contract test queries each table and parses one row with Zod |
| Event and metric-snapshot schemas | Python (Pydantic) | JSON Schema -> generated Zod validators (section 6.5) |
| Metric dictionary `contracts/metrics.yaml` | Python | The API serves it at `/meta/metrics`; evidence cards read it |

The Kafka client library for Node is to verify at the start of Phase 6 (candidates: the Confluent JavaScript client, KafkaJS); check the maintenance status then, not from memory.

### 9.2 Web (Next.js + TypeScript)

One page inventory (spec v0 S49 and S50 disagreed):

| # | Page | Main content |
|---|---|---|
| 1 | Overview | Item sales, orders, customers, average order value, review score, on-time share, trends |
| 2 | Customers | Customer 360, RFM, segments, repeat behaviour, geography |
| 3 | Products | Product and category performance, Pareto, price, association rules |
| 4 | Sellers and Logistics | Seller performance, delivery time and delay, freight, map |
| 5 | Satisfaction | Score distribution, delay against score (observed relation, not cause), topics when NLP exists |
| 6 | Forecasting | Actual against forecast, horizon, error metrics against the baseline |
| 7 | Real-Time | Live windows, anomalies, recent predictions, replay controls, evidence labels |
| 8 | Models | Champion and challenger, metrics, versions, data and feature versions |
| 9 | Data Quality | Rule results, quarantine counts, freshness, run manifests, link to the dbt documentation and lineage graph |
| 10 | Insights | The case-study memos (section 8.4), each with live charts and evidence cards |

Dashboards follow analyst standards, not only engineering ones. The Overview page opens with the KPI tree. Each page names its audience and the decision it supports. Every view has a period comparison, a drill-down path (state to city, category to product, seller to order), and metric definitions one click away.

Each chart has an **evidence card**: metric id, grain, date coverage, row count, caveats. This blocks unsupported claims during a defense.

**Design system (Line, 2026-09-21): operations console.**

- Component base: shadcn/ui (Radix primitives + Tailwind). The component code lives in the repository. No bought or cloned admin template.
- Charts: shadcn charts (Recharts) for lines, bars, and distributions. One more library only for the cohort heatmap and the Brazil map; choose it in Phase 2 against the current documentation. All three licenses go in the license register in Phase 1 (to verify).
- Look: dense, calm, neutral grays, one accent color, tabular numerals, dark mode first with a full light mode. Colors are tokens, never literals in components.
- One page skeleton on the nine dashboard pages (the Insights page is a reading layout): KPI row, main trend, two breakdowns, detail table. A global date-range filter sits in the header.
- Shell: fixed left sidebar with the ten pages, header with the page title, the date range, and the data-mode badge (`live` or `snapshot`).
- States are part of the design: loading skeleton, empty, error, "insufficient evidence" (a gate failed), and "recorded replay".
- Accessibility: keyboard reachable, visible focus, contrast checked in both themes, charts never encode meaning by color alone.

### 9.3 Two data modes and the Vercel deployment

The web application reads data through one `DataSource` interface with two implementations.

| Mode | Used where | Source |
|---|---|---|
| `live` | Laptop, Linux server | The TypeScript API: REST and WebSocket |
| `snapshot` | Vercel | Static JSON under `apps/web/public/snapshot/`, written by `mercury export snapshot` |

- The snapshot holds aggregates and a capped, anonymized sample for the entity pages. Size budget: under 5 MB (estimate).
- The Real-Time page in snapshot mode plays a recorded capture of `mercury.metrics.v1` and `mercury.predictions.v1` (`realtime-replay.ndjson`). A banner states "Recorded replay".
- The site footer carries the dataset attribution and the CC BY-NC-SA 4.0 notice. Snapshot files are derived data and keep that license. The code has its own license.
- Vercel Hobby allows only non-commercial, personal use (vercel.com docs, checked 2026-09-21). This agrees with the dataset license. A commercial use of Mercury needs a different dataset and a different plan.
- Vercel Functions support WebSockets with Fluid compute, with a 300 s limit on Hobby (checked 2026-09-21). Mercury does not use this in version 1, because Kafka and ClickHouse do not run on Vercel.
- A public `live` backend is out of scope for version 1. It needs TLS, authentication, rate limits, and a private database network first.
- The deploy to Vercel is an outward action. Line starts it.

## 10. Runtime

### 10.1 Technology table

A technology enters in the phase where its problem appears, not before.

| Technology | Problem it solves | Enters |
|---|---|---|
| Python, uv, Ruff, Pyright, pytest | Language, locked dependencies, lint, types, tests | P0 |
| Polars, PyArrow, Parquet | Transforms and columnar files | P0 |
| DuckDB | SQL reconciliation, data tests, snapshot export over Parquet | P0 |
| Pandera | Schema and quality rules on Polars frames | P1 |
| dbt Core + DuckDB adapter | SQL star schema and marts with tests, documentation, lineage | P1 |
| Jupyter, SciPy, statsmodels | Analysis notebooks, tests, intervals, regression | P1-P2 |
| lifelines (license to verify) | Time-to-second-order survival curves | P3 |
| ClickHouse | Concurrent analytical serving store | P1 |
| Pydantic | Data and event contracts (Python side) | P1 |
| Hono, Zod, ClickHouse JS client, Node LTS | API and API contracts (TypeScript side) | P1 |
| Next.js, TypeScript | Web application | P1 |
| pnpm workspace | One TypeScript workspace: `apps/api`, `apps/web`, `packages/contracts` | P1 |
| Docker Compose, GitHub Actions | Reproducible run, CI | P0-P1 |
| scikit-learn, mlxtend | Clustering, anomaly models, association rules | P3 |
| LightGBM, XGBoost, statsmodels | Forecast and prediction candidates | P4 |
| MLflow + PostgreSQL | Experiment tracking, registry, metadata database | P4 |
| Apache Kafka (KRaft, one node) | Event broker with replay and per-key order | P6 |
| Quix Streams or `confluent-kafka` | Python stream processing | P6 |
| Dagster | Schedules, asset lineage, retries | P7 |
| Prometheus, structured JSON logs | Metrics and diagnosis. Logs start in P0. | P8 |

Python version: 3.13 when the full lock resolves (LightGBM, XGBoost, MLflow, stream library); else 3.12. Phase 0 decides and pins it.

### 10.2 Compose profiles and the RAM budget

The laptop has 15.2 GB usable. Reserve about 6 GB for Windows, WSL2, the editor, and a browser. The budget for containers is about 9 GB. All numbers are estimates; Phase 8 replaces them with measurements.

| Profile | Services | RAM (estimate) |
|---|---|---|
| `core` | ClickHouse (memory cap 2 GB), API, web | 2-3.5 GB |
| `ml` | core + PostgreSQL + MLflow | 3.5-5 GB |
| `stream` | core + Kafka (heap cap 1 GB) + simulator + processor | 4.5-6 GB |
| `orchestrate` | core + PostgreSQL + Dagster | 3.5-5 GB |
| `observe` | adds Prometheus (Grafana optional) | +0.5-1 GB |

- Batch stages run on the host with `uv run mercury ...`, or in a `worker` container for a clean-machine proof.
- Do not run model training and the `stream` profile at the same time.
- A `full` profile exists for a Linux server with 32 GB or more. It is not for this laptop.
- `docker compose up` starts `core`. Spec v0 S77 promised every service from one command; this conflicts with S56.
- All ports bind to `127.0.0.1`. Keep working data and volumes on the WSL2 Linux file system; the Windows mount is slow.

## 11. Repository layout

```text
olist-mercury/
  apps/                   TypeScript (pnpm workspace)
    api/                  Hono API: REST over ClickHouse, WebSocket over Kafka topics
    web/                  Next.js application; public/snapshot/ for Vercel
  packages/
    contracts/            Zod schemas and types: API contract (hand-written), events and
                          serving rows (generated from contracts/*.json)
  src/mercury/            Python: one installable package
    adapters/olist/       source -> canonical
    adapters/pos_fixture/ synthetic second source (Phase 8)
    contracts/            Pydantic models: entities, events, API
    ingest/  quality/  conform/  features/
    mining/               rfm, clustering, association, anomaly
    ml/                   forecasting, delivery, satisfaction, registry
    streaming/            simulator, processor, sinks
    serving/              ClickHouse DDL, publish, snapshot export
    cli.py                `mercury` command
  dbt/                    SQL: models/staging, models/core (fct_, dim_), models/marts, tests, docs
  analysis/               one folder per case study: SQL, notebook, chart-data export script
  contracts/              generated JSON Schema, metrics.yaml, features.yaml
  orchestration/dagster/  Phase 7
  infra/                  compose files, ClickHouse and Kafka config, Prometheus
  tests/
    unit/  data/  contract/  integration/  recovery/  fixtures/ (synthetic only)
  docs/                   MASTER_PLAN.md, ARCHITECTURE.md, decisions/, source/
  notebooks/              exploration only; no pipeline logic
  data/                   git-ignored lake: raw, conformed, analytical, feature, quarantine, _runs
  scripts/download_olist.py
  docker-compose.yml  Makefile  README.md  LICENSE  LICENSE-DATA.md
  pyproject.toml  uv.lock                       (Python side)
  package.json  pnpm-workspace.yaml  pnpm-lock.yaml   (TypeScript side)
```

Spec v0 S67 had loose top-level folders (`analytics/`, `mining/`, `ml/`, `pipelines/`). They cannot import one another cleanly. One package can.

## 12. Cross-cutting rules

**Testing.** Unit tests for transforms, metrics, and features. Data tests for keys, ranges, join cardinality, and money reconciliation. Contract tests for events and the API. Integration tests for Raw -> Serving -> API. Recovery tests for duplicates, late events, malformed events, and a processor crash. Feature-parity tests for batch against stream. Non-trivial logic is test-first.

**CI (GitHub Actions).** Two lanes. Python: lint, type check, unit, contract, and data tests on synthetic fixtures. TypeScript (pnpm workspace): lint, `tsc`, API unit tests, web build. Then a contract-drift check (Pydantic -> JSON Schema -> Zod must regenerate with no diff) and one integration job with a ClickHouse service container. No Olist data enters CI (size and license). Image publication is manual, not a default step.

**Security.** No secrets in git; `.env` local and `.env.example` in the repository; GitHub Secrets in CI. Input validation at the API. CORS allows only the known web origins. Databases have no public port. Models load only from the project registry. A secret scan runs in CI.

**Logging and observability.** Structured JSON logs from Phase 0 with `run_id`, stage, and counts. Prometheus in Phase 8: API latency and errors, consumer lag, window lateness, stage duration, data freshness, quality failures, inference latency. Grafana is optional (AGPLv3; acceptable for this use).

**Licensing.** `docs/LICENSE_REGISTER.md` lists each dataset and major dependency with license, source link, and check date. The Olist data is CC BY-NC-SA 4.0 (Kaggle page; Line confirms on the page in Phase 0). Raw data never enters git; `scripts/download_olist.py` fetches it with the user's own Kaggle token. The project stays non-commercial while it uses this dataset.

## 13. Deferred and rejected

| Item | Status | Reason |
|---|---|---|
| Spark, PySpark, Spark Structured Streaming | Rejected (D1) | No problem to solve at this size; breaks the pure-Python rule |
| Airflow | Rejected (D3) | Dagster fits the asset model and the RAM budget |
| FastAPI | Rejected (D8) | The API is TypeScript. Python exposes no HTTP service in version 1. |
| Redis or another real-time store | Rejected | ClickHouse `rt` plus Kafka topics cover the need |
| SeaweedFS, Iceberg | Deferred | Local Parquet is enough. Revisit with a real multi-source volume. |
| Grafana | Optional in P8 | Prometheus alone answers the questions of spec v0 S65 |
| Kafka-free stream (SQLite journal and outbox) | Fallback | Astra's strict no-JVM design. Use it when Kafka does not fit the RAM budget in practice. The processor interface allows the swap. |
| Marketing Funnel dataset | Optional in P9 | Not needed by the core |
| `order.canceled` events | Deferred | The source has no transition time |

## 14. Open risks and items to verify

1. All dataset numbers in section 8.1. Phase 0 measures them and freezes the gates.
2. The time-zone assumption for Olist timestamps.
3. The dataset license text on the Kaggle page, and the rule for publishing derived aggregates. Line confirms.
4. Quix Streams license and maintenance status, at the start of Phase 6.
5. Python 3.13 support across the full lock.
6. Measured RAM per profile against the estimates in section 10.2.
8. The DuckDB adapter release that supports the current dbt Core major version (dbt Core v2 arrived in 2026-06); the `lifelines` license.
7. ClickHouse `EXCHANGE TABLES` and `ReplacingMergeTree` behaviour on the chosen ClickHouse version; check the current documentation before Phase 1 code.

Sources checked on 2026-09-21: kafka.apache.org (Kafka 4.3.1, 2026-06-25), spark.apache.org (Spark 4.2.0, 2026-07-14; used only to reject Spark on fit, not on age), mlflow.org (stages deprecated, aliases), kaggle.com/datasets/olistbr/brazilian-ecommerce (license, via search summary), vercel.com/docs (Hobby plan, WebSockets, function limits), getdbt.com/licenses-faq and docs.getdbt.com (dbt Core is Apache 2.0; dbt Core v2), duckdb.org (dbt with DuckDB, external Parquet).
