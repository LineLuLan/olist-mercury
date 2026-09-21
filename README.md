# Olist Mercury

**Business analysis of a 100,000-order marketplace, on a data platform built for it: a SQL star schema, ten case-study memos with statistics and recommendations, forecasts, coded dashboards, and a real-time replay. It runs on one laptop, with no paid service.**

> **Status (2026-09-21): design complete, build not started.** Phase 0 is next. This page describes the target system. The roadmap below shows what exists. Nothing here is a claim about finished work.

## What it is

Olist Mercury takes the public Olist dataset (about 100,000 real, anonymized e-commerce orders from Brazil, 2016-2018) and works on it the way the analytics team of a marketplace would. It has two sides that ship together in every phase:

- **The analysis.** A business frame with four stakeholders (marketplace lead, logistics, seller success, customer experience), a KPI tree, and ten case-study memos. Each memo has a question, a method, findings with an effect size and a confidence interval, a recommendation, an impact estimate, and its limits.
- **The platform.** The pipelines, the SQL model, the API, and the web application that make each number correct, repeatable, and visible.

Questions the memos answer: What does a late delivery cost in review score? Is the promised delivery date over-padded, and where? Why do customers not return? Which sellers carry the marketplace, and how should they be scored? What did Black Friday really add? How should a test of a tighter delivery promise be designed?

The project answers four levels of question:

| Level | Question | Method |
|---|---|---|
| 1 | What happened? | Analytics: sales, customers, products, sellers, delivery, geography |
| 2 | Why did it happen? | Data mining: RFM, segmentation, association rules, anomaly detection |
| 3 | What will happen? | Machine learning: demand forecast, late-delivery prediction, satisfaction prediction |
| 4 | What is happening now? | Real time: the order history replayed as a Kafka event stream, with live metrics and predictions |

Olist is the first data source, not the purpose. A source adapter converts Olist into one canonical order model. A later adapter (chat orders, POS, CRM) can feed the same pipeline with no downstream change.

## How it works

```text
 Olist CSV --> RAW --> quality gates --> CONFORMED --+--> ANALYTICAL --+--> case-study memos
   (Python: Polars, Pandera)                         |    (SQL: dbt + DuckDB,  |
                                                     |     star schema, marts) |
                                                     +--> FEATURE --> ML models (MLflow)
                                                                       |
                                                                       v
 CONFORMED --> event simulator --> Kafka --> Python stream processor --> SERVING (ClickHouse)
                                                                       |
                        ------------- Python above / TypeScript below -------------
                                                                       v
                                              API (Hono + Zod): REST + WebSocket
                                                                       |
                                                                       v
                                                        Web app (Next.js, TypeScript)
```

The five data products, in order: **Raw** (immutable source copy), **Conformed** (canonical entities), **Analytical** (a tested and documented star schema, marts, and time series, in SQL), **Feature** (point-in-time ML inputs), **Serving** (tables shaped for the API).

Python does ingest, quality, conforming, features, ML, and streaming. SQL (dbt) does the analytical model. TypeScript does the API and the web application. The two languages never call each other; they meet at ClickHouse tables, Kafka topics, and schemas generated from one source.

## What you will see

Ten pages: Overview (opens with the KPI tree), Customers, Products, Sellers and Logistics, Satisfaction, Forecasting, Real-Time, Models, Data Quality, and **Insights** (the case-study memos with live charts). The dashboards are coded, not built in a BI tool. Each page names its audience, compares periods, and drills down.

Every chart has an **evidence card**: the metric definition, the grain, the date coverage, the row count, and the caveats.

The public demo on Vercel runs in **snapshot mode**: precomputed aggregates and a recorded real-time replay, with a clear label. The full live system (Kafka, ClickHouse, the stream processor) runs locally with Docker Compose.

## Design principles

1. **Start from a decision.** Every analysis names a stakeholder, the decision it supports, and the branch of the KPI tree it moves. It ends with a recommendation and a sized impact, not with a chart.
2. **Statistics with discipline.** Effect sizes and confidence intervals, rank-based tests for ordinal review scores, multiple-comparison control, checks inside the main strata, and "associated with" in place of "causes".
3. **No technology without a problem.** The data is small, so there is no Spark and no Airflow. Each tool in the stack has a written reason and an entry phase.
4. **Honest analytics.** Each module has a feasibility gate measured on the real data. When the data cannot support an analysis (for example, most Olist customers buy only once), the platform publishes that finding and does not force a chart.
5. **No leakage.** Each model has a written prediction time. Features use only what was known at that time. A test fails when a later field enters a feature set.
6. **Replay-safe events.** Event identifiers are deterministic and sinks are idempotent. A retry or a second replay does not change a total.
7. **One contract, end to end.** Pydantic models generate JSON Schema, which generates the TypeScript validators. CI fails on drift.
8. **Reproducible.** Each memo reruns to the same numbers. Each pipeline run writes a manifest (input hashes, code revision, versions, row counts). Publication to the serving store is atomic.

## Stack

| Layer | Technology |
|---|---|
| Data processing | Python, Polars, PyArrow, Parquet, Pandera |
| Analytical model | SQL with dbt Core + DuckDB: star schema, marts, tests, documentation, lineage |
| Analysis | Jupyter, SciPy, statsmodels, lifelines |
| Mining and ML | scikit-learn, mlxtend, LightGBM, XGBoost, statsmodels, MLflow |
| Streaming | Apache Kafka (KRaft, one node), Python stream processor |
| Serving store | ClickHouse (PostgreSQL only as the metadata database for MLflow and Dagster) |
| API | TypeScript, Hono, Zod, OpenAPI |
| Web | Next.js, TypeScript |
| Orchestration | Python CLI, then Dagster |
| Operations | Docker Compose profiles, GitHub Actions, Prometheus, structured logs |

## Roadmap

Each phase ships an engineering deliverable and an analyst deliverable.

- [ ] **P0** Foundation and data profiling; business frame, KPI tree, analysis standards
- [ ] **P1** Vertical slice: raw files to one live dashboard page, first dbt models, first Vercel deploy; memo M0 (state of the marketplace)
- [ ] **P2** Full star schema and dashboards; memos M1-M5 (late delivery, delivery promise, sellers, categories, installments)
- [ ] **P3** Data mining: RFM, segmentation, association rules, anomaly detection; memos M6-M7 (repeat purchase, segments with actions)
- [ ] **P4** Forecasting and MLflow; memo M8 (planning forecast, A/B test design) - **v1.0 public release**
- [ ] **P5** Delivery and satisfaction models; memo M9 (drivers of late delivery, risk-based intervention)
- [ ] **P6** Real time: simulator, Kafka, stream processor, WebSocket
- [ ] **P7** Dagster orchestration and model promotion flow
- [ ] **P8** Observability, recovery tests, second-source adapter proof, benchmark report
- [ ] **P9** Optional: Portuguese review NLP, seller marketing funnel

Each phase has an exit test in the master plan. Setup and run instructions arrive with Phase 0 and Phase 1.

## Documents

| File | Read it for |
|---|---|
| `docs/MASTER_PLAN.md` | Decisions, the analyst track, phases with exit tests, risks |
| `docs/ARCHITECTURE.md` | The full design: data products, star schema, analysis practice (section 8.4), contracts, real-time path, ML rules, runtime |
| `docs/source/PROJECT_SPEC_v0.md` | The original specification, kept as written |
| `docs/source/ASTRA_ANALYSIS.md` | The independent second review of that specification |

## What this project is not

- Not a food-and-beverage dataset project. Olist is general e-commerce.
- Not a commercial product. See the license note below.
- Not a causal study. The platform reports observed relations and predictions, and says so on the page.
- Not a big-data showcase. The data is small; the analysis and the engineering discipline are the point.
- Not a BI-tool project. The dashboards are coded on purpose.

## Data and license

The data is the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) on Kaggle, published under CC BY-NC-SA 4.0. The raw data is **not** in this repository; a script downloads it with your own Kaggle account. Derived aggregates in the public demo keep the same license and carry the attribution. This project is non-commercial while it uses this dataset.

The code license will be added before the first public release.

## Author

Line
