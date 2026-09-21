# Findings

**F1 — BLOCKER — Event contracts do not establish replay correctness. S13, S35–40, S63.**  
A UUID field does not guarantee stable identifiers across retries. The spec omits sink idempotency, ordering, checkpoints, late-event handling, and replay isolation. Historical timestamps cannot simply become current UTC timestamps. Olist timestamps lack explicit time zones; the correct interpretation is **to verify**. Accelerated replay can manufacture apparent spikes if metrics use processing time. Retrying events can inflate revenue.

**F2 — BLOCKER — Prediction time and feature availability are not executable contracts. S15, S30–33, S41, S76.**  
S32 permits delivery delay and delivery duration as satisfaction features without defining when prediction occurs. These are unavailable at order creation. Full-history Customer 360 and seller aggregates can leak future outcomes. Historical replay must reveal fields only when they become available. A final order record is not a valid creation-event payload.

**F3 — MAJOR — Customer and basket analyses need feasibility gates. S8, S16–18, S26.**  
Expect repeat purchasing to be sparse and many baskets to contain only one distinct product; both hypotheses are **to verify against the data**. Frequency may provide little segmentation power. Repeated item rows can represent multiple units of one product, not useful product combinations. Category baskets may help but cannot guarantee useful rules. Observed customer activity is not lifetime value. Unobserved cohort periods must remain unknown.

**F4 — MAJOR — Forecast coverage and density are unproven. S28–30, S40.**  
The stated 2016–2018 span does not establish complete daily coverage. Profile sparse boundary months, extraction cutoffs, missing intervals, and hourly/category density. These are **to verify against the data**. Missing coverage is not zero demand. Five-minute dashboards are replay demonstrations, not evidence of production traffic. Sales measure observed purchases, not unconstrained demand; inventory and stockout information are absent from the specified sources.

**F5 — MAJOR — Table grain and business definitions can corrupt metrics. S8, S11–15, S19–24, S63.**  
Items, payments, reviews, and geolocation can multiply rows when joined directly. Review identifiers and reviews per order require profiling; neither should be assumed unique. Multiple payments per order can be valid. ZIP-prefix coordinates need consolidation, not arbitrary duplicate removal. Null categories need an explicit unknown category. All occurrence counts are **to verify against the data**.

“Revenue” could mean item sales, item sales plus freight, or payment totals. It cannot establish profit or platform revenue without additional definitions. Order-level reviews and delivery outcomes cannot reliably identify the responsible seller in a multi-seller order. ZIP-prefix distance is approximate, not route distance.

**F6 — MAJOR — The simulator promises events not fully observed by the source. S35–36, S40–41.**  
The listed payment data does not establish a payment-created timestamp. Approval is not automatically payment creation. Review creation and answer timestamps need interpretation. Carrier handoff is a proxy for shipment. Final order status does not reconstruct every cancellation transition. Synthetic timing must be labelled. Otherwise active-order counts and lifecycle latency appear more authoritative than the evidence allows.

**F7 — MAJOR — The default infrastructure exceeds demonstrated needs. S43–59, S68, S84.**  
Spark batch processing does not yet earn its place at this dataset scale. Airflow lacks a demonstrated scheduling requirement. PostgreSQL lacks a concrete multi-user transactional workload. SeaweedFS, Iceberg, Grafana, and a permanently running MLflow server are unnecessary initially. ClickHouse earns a conditional place for the intended analytical serving demonstration; benchmark it. Next.js and FastAPI earn separate roles only if the application remains thin.

The undefined **Realtime Store**, durable stream state, checkpoint ownership, and ClickHouse publication path are missing design decisions. They do not necessarily require additional services.

**F8 — MAJOR — Evaluation policy is too generic. S17, S25, S27, S29–32, S75–76.**  
Temporal splits, label maturation, class imbalance, unseen entities, and model-specific baselines are unspecified. No numeric performance promise is defensible before profiling and backtesting. A baseline may win. Detailed evaluation contracts appear below.

**F9 — MAJOR — Operations specify checks without consequences. S9–11, S53–54, S61–66, S73, S77.**  
“Report” and “reject” do not define fail, quarantine, or warn behavior. Version strings alone do not establish lineage. Missing controls include atomic publication, crash recovery, resource limits, disk retention, security boundaries, and reproducible dependency resolution. Compose does not supply datasets, trained models, or initialized schemas automatically.

**F10 — MAJOR — Licensing and cost assertions exceed the evidence. S5, S34, S45–54, S69–71.**  
The supplied Kaggle summary makes CC BY-NC-SA 4.0 **very likely, to confirm on the page**. It is not unconditional permission for commercial reuse. Dataset derivatives, redistribution, and future business use require review. Exact software, image, dependency, Docker Desktop, and CI terms are **to verify**. Statements about competing products’ current licenses are unsupported within this review. No paid service does not mean zero electricity, storage, or maintenance cost.

**F11 — MAJOR — Delivery order and lifecycle guidance conflict. S25, S34, S49–50, S56–57, S77–81, S84.**  
S78 postpones serving and frontend until phases five and six. S84 calls for that vertical slice first. S77 implies one-command availability that conflicts with selective profiles and prerequisite setup. Optional NLP becomes an unqualified final output in S81. S34 uses deprecated registry stages; the supplied facts specify aliases and tags as the replacement.

**F12 — MINOR — Names and layouts need one authoritative inventory. S1, S10, S49–50, S60, S67, S82–83.**  
The title and repository name differ from Olist Mercury. Page lists disagree about Models, Data Quality, and separate seller/logistics pages. Raw layouts differ; Serving is absent from S60. Ingestion and streaming directories overlap. These ambiguities invite duplicated ownership.

# Proposed corrections

| Finding | Concrete correction |
|---|---|
| F1 | Define a versioned event envelope and a replay/recovery acceptance contract before streaming implementation. |
| F2 | Require an availability-time matrix and point-in-time feature tests for every model. |
| F3 | Gate retention, clustering, and association outputs on measured sample support; permit “insufficient evidence.” |
| F4 | Publish a coverage calendar and eligible-series manifest; start with daily aggregate forecasting. |
| F5 | Approve table grains, identity mappings, join cardinalities, and a metric dictionary before aggregation. |
| F6 | Publish a source-to-event evidence matrix distinguishing observed, inferred, synthetic, and unavailable events. |
| F7 | Adopt the bounded profiles below and assign every state store and publication step an owner. |
| F8 | Adopt the model-specific evaluation contracts below; promotion requires measured value over baseline. |
| F9 | Implement a run manifest, quality disposition policy, atomic publication, and recovery test suite. |
| F10 | Create a dated license register before distributing data, artifacts, or deployment images. |
| F11 | Replace competing phase sequences with the vertical plan below; use model aliases and tags. |

For F12, normalize the name, page inventory, and repository layout in the master plan.

# Recommended target architecture

Recommend **R2 for the initial implementation**, with medium confidence pending Line’s interpretation. Preserve **Raw, Conformed, Analytical, Feature, Serving** as logical products, not five mandatory databases.

The weighted comparison uses provisional priorities: laptop fit 40%, correctness 30%, delivery effort 20%, distributed-systems learning 10%. Scores are engineering judgments, not measurements.

| Option | Fit | Correctness | Effort | Learning | Weighted /5 |
|---|---:|---:|---:|---:|---:|
| R1: PySpark qualifies | 2 | 4 | 2 | 5 | 2.9 |
| R2: no JVM | 5 | 3 | 5 | 2 | 4.1 |

R1 provides established streaming machinery but adds runtime and integration work. R2 reduces infrastructure but requires careful bounded state and recovery logic. A dominant distributed-systems learning objective could reverse the choice.

**R1 — Python includes PySpark.** Use PySpark batch transformations and Structured Streaming with Kafka. Load ClickHouse through bounded Python batches inside `foreachBatch`, with deterministic batch identities and sink-level duplicate protection. Avoid collecting unbounded datasets into the driver. Spark checkpoints alone do not prove sink exactly-once behavior. Skew risk comes from Spark versus inference-library differences in nulls, decimals, timestamps, and encodings; test offline/online feature parity.

**R2 — Python processing, no JVM anywhere in the data path.** Use Polars with PyArrow for batch work. Use a bounded Python event processor with an embedded SQLite durable journal, state, and outbox. Kafka is JVM-based and therefore also excluded under this strict reading. A Python Kafka consumer would not satisfy it. This is a single-node replay system, not a distributed broker replacement.

Use a Python ClickHouse client for bounded bulk loads. Share transformation and feature definitions between batch and streaming, with parity tests. Native extensions are assumed acceptable; literal interpreter-only Python would also exclude much of the proposed numerical stack.

**Batch flow:** source files → immutable Raw files and checksums → source adapter → canonical Conformed Parquet → Analytical marts and point-in-time Feature snapshots → model evaluation → versioned Serving tables in ClickHouse → FastAPI → Next.js.

**Real-time flow under R2:** simulator or future adapter → durable Raw event journal → validation → shared canonical transformations → transactional deduplication/state/outbox → ClickHouse Serving snapshots → FastAPI WebSocket → browser.

The **Realtime Store is ClickHouse Serving tables**. SQLite owns ingestion state, not dashboard analytics. Publish versioned absolute snapshots rather than blindly adding retried deltas. Queries must explicitly select one latest version; background merges must not determine correctness. Keep historical batch and replay runs isolated to prevent double counting.

Use deterministic identifiers derived from source namespace, stable record key, event type, and revision. Keep replay-run identity separate. Namespace customer identifiers and map Olist customer identity through `customer_unique_id`.

Represent money as integer minor units or fixed-scale decimal, including currency. Preserve original timestamp text, documented timezone assumptions, event time, ingestion time, and processing time. Replay speed changes wall-clock scheduling only. Use event-time windows, explicit watermarks, allowed lateness, and a replay clock.

Invalid events enter a durable dead-letter record with reason and original payload. Permit additive optional fields within a compatible contract; breaking changes require a new version and adapter tests.

**Compose profiles — total application RAM estimates, excluding Windows/WSL overhead:**

| Profile | Components | Estimated RAM |
|---|---|---:|
| Core | ClickHouse, API, production frontend | 2–3.5 GB |
| Batch | Core plus one Python worker | 3.5–6 GB |
| Streaming R2 | Core plus simulator/processor | 3–5 GB |
| Training | Core stopped; training plus optional MLflow | 3–6 GB |
| Streaming R1 alternative | Core, single-node KRaft Kafka, local Spark | 6–9 GB |

These are estimates, not measurements. Reserve roughly 6 GB for the host and tools. Avoid simultaneous training and streaming. Do not offer an unrestricted Full profile. Actual limits and latency require measurement.

Repository layout: `apps/api`, `apps/web`, `src/mercury/{adapters,contracts,quality,transforms,features,batch,streaming,serving,ml}`, `tests/{unit,data,contract,integration,recovery}`, `infra`, `docs`, and ignored `data/{raw,conformed,analytical,feature,serving}`. Keep contracts outside runtime data.

# ML and operational acceptance contracts

For **F2/F8**, define these prediction boundaries:

- **Forecasting:** forecast after a completed daily cutoff. Shift rolling features. Use rolling-origin backtests, seasonal-naive baselines, MAE and MASE; report horizon-specific errors and interval coverage. Handle zero denominators explicitly.
- **Delivery:** predict at order creation using available fields. Define delay against the promised date and calendar-day policy. Train only on matured labels; describe exclusion bias for canceled or undelivered orders. Compare median-delay and simple linear baselines using MAE, RMSE, and late-order errors.
- **Satisfaction:** recommend post-delivery prediction before review availability. If predicting at creation, remove actual delivery features. Compare majority/prior and logistic baselines using macro-F1, negative-class precision/recall, PR-AUC, and calibration. Evaluate only eligible cases and report missing-review selection bias.
- **Segmentation:** use historical as-of snapshots; fit scaling on training data. Compare with simple RFM groups. Check stability, size, and interpretation, not silhouette alone. Do not promise meaningful loyalty clusters.
- **Anomalies:** score against prior observations. Compare seasonal thresholds; measure alert rate and detection on labelled injected scenarios. Synthetic results do not establish real fraud accuracy.
- **NLP, if added:** predict from text at review availability. Rating-derived labels are weak supervision. Use a manually checked Portuguese sample and class-aware metrics.

Keep all rows from an order together. Use chronological validation and a final untouched holdout, allowing labels to mature. Report returning/unseen customer and seller performance separately. Persist fitted preprocessing with the model. No improvement over baseline is an acceptable outcome.

For **F9**, fail publication on incompatible schemas, broken required keys, or failed reconciliation. Quarantine malformed records. Warn on tolerated null categories and expected lifecycle omissions. Retain previous Serving snapshots after failure.

Manifests should record input hashes, code revision, dependency lock, configuration, time cutoffs, transformation versions, counts, and model artifacts. Test join cardinality, money reconciliation, event replay, crash recovery, lateness, and feature parity.

CI should use synthetic fixtures, linting, unit/contract tests, and a small integration profile. Publishing remains a separate authorized action. Pin resolved dependencies and image digests. Verify Python 3.13 compatibility rather than assume it. Prefer WSL Linux storage for working data and container volumes; document Windows entry points.

Bind local services to localhost. Keep databases private. Require authentication, authorization, and TLS before remote exposure. Bound WebSocket queues and reconnect behavior. Restrict model/artifact loading to trusted outputs.

# Enhancements

Ranked by estimated value divided by effort; these extend the corrections:

1. **Evidence card per chart:** prevents unsupported claims through visible grain, coverage, and caveats.
2. **Deterministic demo command:** removes manual preparation during the defense.
3. **Synthetic POS adapter fixture:** tests canonical portability without company data.
4. **Replay pause/seek controls:** makes event-time behavior explainable.
5. **Resource benchmark report:** supports CV claims with measured throughput, RAM, and latency.
6. **Model abstention/fallback display:** handles missing features and unseen entities honestly.

# Recommended phase plan

1. **Vertical slice first.** Profile orders/items, define revenue, and connect Raw → Conformed → Analytical → Serving → API → one page. Done when totals reconcile and reruns leave results unchanged.
2. **Analytical foundation.** Add customer, product, logistics, quality results, and coverage reporting. Done when joins cannot inflate totals and each metric has an approved definition.
3. **One forecasting product.** Add Feature snapshots, backtests, and baseline comparison. Done when a clean run reproduces holdout results and the dashboard displays limitations.

**MVP cut line: stop after phase 3.** Describe it as batch analytics and forecasting.

4. **Replay system.** Add durable events and live metrics. Done when duplicates, crashes, and late arrivals preserve correct totals and simulation is visibly labelled.
5. **One additional model.** Prefer delivery prediction. Done when prediction-time features, baseline comparison, and fallback behavior pass.
6. **Defense and deployment hardening.** Done when a clean-machine run, restore test, license register, resource report, and security configuration are reproducible.

Introduce logs and tests from phase 1. Add observability services only when they solve a demonstrated diagnostic need.

# Open questions for Line

Recommended options appear first:

1. Python meaning: **Python with native libraries, no JVM**; PySpark permitted; Python processors with Kafka permitted.
2. Main objective: **defensible analytics and engineering correctness**; distributed-stack practice; maximum module breadth.
3. MVP boundary: **batch plus daily forecasting**; include replay; include replay and online inference.
4. Satisfaction timing: **post-delivery, before review**; order creation; descriptive analysis only.
5. Revenue definition: **item sales excluding freight, with eligibility rules**; gross order value; payment totals.
6. Data distribution: **download instructions plus synthetic fixtures**; redistribute only after license confirmation; private artifacts only.
7. Exposure: **localhost initially**; private Linux server; authenticated public demonstration.

Static review only. Read all 84 sections on 2026-09-21; reopened S13, S32, S78, and S84 to confirm citations. Used supervisor-supplied facts dated 2026-09-21. No web research, dataset inspection, execution tests, or file changes. Main uncertainty is dataset feasibility and measured resource use. Rollback is previous versioned Serving data and model artifacts. Next task: reconcile this analysis, resolve Python scope, then profile the source data.

usage: unavailable in current runtime

C1..C8: met / met / met / met / met / met / met / met