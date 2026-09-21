GOAL: Produce an independent, critical engineering analysis of the project specification in docs/source/PROJECT_SPEC_v0.md (project name: Olist Mercury), so that the supervisor can reconcile it with a second analysis and write the master plan and the architecture document.

BUSINESS REASON: Line builds this platform alone on one laptop. It must be defensible in a project defense, credible on a CV, and later extendable to real Chat Order / POS data. A wrong architecture or an unrealistic analytic promise costs weeks of rework. Your analysis is the second mind; the supervisor (a different model) writes its own analysis in parallel and has NOT shared its conclusions with you on purpose. Do not try to guess them. Disagree with the spec where the evidence supports it.

SUCCESS CRITERIA:
1. Every finding cites the spec section number (for example "S29") and has a severity: BLOCKER, MAJOR, MINOR.
2. Findings cover all of these lenses, with at least one explicit statement per lens (a statement of "no material issue" is allowed, with the residual risk):
   a. Dataset reality: which promised analyses the Olist data can and cannot support (think about repeat-purchase rate, items per order, hourly density, the date range edges, duplicate keys in reviews / geolocation / payments, null categories, timestamps without a time zone). Give approximate numbers only when you are confident, and mark each number "to verify against the data".
   b. Internal contradictions in the spec (phase order, principles against technology choices, names, page lists, folder layout).
   c. Technology fit for a single 16 GB laptop (see FACTS). Name each service that does not earn its place, and each service that is missing (for example: what exactly is the "Realtime Store"?).
   d. Python-first data processing (see CONSTRAINTS). Analyze BOTH readings: (R1) PySpark counts as Python; (R2) pure Python with no JVM in the data path. For each reading give the batch engine, the stream processor, the ClickHouse load path, and the train/serve feature-skew risk.
   e. Event and data contracts: idempotency, deterministic event identifiers, money type, event time against processing time, replay-speed policy for the simulator, dead-letter handling, schema evolution.
   f. ML validity: leakage, prediction-time definition for each model, split strategy, class imbalance, metric choice, baseline policy, realistic expected performance.
   g. Operations: data-quality failure policy (fail, quarantine, warn), lineage and versioning, testing, CI, security, licensing (dataset and software), reproducibility on Windows 11 + WSL2 + Docker Desktop.
3. A section "Proposed corrections" that maps each BLOCKER and MAJOR finding to one concrete correction.
4. A section "Enhancements" with at most 10 additions that raise the quality of the project, ranked by value divided by effort. Each one states the problem it solves. Do not add a technology that solves no named problem.
5. A section "Recommended target architecture" in text form: components, data flow for the batch path and the real-time path, Docker Compose profiles with a RAM estimate per profile (mark estimates as estimates), and the repository layout.
6. A section "Recommended phase plan": vertical slice first, with a done-when condition per phase, and an explicit MVP cut line.
7. A section "Open questions for Line": at most 8, each with 2 to 4 options and your recommended option first.
8. No version, price, or license claim from memory. Use FACTS below, or write "to verify".

FILES:
- Read: docs/source/PROJECT_SPEC_v0.md (the full spec, 84 sections, about 3,250 lines; most lines are short).
- Create or modify: nothing. The sandbox is read-only. Your final message IS the deliverable.

FACTS (checked by the supervisor on 2026-09-21 from primary sources unless marked):
- Machine: Windows 11 Pro, AMD Ryzen 7 7840H (16 logical processors), 15.2 GB usable RAM, Docker Desktop, WSL2, Python 3.13, uv installed. The workspace is empty except docs/source. It is not yet a git repository.
- Apache Kafka 4.3.1 was released on 2026-06-25 (kafka.apache.org). Kafka 4.x runs in KRaft mode without ZooKeeper.
- Apache Spark 4.2.0 was released on 2026-07-14 (spark.apache.org). Spark 4 is built with Scala 2.13.
- The Kaggle page of the Olist dataset states the license CC BY-NC-SA 4.0 (non-commercial, share-alike, attribution). Source: search result summary of the Kaggle page; treat as "very likely, to confirm on the page".
- MLflow: Model Registry stages (Staging, Production, Archived) are deprecated since MLflow 2.9. Model version aliases (for example @champion, @challenger) and tags are the replacement (mlflow.org docs).
- Line's standing requirement: no paid service; everything runs locally or on a self-managed Linux server.

CONSTRAINTS:
- Line's new requirement, verbatim intent: "the data process should be Python". The exact meaning is being clarified with Line now. Until then, analyze both readings R1 and R2 (criterion 2d) and say which one you recommend and why.
- Keep the five data-product names of the spec: Raw, Conformed, Analytical, Feature, Serving.
- Keep the canonical-model idea: a source adapter in front of one shared downstream pipeline.
- Write in plain technical English, short sentences, in the style of the spec.
- Length target: 1,500 to 2,500 words. Findings first, worst first.

NON-GOALS:
- Do not write code, Docker files, or the final master plan and architecture documents. The supervisor writes those.
- Do not restate the spec. Do not praise it. Omit non-issues.
- Do not research the web. Mark unknowns "to verify".

DONE WHEN: All 8 success criteria are met in one final message, and each of the 7 lenses in criterion 2 has an explicit statement.

VALIDATE: Before you finish, check your own output against criteria 1 to 8 and print a one-line checklist "C1..C8: met / not met" at the end. Spot-check three of your section citations against the file (open the cited section and confirm it says what you claim).

STOP RULES: Missing prerequisite (for example the spec file cannot be read) - report, do not guess. Ambiguous requirement - state the ambiguity under "Open questions for Line" and continue with both readings. Unsure about a dataset number - mark it "to verify", do not invent precision.

ROLLBACK: None needed. Read-only run; no file changes.
