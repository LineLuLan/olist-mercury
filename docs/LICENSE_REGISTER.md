# License and Cost Register

Checked: 2026-09-21. Rule: before a new dependency or service enters the project, add a row here with a source link and the check date. "Free" means no paid license for this project's use: personal, educational, non-commercial.

## 1. Short answer

Every piece of software in the design is free and open source. Three free items have conditions (section 3). Nothing in the project needs a credit card.

## 2. Software

| Component | License | Source checked |
|---|---|---|
| Python, Polars, PyArrow, DuckDB, Pandera, SciPy, statsmodels, scikit-learn | Permissive (PSF, MIT, Apache 2.0, BSD) | Well-known; confirm each exact license when `uv.lock` is first created (P0) |
| dbt Core | Apache 2.0 | getdbt.com/licenses-faq |
| dbt DuckDB adapter | To verify in P1 (also the dbt Core version it supports) | github.com/duckdb/dbt-duckdb |
| lifelines | MIT | github.com/lifelines/lifelines |
| LightGBM, XGBoost, mlxtend | Permissive; confirm in P3-P4 | Project repositories |
| MLflow | Apache 2.0 | mlflow.org |
| ClickHouse (self-hosted server) | Apache 2.0 | github.com/ClickHouse/ClickHouse LICENSE |
| PostgreSQL | PostgreSQL License (permissive) | postgresql.org |
| Apache Kafka | Apache 2.0 | kafka.apache.org |
| Quix Streams | Apache 2.0 | github.com/quixio/quix-streams LICENSE |
| Dagster (open-source package) | Apache 2.0 | github.com/dagster-io/dagster LICENSE |
| Prometheus | Apache 2.0 | prometheus.io |
| Grafana OSS (optional) | AGPLv3. Free. If you change Grafana itself and offer it over a network, you must publish those changes. Unchanged use is fine. | grafana.com |
| Node.js, TypeScript, Hono, Zod, Next.js, Tailwind, Radix, shadcn/ui, Recharts | MIT or similar permissive; confirm when `pnpm-lock.yaml` is first created (P1) | Project repositories |
| Docker Engine, Docker Compose | Apache 2.0 | docker.com |

Not used, on purpose: ClickHouse Cloud, Dagster+, dbt platform (cloud), Quix Cloud, Confluent Cloud, MLflow managed services. Each of these tools has a paid hosted product. The project uses only the self-hosted open-source package.

## 3. Free with conditions

| Item | Condition | Effect on this project |
|---|---|---|
| **Docker Desktop** | Free for personal use, education, non-commercial open source, and companies with fewer than 250 employees and less than 10 million USD revenue. Larger companies need a paid plan. (docs.docker.com/subscription/desktop-license) | Free for Line and for students. A user at a large company can run the same Compose files with Docker Engine inside WSL2 or on Linux, which is free for everyone. |
| **GitHub Actions** | Public repository: free, no minute limit on standard runners. Private repository on the Free plan: 2,000 minutes per month. (docs.github.com, Actions billing) | The repository is private until v1.0, so CI must stay lean: cache dependencies, run the heavy integration job only on pull requests to `main`. |
| **Vercel Hobby** | Free, non-commercial and personal use only. Usage limits pause the project when exceeded. (vercel.com/docs/plans/hobby) | Fits the snapshot demo. No commercial use. |
| **Olist dataset** | CC BY-NC-SA 4.0: attribution, non-commercial, share-alike. A free Kaggle account is needed for the download. (kaggle.com/datasets/olistbr/brazilian-ecommerce; Line confirms on the page) | Raw data never enters git. Derived aggregates carry the attribution and the same license. |

## 4. Costs that are not software

- Your computer, electricity, and time.
- A Linux server, only if you later host the live backend in public. The design does not need one: the public demo is the Vercel snapshot.
- A custom domain name, optional.
- AI coding assistants used during development are a personal tooling choice. The project does not need them to build or run.
