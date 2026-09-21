Below is the **full project specification** in English. I use **ASD-STE100-style technical English**: short sentences, controlled vocabulary, clear terms, and limited idioms.

I also changed the technology choices to match your requirement: **no paid cloud service is required**. The stack can run locally with Docker. I use **Apache Kafka instead of Redpanda** because Kafka is an Apache open-source project. This avoids a licensing ambiguity for a project that you may later publish or deploy. Kafka is an open-source event-streaming platform, and the current official release page lists Kafka 4.3.1. :chatgpt-content-reference{index="0"}

# REAL-TIME COMMERCE INTELLIGENCE PLATFORM

## 1. Project Title

**Real-Time Commerce Intelligence Platform**

### Subtitle

**An End-to-End Data Platform for Customer Analytics, Data Mining, Machine Learning, Forecasting, and Real-Time Order Intelligence**

---

# 2. Project Summary

This project develops a production-oriented commerce intelligence platform.

The platform uses the **Brazilian E-Commerce Public Dataset by Olist** as the main historical data source.

The Olist dataset contains about 100,000 orders from 2016 to 2018.

The data includes:

- Orders
- Customers
- Products
- Sellers
- Order items
- Payments
- Reviews
- Geolocation
- Product category translation

The data is real commercial data and is anonymized.

The dataset is available on Kaggle through the Olist organization. Kaggle currently shows the dataset with a Gold Medal and a usability score of 10.0.

The platform has two main data paths:

1. **Batch data path**
2. **Real-time data path**

Both paths use a shared **canonical data model**.

The system can therefore start with Olist data and later accept data from:

- Chat Order
- POS
- CRM
- Payment systems
- Other order systems

The project does not treat Olist as an F&B dataset.

Olist is an e-commerce dataset.

The project uses Olist as the historical transaction source for a generalized commerce and order intelligence architecture.

---

# 3. Main Objective

The main objective is to build a complete data platform that can answer four levels of questions.

## Level 1: What happened?

Use data analytics.

Examples:

- How many orders were created?
- How much revenue was generated?
- Which products generated the most revenue?
- Which states generated the most orders?
- Which sellers had delivery problems?

## Level 2: Why did it happen?

Use data mining and statistical analysis.

Examples:

- Which customer groups have similar behavior?
- Which products are frequently purchased together?
- Which factors are associated with poor reviews?
- Which sellers have unusual behavior?

## Level 3: What will happen?

Use machine learning and time-series forecasting.

Examples:

- What will the order volume be tomorrow?
- What will the revenue be next week?
- Will an order have a delivery delay?
- What customer satisfaction score can be expected?

## Level 4: What is happening now?

Use real-time data processing.

Examples:

- How many orders arrived in the last five minutes?
- Is there an unusual order spike?
- What is the current revenue rate?
- What is the predicted delivery delay for a new order?

---

# 4. Main Design Principle

The platform uses this data lifecycle:

```text
DATA SOURCES
      |
      v
INGESTION
      |
      v
RAW DATA
      |
      v
DATA QUALITY
      |
      v
CONFORMED DATA
      |
      +------------------+
      |                  |
      v                  v
ANALYTICAL DATA      FEATURE DATA
      |                  |
      v                  v
BI / ANALYTICS       ML PIPELINES
      |                  |
      |                  v
      |             MODEL REGISTRY
      |                  |
      +--------+---------+
               |
               v
          SERVING DATA
               |
          +----+----+
          |         |
          v         v
       REST API  WebSocket
          |         |
          +----+----+
               |
               v
           WEB APP
```

The project does not use the terms "Bronze", "Silver", and "Gold" as the main architectural names.

The main names are:

- **Raw Data**
- **Conformed Data**
- **Analytical Data**
- **Feature Data**
- **Serving Data**

These names describe the purpose of each data product.

---

# 5. Source Data

## 5.1 Main Dataset

**Brazilian E-Commerce Public Dataset by Olist**

Source:

[Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?utm_source=chatgpt.com)

The dataset contains about 100,000 orders from 2016 to 2018.

The data was generated from real commercial operations and was anonymized before publication.

The dataset contains nine CSV files.

The main tables are:

1. Customers
2. Geolocation
3. Order items
4. Payments
5. Reviews
6. Orders
7. Products
8. Sellers
9. Product category translation

The dataset provides information about:

- Order status
- Order time
- Product price
- Freight value
- Customer location
- Product attributes
- Seller location
- Payment method
- Review score
- Review text



---

# 6. Optional Olist Marketing Dataset

The project can also use the:

**Marketing Funnel by Olist**

Source:

[Olist Marketing Funnel Dataset](https://www.kaggle.com/datasets/olistbr/marketing-funnel-olist?utm_source=chatgpt.com)

This dataset contains about 8,000 marketing-qualified leads.

It covers seller acquisition activity from June 2017 to June 2018.

The dataset can be connected to the main Olist dataset through `seller_id`.

This dataset is optional.

The core project does not depend on it.

If it is added, the project can include:

```text
Marketing Lead
      |
      v
Seller Acquisition
      |
      v
Seller
      |
      v
Products
      |
      v
Orders
      |
      v
Revenue
```

This creates an additional **Seller Acquisition Intelligence** module.

---

# 7. Data Source Policy

The project uses one primary data ecosystem.

This has several advantages:

- Consistent entity relationships
- Known identifiers
- Clear join paths
- Less risk of incompatible definitions
- Easier data validation
- Easier explanation during project defense
- Easier reproducibility

The project does not require external weather, Yelp, or food datasets.

External data can be added later as enrichment.

The core platform must work without external enrichment.

---

# 8. Olist Data Model

The main relationship is:

```text
CUSTOMER
    |
    v
ORDER
    |
    +----------------+
    |                |
    v                v
ORDER ITEM        PAYMENT
    |
    +-------------+
    |             |
    v             v
PRODUCT         SELLER
    |
    v
CATEGORY

ORDER
    |
    v
REVIEW

CUSTOMER
    |
    v
GEOLOCATION
```

Olist uses `customer_unique_id` to identify a customer across multiple orders.

This is important for repeat-purchase analysis.

The Olist metadata explains that `customer_id` is assigned per order, while `customer_unique_id` can identify customers who made repeat purchases.

---

# 9. Data Ingestion

The first technical layer is the **Ingestion Layer**.

Its responsibilities are:

- Read source files
- Validate file availability
- Validate schema
- Record ingestion time
- Record source version
- Store the original data
- Detect ingestion failures

The ingestion process must not modify the original source data.

Example:

```text
Olist CSV
   |
   v
Ingestion Service
   |
   +--> Schema Validation
   |
   +--> Metadata
   |
   v
Raw Data Store
```

---

# 10. Raw Data

Raw Data preserves source information.

The system should keep the source structure as much as possible.

Example:

```text
data/
└── raw/
    └── olist/
        ├── orders/
        ├── customers/
        ├── order_items/
        ├── payments/
        ├── reviews/
        ├── products/
        ├── sellers/
        ├── geolocation/
        └── categories/
```

Each ingestion batch should contain metadata.

Example:

```text
_source
_source_file
_ingestion_timestamp
_batch_id
_schema_version
```

Raw data should be treated as immutable.

This allows pipeline replay.

If a downstream transformation fails, the system can process the same raw data again.

---

# 11. Data Quality Layer

Data quality is a required part of the project.

The project must not assume that the dataset is clean.

The system measures data quality before transformation.

The quality checks include:

## Schema Validation

Check:

- Column names
- Data types
- Required columns
- Unexpected columns

## Completeness

Measure:

- Null count
- Null percentage
- Missing timestamps

## Uniqueness

Check:

- Duplicate order IDs
- Duplicate customer IDs
- Duplicate product IDs
- Duplicate review IDs

## Referential Integrity

Examples:

```text
order_items.order_id
        |
        v
orders.order_id
```

and:

```text
order_items.product_id
        |
        v
products.product_id
```

and:

```text
order_items.seller_id
        |
        v
sellers.seller_id
```

## Domain Validation

Examples:

```text
price >= 0
freight_value >= 0
payment_value >= 0
review_score between 1 and 5
```

## Timestamp Validation

Check the order lifecycle.

Example:

```text
purchase
   |
   v
approval
   |
   v
carrier
   |
   v
delivery
```

Invalid sequences must be reported.

---

# 12. Conformed Data

Conformed Data is the central data layer.

It contains standardized entities.

The main entities are:

```text
Customer
Order
OrderItem
Product
Seller
Payment
Review
Location
Category
```

This layer defines the shared data contract.

For example:

```text
customer_id
order_id
product_id
seller_id
```

must have stable definitions across the platform.

The purpose is to allow different sources to use the same downstream pipeline.

For example:

```text
Olist
   |
   v
Olist Adapter
   |
   v
Canonical Order Model
```

Later:

```text
Chat Order
   |
   v
Chat Order Adapter
   |
   v
Canonical Order Model
```

Both sources can use the same analytical and ML systems.

---

# 13. Canonical Order Model

The platform should define a canonical order event.

Example:

```json
{
  "event_id": "uuid",
  "event_type": "order.created",
  "event_version": 1,
  "event_time": "2026-09-21T10:30:00Z",
  "order_id": "order-001",
  "customer_id": "customer-001",
  "items": [],
  "total_amount": 100.00,
  "currency": "BRL",
  "source": "olist"
}
```

Later, a Chat Order event can use the same contract.

Example:

```text
source = chat_order
```

The analytical system does not need to know the original source format.

---

# 14. Analytical Data

Analytical Data is optimized for business analysis.

It contains:

- Aggregations
- Business metrics
- Customer profiles
- Product metrics
- Seller metrics
- Delivery metrics
- Time-series tables

Examples:

```text
analytical/
├── customer_360
├── daily_sales
├── hourly_orders
├── product_performance
├── seller_performance
├── delivery_performance
├── customer_retention
└── geographic_performance
```

---

# 15. Customer 360

The system creates one analytical customer profile.

Example fields:

```text
customer_id
first_order_date
last_order_date
order_count
total_spend
average_order_value
average_review_score
average_freight_value
average_delivery_delay
customer_segment
```

This table supports:

- Customer analysis
- RFM
- Segmentation
- Retention
- ML features
- API responses

---

# 16. RFM Analysis

RFM means:

- Recency
- Frequency
- Monetary

## Recency

Number of days since the last order.

```text
Recency =
Reference Date - Last Order Date
```

## Frequency

Number of orders.

```text
Frequency =
Number of Orders
```

## Monetary

Total customer spending.

```text
Monetary =
Total Order Value
```

The system generates RFM scores.

It then creates customer groups.

The group names are project-defined business labels.

Examples:

```text
High Value
Loyal
Recent
Potential
At Risk
Inactive
```

The labels must be defined by the project.

They are not labels supplied by Olist.

---

# 17. Customer Segmentation

Features can include:

```text
recency
frequency
monetary
average_order_value
average_review_score
average_freight
average_delivery_delay
```

Pipeline:

```text
Customer 360
     |
     v
Feature Selection
     |
     v
Scaling
     |
     v
K-Means
     |
     v
Cluster Analysis
     |
     v
Customer Segments
```

Model evaluation:

- Elbow method
- Silhouette score
- Cluster size
- Cluster stability
- Business interpretation

---

# 18. Customer Retention

The platform measures:

- New customers
- Repeat customers
- Repeat purchase rate
- Orders per customer
- Customer lifetime activity
- Cohort retention

Cohort example:

```text
              Month 0  Month 1  Month 2  Month 3
Jan 2017       100%      X%       X%       X%
Feb 2017       100%      X%       X%       X%
Mar 2017       100%      X%       X%       X%
```

The actual values must be calculated from the dataset.

---

# 19. Product Intelligence

The product module measures:

- Product sales
- Category sales
- Product revenue
- Product price
- Freight value
- Review score
- Product dimensions
- Product weight
- Product photo count

Main outputs:

```text
product_performance
category_performance
product_revenue_rank
product_review_analysis
```

---

# 20. Seller Intelligence

The seller module measures:

- Number of orders
- Revenue
- Average order value
- Delivery performance
- Review performance
- Geographic distribution
- Product category distribution

Example:

```text
Seller
   |
   +--> Orders
   +--> Revenue
   +--> Products
   +--> Delivery
   +--> Reviews
```

---

# 21. Logistics Intelligence

Olist contains several order timestamps.

The project can derive:

```text
approval_time
shipping_time
delivery_time
estimated_delivery_time
delivery_delay
```

Example:

```text
delivery_delay =
actual_delivery_date
-
estimated_delivery_date
```

The system can classify orders as:

```text
On Time
Late
Very Late
```

The thresholds must be defined in the project documentation.

---

# 22. Geographic Analytics

The geolocation data provides coordinates for Brazilian ZIP code prefixes.

The project can build:

- Customer distribution map
- Seller distribution map
- Order density map
- State-level revenue
- State-level order volume
- Customer-seller distance analysis

The geolocation dataset is part of the Olist data ecosystem.

---

# 23. Payment Analytics

The payment table supports:

- Payment method analysis
- Installment analysis
- Payment value
- Payment frequency
- Order value by payment type

Example payment types can be analyzed without hard-coding assumptions about future payment systems.

---

# 24. Review Analytics

The review data supports:

- Review score distribution
- Review score by seller
- Review score by product category
- Review score by delivery performance
- Review text analysis

The system must distinguish between:

```text
Observed relationship
```

and:

```text
Causal relationship
```

The project does not claim causality from observational data.

---

# 25. NLP Module

The optional NLP module processes review text.

Pipeline:

```text
Review Text
    |
    v
Text Cleaning
    |
    v
Tokenization
    |
    v
TF-IDF / Embedding
    |
    +----------+
    |          |
    v          v
Sentiment    Topics
```

Possible outputs:

- Positive sentiment
- Neutral sentiment
- Negative sentiment
- Common complaint topics
- Common positive topics

This module is optional.

---

# 26. Association Rule Mining

The project uses order items to create baskets.

Example:

```text
Order A:
Product X
Product Y
Product Z
```

The system can find rules such as:

```text
Product X -> Product Y
```

Main metrics:

- Support
- Confidence
- Lift

Possible algorithms:

- FP-Growth
- Apriori

The output can support:

- Cross-selling
- Product bundling
- Recommendation experiments

---

# 27. Anomaly Detection

The platform detects unusual behavior.

Possible targets:

- Order volume
- Revenue
- Seller activity
- Cancellation activity
- Review activity
- Customer activity

Methods:

- Z-score
- IQR
- Isolation Forest

Example:

```text
Normal Order Volume
        |
        v
Expected Range
        |
        v
Observed Spike
        |
        v
Anomaly Event
```

---

# 28. Time-Series Analytics

The project creates time-series datasets.

Examples:

```text
daily_orders
daily_revenue
hourly_orders
weekly_revenue
monthly_revenue
```

The time-series pipeline is:

```text
Orders
  |
  v
Time Aggregation
  |
  v
Missing Period Detection
  |
  v
Trend Analysis
  |
  v
Seasonality Analysis
  |
  v
Feature Engineering
  |
  v
Forecasting
```

---

# 29. Forecasting

The system forecasts:

- Order volume
- Revenue
- Category demand

Forecast horizons can include:

- Next hour
- Next day
- Next seven days

The actual horizon depends on the density and quality of the selected time series.

The project uses baseline models first.

Baselines:

- Naive forecast
- Moving average
- Seasonal naive

ML models:

- LightGBM
- XGBoost

Optional statistical models:

- SARIMA

The model must be compared against a baseline.

---

# 30. Forecasting Features

Example features:

```text
lag_1
lag_7
lag_14
lag_28

rolling_mean_7
rolling_mean_14
rolling_mean_28

day_of_week
day_of_month
week_of_year
month
```

The feature set must only use information that was available before the forecast timestamp.

This prevents data leakage.

---

# 31. Delivery Prediction

The system predicts delivery delay.

Target:

```text
delivery_delay_days
```

Possible features:

```text
seller_state
customer_state
freight_value
product_weight
product_volume
product_category
order_day
order_month
```

Possible models:

- Linear Regression
- Random Forest
- LightGBM
- XGBoost

Metrics:

- MAE
- RMSE
- R²

---

# 32. Customer Satisfaction Prediction

Target:

```text
review_score
```

or a derived class:

```text
negative
neutral
positive
```

Possible features:

```text
delivery_delay
freight_value
price
delivery_time
seller
product_category
payment_type
```

Possible models:

- Logistic Regression
- Random Forest
- LightGBM
- XGBoost

The model output is a prediction.

It is not a causal conclusion.

---

# 33. Feature Data

Feature Data is separate from Analytical Data.

The purpose is to provide stable inputs for ML.

Example:

```text
features/
├── customer_features
├── delivery_features
├── satisfaction_features
└── demand_features
```

Each feature set should have:

```text
feature_name
feature_type
definition
source
calculation
version
```

Example:

```text
feature:
customer_30d_order_count

definition:
Number of orders made by the customer
during the previous 30 days.

version:
1
```

---

# 34. Model Management

The project uses **MLflow**.

MLflow is open source under Apache 2.0 and supports self-hosting.

The model lifecycle is:

```text
Training Data
     |
     v
Experiment
     |
     v
MLflow Tracking
     |
     v
Model Validation
     |
     v
Model Registry
     |
     v
Staging
     |
     v
Production
```

Each model version records:

- Parameters
- Metrics
- Dataset version
- Feature version
- Source code version
- Model artifact

---

# 35. Real-Time Architecture

The real-time system does not require a second external dataset.

It uses an **event simulator**.

The simulator replays historical transactions as events.

Example:

```text
Olist Order
    |
    v
Event Simulator
    |
    v
order.created
    |
    v
Apache Kafka
    |
    v
Spark Structured Streaming
    |
    v
Real-Time Processing
```

This allows the project to test streaming without access to a private production event source.

---

# 36. Event Types

Initial event types:

```text
order.created
order.approved
order.shipped
order.delivered

payment.created

review.created
```

Later, the project can add:

```text
customer.updated
product.updated
seller.updated
```

---

# 37. Apache Kafka

The project uses **Apache Kafka** as the event broker.

Kafka is an open-source distributed event-streaming platform.

It supports:

- Event storage
- Event replay
- Producers
- Consumers
- Topics
- Partitions
- Consumer groups

Kafka is available as an official Docker image.

The current Kafka documentation lists Kafka 4.3.1 as a supported release.

The project uses Kafka instead of a managed cloud streaming service.

This keeps the infrastructure cost at zero for local development.

---

# 38. Kafka Topics

Example:

```text
orders.created
orders.approved
orders.shipped
orders.delivered

payments.created

reviews.created
```

The event format should contain:

```text
event_id
event_type
event_version
event_time
source
order_id
customer_id
```

This creates a stable event contract.

---

# 39. Stream Processing

The streaming engine is **Apache Spark Structured Streaming**.

Spark provides:

- DataFrame processing
- SQL
- Structured Streaming
- MLlib
- Kafka integration

Apache Spark is an Apache open-source project. Official Spark documentation provides current releases and PySpark installation through PyPI.

Streaming pipeline:

```text
Kafka
  |
  v
Spark Structured Streaming
  |
  +--> Validate
  |
  +--> Deduplicate
  |
  +--> Transform
  |
  +--> Enrich
  |
  +--> Aggregate
  |
  +--> Inference
  |
  v
Serving / Analytical Store
```

---

# 40. Real-Time Metrics

The streaming system can calculate:

```text
orders_per_minute
revenue_per_minute
active_orders
orders_by_status
orders_by_state
recent_anomalies
```

Example:

```text
Current 5-minute window

Orders:       127
Revenue:      X BRL
Active:       XX
Anomalies:    X
```

The actual values come from the running stream.

---

# 41. Real-Time ML

A new order can trigger:

```text
New Order
    |
    v
Feature Extraction
    |
    +--> Customer Segment
    |
    +--> Delivery Prediction
    |
    +--> Anomaly Score
    |
    v
Prediction Event
```

Example output:

```json
{
  "order_id": "order-001",
  "delivery_delay_prediction": 1.7,
  "customer_segment": "segment_03",
  "anomaly_score": 0.04
}
```

The values above are examples only.

---

# 42. Serving Data

Serving Data is optimized for application access.

Examples:

```text
serving/
├── customer_profile
├── product_summary
├── seller_summary
├── demand_forecast
├── delivery_prediction
├── anomaly_alert
└── realtime_metrics
```

The API should not run large analytical joins for every request.

Instead:

```text
Raw
 |
Conformed
 |
Analytical
 |
Serving
 |
API
```

This gives predictable API performance.

---

# 43. Analytical Database

The project uses **ClickHouse** for analytical workloads.

ClickHouse is an open-source column-oriented database for real-time analytical workloads.

Use ClickHouse for:

- Dashboard queries
- Time-series aggregations
- Large analytical queries
- Real-time metrics
- OLAP workloads

---

# 44. Operational Database

The project uses **PostgreSQL** for application state.

Use PostgreSQL for:

- API state
- User configuration
- Job status
- Metadata
- Data quality results
- Application configuration

PostgreSQL is released under a permissive open-source license similar to BSD/MIT.

Do not use PostgreSQL as the primary high-volume analytics engine.

Use:

```text
PostgreSQL
= operational data

ClickHouse
= analytical data
```

---

# 45. Data Lake Storage

The project should use:

**Parquet + local object storage**

For a completely free local deployment, there are two options.

### Simple mode

Use the local filesystem:

```text
/data/
```

with:

```text
Parquet
```

### Distributed mode

Use **SeaweedFS** as an S3-compatible object store.

SeaweedFS is available under Apache 2.0.

I recommend the simple mode first.

Use SeaweedFS only when the project needs object-storage behavior.

This avoids unnecessary infrastructure.

---

# 46. Table Format

Optional:

**Apache Iceberg**

Iceberg can provide:

- Schema evolution
- Partition evolution
- Snapshot management
- Time travel
- Table metadata

Apache Iceberg is licensed under Apache 2.0.

However, Iceberg is not mandatory for the first version.

For this dataset, Parquet is enough.

Add Iceberg when the project demonstrates table versioning or larger data volumes.

---

# 47. API Layer

The backend uses **FastAPI**.

FastAPI is MIT licensed.

Main API groups:

```text
/api/v1/orders
/api/v1/customers
/api/v1/products
/api/v1/sellers

/api/v1/analytics
/api/v1/forecast
/api/v1/predictions
/api/v1/anomalies

/api/v1/realtime
```

Example:

```text
GET /api/v1/analytics/revenue
GET /api/v1/customers/{customer_id}
GET /api/v1/products/top
GET /api/v1/forecast/demand
GET /api/v1/realtime/metrics
```

---

# 48. Real-Time API

Use WebSocket for live dashboard updates.

Example:

```text
Kafka
  |
  v
Stream Processor
  |
  v
Realtime Store
  |
  v
FastAPI WebSocket
  |
  v
Next.js
```

The browser does not need to refresh the page.

---

# 49. Frontend

Use:

**Next.js + TypeScript**

Next.js is MIT licensed.

The frontend contains:

```text
Dashboard
Customers
Products
Sellers
Logistics
Forecasting
Reviews
Real-Time
Models
Data Quality
```

---

# 50. Dashboard Pages

## Page 1 — Executive Overview

Metrics:

- Revenue
- Orders
- Customers
- Average Order Value
- Average Review Score
- On-Time Delivery

Charts:

- Revenue trend
- Order trend
- Category revenue
- State distribution
- Order status

---

## Page 2 — Customer Intelligence

Show:

- Customer 360
- RFM
- Segments
- Cohorts
- Retention
- Customer geography

---

## Page 3 — Product Intelligence

Show:

- Product performance
- Category performance
- Pareto analysis
- Price distribution
- Association rules

---

## Page 4 — Seller and Logistics

Show:

- Seller performance
- Delivery time
- Delivery delay
- Freight value
- Geographic distribution

---

## Page 5 — Customer Satisfaction

Show:

- Review score
- Review distribution
- Sentiment
- Review topics
- Delivery delay vs review score

---

## Page 6 — Forecasting

Show:

- Actual values
- Forecast values
- Forecast horizon
- MAE
- RMSE
- sMAPE

---

## Page 7 — Real-Time Operations

Show:

- Live orders
- Orders per minute
- Revenue per minute
- Active events
- Anomalies
- Recent predictions

---

# 51. Orchestration

Use **Apache Airflow** for scheduled workflows.

Airflow is an Apache project and is available under Apache License 2.0.

Example DAG:

```text
daily_ingestion
      |
      v
quality_check
      |
      v
conformed_transform
      |
      v
analytical_transform
      |
      v
feature_generation
      |
      v
model_evaluation
      |
      v
serving_refresh
```

Airflow should not be used for real-time processing.

Use Kafka and Spark for real-time processing.

---

# 52. Monitoring

Use **Prometheus** for system metrics.

Prometheus is open source under Apache 2.0.

Monitor:

```text
API latency
API error rate
Kafka consumer lag
Spark streaming latency
Pipeline duration
Pipeline failures
Data freshness
Data quality failures
Model inference latency
CPU
Memory
Disk
```

Optional dashboard:

**Grafana OSS**

Grafana provides an OSS edition, but its current core license is AGPLv3.

This is acceptable for a personal/open project if you follow the license.

If you want to minimize license complexity, Prometheus can still be used without Grafana.

---

# 53. Testing

The project should include:

## Unit Tests

Test:

- Data transformations
- Feature calculations
- Business metrics
- API functions

Tool:

```text
pytest
```

## Data Tests

Test:

- Null rules
- Unique keys
- Foreign keys
- Value ranges
- Schema

## Integration Tests

Test:

```text
Kafka
  |
Spark
  |
Database
  |
API
```

## API Tests

Test:

```text
GET
POST
WebSocket
Error responses
```

---

# 54. CI/CD

Use:

**GitHub Actions**

Pipeline:

```text
git push
   |
   v
Lint
   |
   v
Unit Tests
   |
   v
Data Tests
   |
   v
Build Docker Images
   |
   v
Integration Tests
   |
   v
Publish Image
```

No paid CI service is required for a public GitHub project within applicable GitHub Actions limits.

---

# 55. Containerization

All major services run with Docker Compose.

Example:

```text
docker-compose.yml

services:

  postgres

  clickhouse

  kafka

  spark

  mlflow

  airflow

  prometheus

  api

  frontend
```

Add SeaweedFS only if object storage is required.

---

# 56. Recommended Local Deployment

For your laptop, do not start every service at the same time.

Use profiles.

### Core profile

```text
PostgreSQL
ClickHouse
FastAPI
Next.js
```

### Data profile

```text
Core
+
Spark
```

### Streaming profile

```text
Core
+
Kafka
+
Spark Streaming
```

### MLOps profile

```text
Core
+
MLflow
```

### Full profile

```text
Everything
```

This reduces RAM usage during development.

---

# 57. Recommended Local Architecture

For the first working version:

```text
                  Docker Compose
                       |
        +--------------+--------------+
        |              |              |
        v              v              v
   PostgreSQL      ClickHouse       Kafka
        |              |              |
        |              |              v
        |              |          Spark Streaming
        |              |              |
        +--------------+--------------+
                       |
                     FastAPI
                       |
                     Next.js
```

Add:

```text
MLflow
Airflow
Prometheus
```

after the core system works.

---

# 58. Python Stack

Recommended Python packages:

```text
pandas
polars
numpy
pyarrow

pyspark

scikit-learn
lightgbm
xgboost

mlxtend

statsmodels

nltk
scikit-learn
```

For API:

```text
fastapi
uvicorn
pydantic
```

For testing:

```text
pytest
pytest-asyncio
```

For data quality:

```text
Pandera
```

or custom validation with:

```text
Pydantic
PySpark
SQL
```

---

# 59. Data Processing Strategy

Do not use Spark for every operation.

Use the correct tool for the workload.

### Small local analysis

```text
Polars / Pandas
```

### Large batch transformation

```text
PySpark
```

### Analytical queries

```text
ClickHouse
```

### Application state

```text
PostgreSQL
```

### Event processing

```text
Kafka + Spark Structured Streaming
```

### ML

```text
scikit-learn
LightGBM
XGBoost
```

This is more important than simply using many technologies.

---

# 60. Data Storage Layout

Example:

```text
data/
│
├── raw/
│   └── olist/
│       └── ingestion_date=YYYY-MM-DD/
│
├── conformed/
│   ├── customers/
│   ├── orders/
│   ├── order_items/
│   ├── products/
│   ├── sellers/
│   ├── payments/
│   └── reviews/
│
├── analytical/
│   ├── customer_360/
│   ├── daily_sales/
│   ├── product_performance/
│   └── delivery_performance/
│
└── features/
    ├── customer/
    ├── delivery/
    └── forecasting/
```

Use Parquet for analytical files.

---

# 61. Data Versioning

Every pipeline run should have:

```text
dataset_version
schema_version
pipeline_version
feature_version
model_version
```

Example:

```text
dataset_version = olist_v1
schema_version = order_v2
feature_version = delivery_features_v3
model_version = delivery_model_v5
```

This allows reproducibility.

---

# 62. Data Lineage

The project should document:

```text
Source
  |
  v
Raw
  |
  v
Conformed
  |
  v
Analytical
  |
  v
Feature
  |
  v
Model
  |
  v
Serving
  |
  v
Dashboard
```

Example:

```text
olist_orders_dataset.csv
        |
        v
raw.orders
        |
        v
conformed.orders
        |
        v
analytical.daily_sales
        |
        v
dashboard.revenue
```

---

# 63. Data Contracts

Every event and important table should have a schema.

Example:

```text
OrderEvent v1

event_id       STRING
event_type     STRING
event_version  INTEGER
event_time     TIMESTAMP
order_id       STRING
customer_id    STRING
total_amount   DECIMAL
currency       STRING
source         STRING
```

The pipeline must reject invalid events.

This prevents schema errors from moving through the system.

---

# 64. Security

The project should implement basic security.

API:

- API versioning
- Input validation
- Error handling
- Rate limiting for public deployment
- CORS configuration
- Secret management

Do not store secrets in Git.

Use:

```text
.env
```

locally.

Use:

```text
GitHub Secrets
```

for CI/CD.

---

# 65. Observability

The system should answer:

```text
Is the service running?
Is the data fresh?
Is Kafka processing events?
Is Spark delayed?
Is the API slow?
Did the model fail?
Did data quality fail?
```

This is more useful than only having application logs.

---

# 66. Logging

Use structured logs.

Example:

```json
{
  "timestamp": "...",
  "service": "order-api",
  "level": "INFO",
  "event": "order_created",
  "order_id": "order-001",
  "latency_ms": 32
}
```

Do not use only plain text logs.

---

# 67. Project Repository

Recommended structure:

```text
commerce-intelligence-platform/
│
├── apps/
│   ├── api/
│   └── web/
│
├── ingestion/
│   ├── batch/
│   └── streaming/
│
├── data/
│   ├── contracts/
│   └── schemas/
│
├── pipelines/
│   ├── conformed/
│   ├── analytical/
│   └── features/
│
├── analytics/
│   ├── customer/
│   ├── product/
│   ├── seller/
│   └── logistics/
│
├── mining/
│   ├── rfm/
│   ├── clustering/
│   ├── association/
│   └── anomaly/
│
├── ml/
│   ├── forecasting/
│   ├── delivery/
│   ├── satisfaction/
│   └── inference/
│
├── streaming/
│   ├── producers/
│   └── consumers/
│
├── orchestration/
│   └── airflow/
│
├── monitoring/
│   ├── prometheus/
│   └── grafana/
│
├── tests/
│   ├── unit/
│   ├── integration/
│   └── data/
│
├── docker/
│
├── docs/
│
├── notebooks/
│
├── docker-compose.yml
├── Makefile
├── pyproject.toml
└── README.md
```

---

# 68. Main Technologies

| Area | Technology | Purpose |
|---|---|---|
| Language | Python | Main data and ML language |
| SQL | SQL | Data transformation and analytics |
| Batch Processing | PySpark | Distributed processing |
| Local Processing | Polars | Fast local data processing |
| Storage Format | Parquet | Columnar analytical storage |
| Object Storage | Local FS / SeaweedFS | Data lake storage |
| Event Streaming | Apache Kafka | Event broker |
| Stream Processing | Spark Structured Streaming | Real-time processing |
| OLTP | PostgreSQL | Application state |
| OLAP | ClickHouse | Analytical queries |
| API | FastAPI | Backend API |
| Frontend | Next.js | Web application |
| Frontend Language | TypeScript | Frontend development |
| ML | Scikit-learn | Classical ML |
| Gradient Boosting | LightGBM / XGBoost | Prediction |
| Data Mining | mlxtend / Spark MLlib | Mining algorithms |
| NLP | scikit-learn / optional Transformers | Review analysis |
| Experiment Tracking | MLflow | ML lifecycle |
| Orchestration | Apache Airflow | Batch scheduling |
| Monitoring | Prometheus | Metrics |
| Dashboard Monitoring | Grafana OSS | Observability |
| Containerization | Docker | Deployment |
| CI/CD | GitHub Actions | Automation |
| Testing | Pytest | Automated tests |

---

# 69. Free and Open-Source Strategy

The target is:

**No paid software service is required.**

The system can run on:

- Your laptop
- A university server
- A self-hosted Linux server
- A free or low-cost VM, if available

The core software is open source or freely available.

Important examples:

- PostgreSQL uses a permissive open-source license.
- FastAPI uses MIT.
- MLflow uses Apache 2.0.
- Apache Iceberg uses Apache 2.0.
- SeaweedFS uses Apache 2.0.
- Prometheus uses Apache 2.0.
- Apache Airflow uses Apache 2.0.
- Next.js uses MIT.
- Kafka is an open-source event-streaming platform.

"Free" means that the software does not require a paid license for the planned use.

Infrastructure still has a cost if you use a paid cloud server.

---

# 70. Why Apache Kafka Instead of Redpanda?

Redpanda Community Edition is free and source-available.

However, its Community Edition uses the Business Source License.

It also has restrictions on providing Redpanda itself as a commercial streaming service.

For this project, Apache Kafka is a cleaner choice.

The project can therefore state:

> "The streaming layer uses Apache Kafka."

This is easier to explain in a CV and in technical documentation.

---

# 71. Why Not Use MinIO?

MinIO is a common object-storage choice.

However, the current MinIO licensing situation is more complex than the older tutorials often suggest.

MinIO's current documentation states that its current software license has specific evaluation and enterprise terms, and its legal documentation notes changes to its open-source distribution.

For this project, I recommend:

```text
Phase 1:
Local filesystem + Parquet

Phase 2:
SeaweedFS if S3-compatible storage is required
```

This avoids adding a licensing problem that does not improve the core academic project.

---

# 72. Production Deployment Target

The complete application should be deployable as:

```text
                    Linux Server
                         |
                  Docker Compose
                         |
        +----------------+----------------+
        |                |                |
        v                v                v
    PostgreSQL       ClickHouse        Kafka
        |                |                |
        +----------------+----------------+
                         |
                    Spark Jobs
                         |
        +----------------+----------------+
        |                                 |
        v                                 v
     FastAPI                         MLflow
        |
        v
     Next.js
```

Optional:

```text
Prometheus
Grafana
Airflow
SeaweedFS
```

---

# 73. Development Environment

Recommended:

```text
Windows 11
WSL2
Docker Desktop
Git
VS Code
Python
Node.js
```

Linux deployment:

```text
Ubuntu
Docker Engine
Docker Compose
```

The application should not depend on Windows-specific behavior.

---

# 74. Development Workflow

Use:

```text
Git
 |
 +-- feature/data-quality
 +-- feature/customer-rfm
 +-- feature/forecasting
 +-- feature/realtime
 +-- feature/dashboard
```

Pull request:

```text
Code
 |
 v
Lint
 |
 v
Unit Tests
 |
 v
Integration Tests
 |
 v
Docker Build
 |
 v
Merge
```

---

# 75. Model Evaluation

Every ML task must have:

```text
Baseline
Candidate Models
Validation
Test
Error Analysis
Model Version
```

Example:

```text
Naive
   |
Moving Average
   |
LightGBM
   |
XGBoost
```

Do not report only one model.

The report must explain why the final model was selected based on measured results and operational requirements.

---

# 76. Data Leakage Control

This is a major requirement.

For forecasting:

Do not use future information.

For delivery prediction:

Do not use fields that become available only after delivery.

For customer prediction:

Use only information available at the prediction time.

Example:

Incorrect:

```text
Predict delivery delay
+
use actual delivery date
```

Correct:

```text
Predict delivery delay
+
use information available when the order is created
```

---

# 77. Reproducibility

The project must allow another developer to run:

```bash
git clone <repository>
docker compose up
```

Then:

```text
API
Frontend
Database
Kafka
MLflow
```

should become available.

The README must contain:

- Prerequisites
- Installation
- Configuration
- Dataset setup
- Pipeline execution
- Model training
- Streaming execution
- Dashboard access
- Troubleshooting

---

# 78. Project Phases

## Phase 1 — Data Foundation

Build:

```text
Olist ingestion
Raw storage
Schema validation
Data profiling
Data quality
Conformed data
```

---

## Phase 2 — Analytical Platform

Build:

```text
Customer 360
Product performance
Seller performance
Logistics
Revenue
Geography
```

---

## Phase 3 — Data Mining

Build:

```text
RFM
Customer clustering
Association rules
Anomaly detection
```

---

## Phase 4 — Machine Learning

Build:

```text
Demand forecasting
Delivery prediction
Satisfaction prediction
```

---

## Phase 5 — Serving

Build:

```text
ClickHouse
PostgreSQL
FastAPI
```

---

## Phase 6 — Frontend

Build:

```text
Next.js
Dashboard
Filters
Charts
Customer pages
Product pages
Forecast pages
```

---

## Phase 7 — Real-Time

Build:

```text
Event simulator
Kafka
Spark Structured Streaming
Real-time metrics
WebSocket
```

---

## Phase 8 — MLOps

Build:

```text
MLflow
Model Registry
Model versioning
Experiment tracking
```

---

## Phase 9 — Observability

Build:

```text
Prometheus
Grafana
Structured logs
Data quality metrics
Pipeline monitoring
```

---

# 79. Minimum Viable Product

If time becomes limited, the MVP is:

```text
Olist
   |
   v
Raw Data
   |
   v
Conformed Data
   |
   v
Analytical Data
   |
   +--> Customer Analytics
   +--> Product Analytics
   +--> Logistics Analytics
   |
   v
Forecasting
   |
   v
FastAPI
   |
   v
Next.js
```

This is already a complete project.

---

# 80. Full Production-Oriented Version

The full version is:

```text
                         OLIST
                           |
                           v
                      INGESTION
                           |
                           v
                       RAW DATA
                           |
                           v
                    DATA QUALITY
                           |
                           v
                   CONFORMED DATA
                     /          \
                    /            \
                   v              v
           ANALYTICAL DATA    FEATURE DATA
                   |              |
                   |              v
                   |          ML TRAINING
                   |              |
                   |              v
                   |         MODEL REGISTRY
                   |              |
                   +------+-------+
                          |
                          v
                    SERVING DATA
                          |
                 +--------+--------+
                 |                 |
                 v                 v
              FastAPI          WebSocket
                 |                 |
                 +--------+--------+
                          |
                          v
                       Next.js


Chat Order / POS
       |
       v
Event Producer
       |
       v
Apache Kafka
       |
       v
Spark Structured Streaming
       |
       +--> Conformed Events
       |
       +--> Online Features
       |
       +--> Anomaly Detection
       |
       +--> ML Inference
       |
       v
Real-Time Serving
       |
       v
WebSocket
       |
       v
Live Dashboard
```

---

# 81. Final Project Outputs

The final project should provide:

## Data Engineering

- Data ingestion pipeline
- Data contracts
- Data quality system
- Conformed data model
- Analytical data model
- Data lineage

## Data Analysis

- Executive analytics
- Customer analytics
- Product analytics
- Seller analytics
- Logistics analytics
- Geographic analytics

## Data Mining

- RFM
- Customer clustering
- Association rules
- Anomaly detection

## Machine Learning

- Demand forecasting
- Delivery prediction
- Satisfaction prediction

## NLP

- Review sentiment
- Review topic analysis

## Real-Time

- Kafka event pipeline
- Spark Structured Streaming
- Real-time metrics
- Real-time anomaly detection
- Real-time ML inference

## Application

- FastAPI
- Next.js
- REST API
- WebSocket

## MLOps

- MLflow
- Experiment tracking
- Model registry
- Model versioning

## DevOps

- Docker
- Docker Compose
- GitHub Actions
- Automated tests

## Observability

- Prometheus
- Grafana
- Structured logs
- Pipeline monitoring

---

# 82. Final Architecture Statement

The project can be described as follows:

> **The Real-Time Commerce Intelligence Platform is an end-to-end data platform that converts historical Olist commerce data into analytical, machine-learning, and real-time data products.**
>
> **The platform uses a shared canonical data model. This model separates source-specific data from business data. It allows multiple data sources to use the same downstream pipelines.**
>
> **The batch pipeline contains ingestion, raw storage, data quality validation, conformed data, analytical data, feature data, and serving data.**
>
> **The real-time pipeline uses Apache Kafka and Spark Structured Streaming to process order events. The streaming pipeline produces real-time metrics, features, anomaly scores, and model predictions.**
>
> **FastAPI provides the application interface. Next.js provides the web interface. ClickHouse provides analytical query performance. PostgreSQL stores operational application data. MLflow manages machine-learning experiments and model versions. Prometheus provides system monitoring. Docker provides reproducible deployment.**
>
> **The platform is designed to run without paid cloud services. The complete system can run locally or on a self-managed Linux server.**
>
> **The Olist dataset is the initial historical source. The architecture is not dependent on Olist. A future Olist adapter can be replaced by a Chat Order, POS, or CRM adapter while the canonical data model and downstream services remain unchanged.**

---

# 83. CV Positioning

After the system is actually implemented, the project can be described on a CV as:

**Real-Time Commerce Intelligence Platform**

> Designed and deployed an end-to-end commerce data platform with batch and real-time data pipelines using Apache Kafka, PySpark, ClickHouse, PostgreSQL, FastAPI, and Next.js.

> Built data quality, canonical data modeling, customer 360, RFM segmentation, association-rule mining, anomaly detection, demand forecasting, and delivery prediction pipelines.

> Implemented ML experiment tracking and model versioning with MLflow and real-time event processing with Kafka and Spark Structured Streaming.

> Containerized the platform with Docker Compose and implemented automated testing, CI/CD, and service monitoring with GitHub Actions and Prometheus.

Only include the statements that match the features that are actually implemented.

---

# 84. Recommended Final Technology Decision

For this project, the recommended core stack is:

```text
                 DATA
                  |
          Olist CSV / Events
                  |
                  v
              Python
                  |
        +---------+---------+
        |                   |
        v                   v
      Polars             PySpark
        |                   |
        +---------+---------+
                  |
             Parquet
                  |
                  v
          Conformed Data
                  |
       +----------+----------+
       |                     |
       v                     v
  ClickHouse             Features
       |                     |
       |                     v
       |                  MLflow
       |                     |
       +----------+----------+
                  |
               FastAPI
                  |
            +-----+-----+
            |           |
            v           v
         REST       WebSocket
            |           |
            +-----+-----+
                  |
               Next.js


REAL-TIME:

Order Event
    |
    v
Apache Kafka
    |
    v
Spark Structured Streaming
    |
    +--> ClickHouse
    +--> Feature Store/Data
    +--> ML Inference
    +--> WebSocket
```

### Core principle

**Do not add a technology unless it solves a real problem in the system.**

This keeps the project:

- Free to run
- Easier to maintain
- Easier to explain
- Easier to deploy
- Easier to test
- Easier to put on a CV
- Easier to extend to real Chat Order data

The first production-quality milestone should therefore be:

**Olist → Ingestion → Raw Data → Data Quality → Conformed Data → Analytical Data → ClickHouse → FastAPI → Next.js.**

After this path works end to end, add:

**ML → MLflow → Kafka → Spark Streaming → Real-Time Serving → Monitoring.**
