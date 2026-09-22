# Atlikon FMCG — Unified Data Engineering Pipeline

**Medallion-architecture data pipeline on Databricks that merges a newly acquired startup's messy data into an established parent company's analytics platform — built with PySpark, Delta Lake, AWS S3 and Databricks Workflows.**

![Architecture](resources/architecture_diagram.png)

---

## 1. Business Context

**Atlon**, a mature sports-equipment manufacturer, acquires **Sports Bar**, a fast-growing athletic-nutrition startup. Atlon already runs a clean Medallion pipeline (OLTP → Bronze → Silver → Gold) feeding a single BI dashboard. Sports Bar's data, by contrast, is scattered across spreadsheets and ad-hoc exports — inconsistent schemas, typos, mixed date formats, and no historical reporting.

**Goal:** ingest, clean, and conform Sports Bar's data to Atlon's existing data model, then merge it into Atlon's Gold layer so both businesses are reportable from **one** dashboard — without disrupting Atlon's existing pipeline.

**Success criteria**
- A single, reliable, combined analytics layer for both companies
- A gentle learning curve so the existing data team can maintain it
- A scalable design that supports daily incremental loads going forward

---

## 2. Architecture

The project implements the **Medallion Architecture** (Bronze → Silver → Gold) entirely inside Databricks, governed by **Unity Catalog**:

- **Parent company (Atlon)** already has a mature pipeline; for this project its existing Gold tables are seeded as the baseline/target.
- **Child company (Sports Bar)**:
  1. Raw OLTP exports land in an **AWS S3** bucket (the data lake).
  2. **Bronze** — raw files are ingested as-is into Delta tables with lineage metadata (`read_timestamp`, `file_name`, `file_size`) and Change Data Feed enabled.
  3. **Silver** — cleaning, deduplication, regex fixes, type casting, standardization.
  4. **Gold** — BI-ready dimension/fact tables for Sports Bar (`sb_dim_*`, `sb_fact_*`).
  5. **Merge** — each Sports Bar Gold table is **upserted (`MERGE`)** into Atlon's parent Gold table, conforming Sports Bar's schema to Atlon's data model along the way.
- A denormalized **Gold view** joins the merged fact/dimension tables for fast dashboarding.
- **Databricks SQL dashboards** and **Genie AI** (natural-language → SQL) sit on top as the serving layer.

**Load strategy**
- **Historical backfill**: 5 months (Jul 1 – Nov 30) loaded as a one-off batch.
- **Incremental**: daily files from Dec 1 onward processed through a **staging-table pattern** so only the active month is recomputed and re-merged, instead of reprocessing all history.

---

## 3. Data Model (Star Schema)

The merged Gold layer is a star schema centered on `fact_orders`:

| Table | Grain | Key columns |
|---|---|---|
| `dim_customers` | one row per customer | `customer_code` (PK), `customer`, `market`, `platform`, `channel` |
| `dim_products` | one row per product | `product_code` (PK), `division`, `category`, `product`, `variant` |
| `dim_gross_price` | one row per product/year | `product_code` (FK), `price_inr`, `year` |
| `dim_date` | one row per month | `date_key`, `month_start_date`, `year`, `month_name`, `month_short_name`, `quarter`, `year_quarter` |
| `fact_orders` | monthly, per product × customer | `date` (month start), `product_code` (FK), `customer_code` (FK), `sold_quantity` |

Sports Bar's **raw** source schema is very different and has to be conformed to the model above:

| Source file | Raw columns |
|---|---|
| `customers.csv` | `customer_id`, `customer_name`, `city` |
| `products.csv` | `product_name`, `product_id`, `category` |
| `gross_price.csv` | `product_id`, `month`, `gross_price` |
| `orders/*.csv` (daily) | `order_id`, `order_placement_date`, `customer_id`, `product_id`, `order_qty` |

---

## 4. Data Quality Issues Handled

Sports Bar's exports are intentionally messy to mirror a real post-acquisition scenario. The Silver-layer transformations fix:

- **City typos** — `Bengaluruu`, `Bengalore` → `Bengaluru`; `Hyderabadd`, `Hyderbad` → `Hyderabad`; `NewDelhi`, `NewDheli`, `NewDelhee` → `New Delhi`; anything outside the allowed list is nulled, then a small number of remaining nulls are fixed via a business-confirmed lookup table.
- **Free-text casing** — customer names and product categories forced to Title Case (`initcap`).
- **Spelling errors** — `"Protien"` → `"Protein"` in both product names and categories via `regexp_replace`.
- **Four different date formats** in `gross_price.month` (`yyyy/MM/dd`, `dd/MM/yyyy`, `yyyy-MM-dd`, `dd-MM-yyyy`) reconciled with `coalesce(try_to_date(...))`.
- **Dirty order dates** — weekday prefixes like `"Tuesday, July 01, 2025"` stripped via regex before parsing multiple date formats.
- **Bad prices** — negative values flipped positive, `"unknown"`/blank values set to `0`.
- **Invalid IDs** — non-numeric `customer_id` / `product_id` mapped to a `999999` fallback so fact rows aren't silently dropped.
- **Duplicates** — removed at both the dimension and fact grain before writing Silver/Gold.
- **Surrogate keys** — Sports Bar has no reliable `product_code`, so one is generated deterministically with `sha2(product_name, 256)` to safely join across layers.

---

## 5. Tech Stack

- **Databricks** (Free Edition) — notebooks, Delta Lake, Workflows, Unity Catalog, Genie AI
- **PySpark / Spark SQL** — all ETL transformations
- **Delta Lake** — Bronze/Silver/Gold tables, `MERGE` (upsert), Change Data Feed
- **AWS S3** — landing zone / data lake for the child company's raw exports
- **Databricks Workflows (Jobs)** — orchestration, scheduling, email alerts
- **Databricks SQL Dashboards + Genie** — serving layer / natural-language analytics

---

## 6. Repository Structure

```
project-de-fmcg-atlikon/
├── 0_data/
│   ├── 1_parent_company/
│   │   ├── full_load/            # Atlon's baseline Gold tables (seed data)
│   │   └── incremental_load/     # Atlon's monthly incremental (COPY INTO)
│   └── 2_child_company/
│       ├── full_load/            # Sports Bar historical: customers, products, gross_price, orders/landing (Jul–Nov)
│       └── incremental_load/     # Sports Bar daily order files (Dec 1–31)
├── 1_codes/
│   ├── 1_setup/                  # Catalog/schema setup, shared utilities, dim_date generation
│   ├── 2_dimension_data_processing/   # customers, products, gross_price (Bronze → Silver → Gold → Merge)
│   └── 3_fact_data_processing/   # orders: full historical load + daily incremental load
├── 2_dashboarding/
│   ├── denormalise_table_query_fmcg.txt   # Gold serving view (vw_fact_orders_enriched)
│   ├── fmcg_dashboard.pdf         # exported dashboard
│   └── dashboard_preview.png      # dashboard screenshot (see below)
├── resources/
│   ├── architecture_diagram.png
│   └── databricks_project.excalidraw   # editable source of the architecture diagram
├── README.md
├── .gitignore
└── LICENSE
```

---




## 7. Dashboard

![Dashboard preview](2_dashboarding/dashboard_preview.png)

The dashboard combines both companies' data behind a single set of filters (Year, Quarter, Month, Channel, Category) with:
- KPI scorecards — Total Revenue, Total Quantity Sold, Unique Customers, Average Selling Price
- Top products / top variants by revenue
- Revenue share by channel
- Monthly revenue trend
- Customer leaderboard
- Price vs. quantity-sold scatter, for spotting pricing outliers

---

## 9. Notes Before You Run This

- No AWS credentials or secrets are committed to this repo — S3 access is configured through Databricks' own external-location/IAM setup, not hardcoded keys.
- Bucket names and workspace paths in the notebooks are placeholders from the original build — update them per step 4 above before running.
- All CSVs under `0_data/` are the project's own sample/synthetic dataset, safe to keep in a public repo.
- All these things are result of learning from various youtube channels.
---

## 10. License

Released under the [MIT License](LICENSE).
