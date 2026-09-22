# Atlikon FMCG — Data Engineering Pipeline

A Medallion-architecture (Bronze → Silver → Gold) pipeline built on **Databricks** that ingests a newly acquired company's messy sales data from **AWS S3**, cleans and conforms it with **PySpark**, and merges it into the parent company's existing **Delta Lake** analytics layer — feeding one unified BI dashboard.

## Stack
Databricks · PySpark · Delta Lake · AWS S3 · Unity Catalog · Databricks Workflows · SQL Dashboards

## Structure
- `0_data/` — sample datasets for the parent and child company
- `1_codes/` — setup, dimension processing, and fact processing notebooks
- `2_dashboarding/` — serving view SQL + dashboard export
- `resources/` — architecture diagram

## Run it
1. Upload `0_data/2_child_company` to your own S3 bucket and connect it to Databricks
2. Run notebooks under `1_codes/1_setup`, then `2_dimension_data_processing`, then `3_fact_data_processing`, in order
3. Build the serving view from `2_dashboarding/denormalise_table_query_fmcg.txt`
4. Point a dashboard (or Genie AI) at the resulting view

## License
MIT
