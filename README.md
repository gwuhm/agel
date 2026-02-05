# Diabetes Bronze-to-Silver Pipeline (Microsoft Fabric)

This project implements a two-layer ETL pipeline for the UCI Diabetes 130-US Hospitals dataset in Microsoft Fabric Lakehouse:
- Bronze notebook downloads and extracts raw files.
- Silver notebook enforces schema, validates data quality, transforms data, and saves curated Delta tables.
- Orchestrator notebook runs both layers and writes data quality reports.

## Repository Structure

- `agel_orchestrator` - parent pipeline orchestration + report generation
- `agel_diabetes_bronze_ntb` - raw ingestion (download + extract)
- `agel_diabetes_silver_ntb` - curation, validation, and analytics
- `agel_lh.Lakehouse/` - Lakehouse metadata

## Setup

1. Create/import this repository into a Microsoft Fabric workspace.
2. Attach notebooks to the Lakehouse `agel_lh`.
3. Ensure notebook runtime is `synapse_pyspark`.
4. Grant outbound access from Fabric runtime to:
   - `https://archive.ics.uci.edu`

## Dependencies

Runtime dependencies used by the notebooks:
- Python standard library: `json`, `datetime`, `zipfile`, `io`, `os`, `requests`
- Fabric notebook utilities: `notebookutils`
- PySpark: `pyspark.sql`, `pyspark.sql.functions`, `pyspark.sql.types`, `pyspark.sql.window`

## How to Run the Pipeline

Recommended:
1. Open `agel_orchestrator`.
2. Run all cells.

The orchestrator runs:
1. `agel_diabetes_bronze_ntb`
2. `agel_diabetes_silver_ntb`
3. Quality report generation for both layers

## Output Locations

- Bronze data: `Files/bronze/diabetes_uci`
- Silver data: `Files/silver/diabetes_uci`
- Silver fact table: `Silver.diabetic_encounters`
- Dimension tables:
  - `Silver.dim_admission_type`
  - `Silver.dim_discharge_disposition`
  - `Silver.dim_admission_source`
- JSON data quality reports:
  - `Files/bronze/diabetes_uci/data_quality_report.json`
  - `Files/silver/diabetes_uci/data_quality_report.json`

## CI/CD Consideration

- This solution is CI/CD-ready.
- When deploying to another Microsoft Fabric workspace/environment, default Lakehouse rebind must be handled by a deployment script in the CI/CD pipeline (Lakehouse dependency settings in notebooks).
- Recommended deployment sequence:
  1. Deploy Lakehouse + notebooks.
  2. Run pipeline script to rebind Lakehouse in all notebooks.
  3. Run `agel_orchestrator`.

## Troubleshooting

- If Bronze fails at download, verify network access to UCI URL and rerun Bronze notebook.
- If Silver fails on schema/validation, inspect `Files/silver/diabetes_uci/_logs/pipeline.log.jsonl`.
- If child notebooks are not found, verify notebook names exactly match orchestrator calls.
