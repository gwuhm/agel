# Data Processing Report

## 1) Architecture Overview (Bronze -> Silver Mapping)

### Bronze Layer (`agel_diabetes_bronze_ntb`)
Purpose: ingest raw source files from UCI and persist them unchanged (except extraction).

- Input source: UCI ZIP dataset (public URL)
- Output files:
  - `Files/bronze/diabetes_uci/raw_extracted/diabetic_data.csv`
  - `Files/bronze/diabetes_uci/raw_extracted/IDS_mapping.csv`
- Logging: `Files/bronze/diabetes_uci/_logs/pipeline.log.jsonl`

Main Bronze steps:
1. Download ZIP (`diabetes_zip_download`)
2. Extract expected CSVs only (`diabetes_zip_extract`)
3. Log file-level ingestion metadata and execution status

### Silver Layer (`agel_diabetes_silver_ntb`)
Purpose: parse, validate, standardize, and model Bronze data into analytics-ready Delta tables.

- Inputs:
  - Bronze `diabetic_data.csv` (fact source)
  - Bronze `IDS_mapping.csv` (dimension source)
- Outputs:
  - `Silver.dim_admission_type`
  - `Silver.dim_discharge_disposition`
  - `Silver.dim_admission_source`
  - `Silver.diabetic_encounters`
- Logging: `Files/silver/diabetes_uci/_logs/pipeline.log.jsonl`

Bronze -> Silver mapping:
- `raw_extracted/IDS_mapping.csv` -> parsed by section -> 3 dimension tables
- `raw_extracted/diabetic_data.csv` -> schema enforcement + quality validation + transformations -> `Silver.diabetic_encounters`
- Silver enrichments:
  - standardization of categorical values
  - diagnosis normalization (`diag_1/2/3`)
  - derived features (`age_lower`, `age_upper`, `age_group`, `readmitted_flag`)

### Orchestration and Reporting (`agel_orchestrator`)
- Executes Bronze then Silver notebooks.
- Before execution, deletes previous run logs:
  - `Files/bronze/diabetes_uci/_logs/pipeline.log.jsonl`
  - `Files/silver/diabetes_uci/_logs/pipeline.log.jsonl`
- Builds JSON quality summaries from each layer log:
  - `Files/bronze/diabetes_uci/data_quality_report.json`
  - `Files/silver/diabetes_uci/data_quality_report.json`
- Computes overall Silver quality score from detected issue counts.

## 2) Data Quality Findings

The pipeline implements and logs the following quality controls:

- Ingestion integrity:
  - download success/failure, file size, extracted file count
  - corrupt CSV record tracking via `badRecordsPath`
- Structural quality:
  - strict type casting with per-column cast-failure metrics
  - duplicate detection/removal by `encounter_id`
  - missing-value profiling by column
- Relational quality:
  - foreign-key orphan checks against dimension IDs
- Business-rule quality:
  - numeric range/positivity checks (`time_in_hospital`, count columns, diagnosis count)
  - age interval validation
  - controlled categorical domain validation (`gender`, `max_glu_serum`, `A1Cresult`, `medication status`, `readmitted`)


## 3) Design Decisions and Potential Improvements

### Key design decisions
- Medallion-style split: Bronze for raw persistence, Silver for validated curated data.
- Defensive ingest strategy: permissive CSV load first, then explicit hard schema casting.
- Data observability: JSONL step logs with structured payloads, then report aggregation in orchestrator.
- Log scope decision: orchestrator clears previous log files at start, so reports always reflect only the latest run.
- Star-schema orientation: separate dimensions from `IDS_mapping.csv` and one main encounter fact table.
- Feature-ready output: adds standardized values and derived columns for analytics/ML.

### Potential improvements
- For current dataset volume, advanced performance tuning is intentionally not prioritized.
- Logging retention trade-off: current design keeps only latest-run logs (by deleting previous `pipeline.log.jsonl` files in orchestrator); if historical audit is needed, switch to run-partitioned logs or add a `run_id`.
