# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "034f9c6a-56c2-4d2c-aed2-0f4c7e7ce931",
# META       "default_lakehouse_name": "agel_lh",
# META       "default_lakehouse_workspace_id": "b28fea3e-638e-46c7-8836-03a3cb253998",
# META       "known_lakehouses": [
# META         {
# META           "id": "034f9c6a-56c2-4d2c-aed2-0f4c7e7ce931"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Configuration and Path Setup
# 
# Configures all input/output paths and schema names for the Bronze to Silver ETL pipeline.

# CELL ********************

DIABETES_BRONZE_PATH = "Files/bronze/diabetes_uci"
DIABETES_SILVER_PATH = "Files/silver/diabetes_uci"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Remove old logs
# Remove all logs from previous runs.

# CELL ********************

def reset_pipeline_log(log_dir: str):
    notebookutils.fs.mkdirs(log_dir)
    log_path = f"{log_dir}/pipeline.log.jsonl"
    try:
        notebookutils.fs.rm(log_path, True)
        print(f"[INFO] Cleared previous log: {log_path}")
    except Exception:
        print(f"[INFO] No previous log found: {log_path}")


# Keep only current run logs.
reset_pipeline_log(f"{DIABETES_BRONZE_PATH}/_logs")
reset_pipeline_log(f"{DIABETES_SILVER_PATH}/_logs")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Bronze tier
# Executes pipeline in bronze tier

# CELL ********************

try:
    response = notebookutils.notebook.run("agel_diabetes_bronze_ntb")
    
    print(f"Notebook finished succesfully")

except Exception as e:
    raise Exception(f"Child notebook failed: {str(e)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Silver tier
# Executes pipeline in silver tier

# CELL ********************

try:
    response = notebookutils.notebook.run("agel_diabetes_silver_ntb")
    
    print(f"Notebook finished succesfully")

except Exception as e:
    raise Exception(f"Child notebook failed: {str(e)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Pipeline Summary Report
# 
# Aggregates all pipeline logs and generates comprehensive data quality reports for both Bronze and Silver pipelines.

# CELL ********************

import json

def generate_pipeline_report(pipeline_name, log_dir, output_path, layer="silver"):
    """
    Generate a data quality report from pipeline logs.
    
    Args:
        pipeline_name: Name of the pipeline for the report
        log_dir: Directory containing pipeline.log.jsonl
        output_path: Where to save the JSON report
        layer: "bronze" or "silver" to determine metric extraction logic
    """
    try:
        log_path = f"{log_dir}/pipeline.log.jsonl"
        
        # Read all log entries - using Spark to read entire file
        df_logs = spark.read.text(log_path)
        log_entries = [json.loads(row.value) for row in df_logs.collect() if row.value.strip()]
        
        if not log_entries:
            raise ValueError(f"No valid log entries found in {log_path}")
        
        # Build summary report
        report = {
            "pipeline": pipeline_name,
            "layer": layer.upper(),
            "execution_timestamp": log_entries[-1]["timestamp_utc"],
            "total_steps": len(log_entries),
            "steps_status": {},
            "data_quality_metrics": {}
        }
        
        # Aggregate metrics from each step
        for entry in log_entries:
            step = entry["step"]
            status = entry["status"]
            payload = entry["payload"]
            
            report["steps_status"][step] = status
            
            # Extract key metrics based on layer
            if layer == "bronze":
                # Bronze layer metrics
                if step == "bronze_download":
                    report["data_quality_metrics"]["download_url"] = payload.get("url", "")
                    report["data_quality_metrics"]["zip_size_mb"] = payload.get("zip_size_mb", 0)
                    
                elif step == "bronze_extract":
                    report["data_quality_metrics"]["files_extracted"] = payload.get("files_extracted", 0)
                    report["data_quality_metrics"]["extraction_path"] = payload.get("extract_dir", "")
                    
                elif step == "bronze_validation":
                    report["data_quality_metrics"]["csv_files_found"] = payload.get("csv_files_found", 0)
                    report["data_quality_metrics"]["total_rows"] = payload.get("total_rows", 0)
                    
            elif layer == "silver":
                # Silver layer metrics
                if step == "silver_fact_ingest__load_csv":
                    report["data_quality_metrics"]["input_rows"] = payload.get("rows", 0)
                    report["data_quality_metrics"]["corrupt_rows"] = payload.get("corrupt_rows", 0)
                    
                elif step == "silver_fact_ingest__hard_schema_exact":
                    report["data_quality_metrics"]["casting_failures"] = payload.get("casting_failures_total", 0)
                    
                elif step == "silver_validation__duplicates":
                    report["data_quality_metrics"]["duplicates_removed"] = payload.get("duplicates_removed", 0)
                    
                elif step == "silver_validation__missing_values":
                    report["data_quality_metrics"]["columns_with_nulls"] = payload.get("columns_with_nulls", 0)
                    report["data_quality_metrics"]["missing_values_details"] = payload.get("missing_values", {})
                    
                elif step == "silver_validation__referential_integrity":
                    report["data_quality_metrics"]["orphaned_fk_rows"] = payload.get("total_orphaned_rows", 0)
                    
                elif step == "silver_validation__business_constraints":
                    report["data_quality_metrics"]["constraint_violations"] = payload.get("total_violations", 0)
                    report["data_quality_metrics"]["violations_by_constraint"] = payload.get("violations_by_constraint", {})
                    
                elif step == "silver_fact_save":
                    report["data_quality_metrics"]["output_rows"] = payload.get("rows_saved", 0)
                    report["data_quality_metrics"]["output_table"] = payload.get("table", "")
        
        # Calculate data quality score for Silver layer
        if layer == "silver":
            input_rows = report["data_quality_metrics"].get("input_rows", 1)
            issues = (
                report["data_quality_metrics"].get("corrupt_rows", 0) +
                report["data_quality_metrics"].get("casting_failures", 0) +
                report["data_quality_metrics"].get("duplicates_removed", 0) +
                report["data_quality_metrics"].get("orphaned_fk_rows", 0) +
                report["data_quality_metrics"].get("constraint_violations", 0)
            )
            report["data_quality_metrics"]["quality_score_pct"] = round(100 * (1 - issues / input_rows), 2)
        
        # Save report as JSON
        notebookutils.fs.put(output_path, json.dumps(report, indent=2), True)
        
        # Display summary
        print("=" * 80)
        print(f"DATA QUALITY REPORT - {pipeline_name}")
        print("=" * 80)
        print(f"\nPipeline: {report['pipeline']}")
        print(f"Layer: {report['layer']}")
        print(f"Completed: {report['execution_timestamp']}")
        print(f"Total Steps: {report['total_steps']}")
        
        success_count = sum(1 for status in report["steps_status"].values() if status == "SUCCESS")
        print(f"\nSteps Status: {success_count}/{len(report['steps_status'])} SUCCESS")
        for step, status in report["steps_status"].items():
            icon = "[OK]" if status == "SUCCESS" else "[FAIL]"
            print(f"  {icon} {step}: {status}")
        
        # Data quality metrics
        metrics = report["data_quality_metrics"]
        print(f"\n{layer.upper()} Layer Metrics:")
        
        if layer == "bronze":
            print(f"  Files Extracted:      {metrics.get('files_extracted', 0)}")
            print(f"  CSV Files Found:      {metrics.get('csv_files_found', 0)}")
            print(f"  Total Rows:           {metrics.get('total_rows', 0):,}")
            print(f"  Zip Size (MB):        {metrics.get('zip_size_mb', 0):.2f}")
            
        elif layer == "silver":
            print(f"  Input Rows:           {metrics.get('input_rows', 0):,}")
            print(f"  Output Rows:          {metrics.get('output_rows', 0):,}")
            print(f"  Casting Failures:     {metrics.get('casting_failures', 0):,}")
            print(f"  Duplicates Removed:   {metrics.get('duplicates_removed', 0):,}")
            print(f"  Orphaned FK Rows:     {metrics.get('orphaned_fk_rows', 0):,}")
            print(f"  Constraint Violations: {metrics.get('constraint_violations', 0):,}")
            print(f"\nOverall Quality Score: {metrics.get('quality_score_pct', 0)}%")
            
            if metrics.get("constraint_violations", 0) > 0:
                print("\n[WARNING] Active Constraint Violations:")
                for constraint, count in metrics.get("violations_by_constraint", {}).items():
                    if count > 0:
                        print(f"  - {constraint}: {count:,} rows")
        
        print(f"\nReport saved to: {output_path}")
        print("=" * 80)
        
        return report
        
    except Exception as e:
        print(f"[FAIL] Failed to generate {layer} pipeline report: {str(e)}")
        raise


# Generate Bronze Layer Report
try:
    bronze_log_dir = f"{DIABETES_BRONZE_PATH}/_logs"
    bronze_report_path = f"{DIABETES_BRONZE_PATH}/data_quality_report.json"
    
    bronze_report = generate_pipeline_report(
        pipeline_name="UCI Diabetes - Bronze Layer ETL",
        log_dir=bronze_log_dir,
        output_path=bronze_report_path,
        layer="bronze"
    )
    print("\n")
    
except Exception as e:
    print(f"[ERROR] Bronze report generation failed: {str(e)}")
    print("Continuing with Silver report...\n")


# Generate Silver Layer Report
try:
    silver_log_dir = f"{DIABETES_SILVER_PATH}/_logs"
    silver_report_path = f"{DIABETES_SILVER_PATH}/data_quality_report.json"
    
    silver_report = generate_pipeline_report(
        pipeline_name="UCI Diabetes - Silver Layer ETL",
        log_dir=silver_log_dir,
        output_path=silver_report_path,
        layer="silver"
    )
    
except Exception as e:
    print(f"[ERROR] Silver report generation failed: {str(e)}")
    raise


# Generate Combined Summary
print("\n" + "=" * 80)
print("COMBINED PIPELINE SUMMARY")
print("=" * 80)
print("\nBronze → Silver ETL Pipeline Complete")
print(f"Bronze Report: {DIABETES_BRONZE_PATH}/data_quality_report.json")
print(f"Silver Report: {DIABETES_SILVER_PATH}/data_quality_report.json")
print("=" * 80)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
