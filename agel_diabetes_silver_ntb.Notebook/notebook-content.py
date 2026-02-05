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

# # Pipeline Logging Utility
# 
# Defines a reusable function to write structured JSONL logs for each pipeline step.

# CELL ********************

import json
from datetime import datetime

def write_pipeline_log(
    log_dir: str,
    step: str,
    status: str,
    payload: dict
):
    notebookutils.fs.mkdirs(log_dir)

    log_record = {
        "step": step,
        "status": status,
        "timestamp_utc": datetime.utcnow().isoformat() + "Z",
        "payload": payload
    }

    log_path = f"{log_dir}/pipeline.log.jsonl"

    notebookutils.fs.append(
        log_path,
        json.dumps(log_record) + "\n",
        True  
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Configuration and Path Setup
# 
# Configures all input/output paths and schema names for the Bronze to Silver ETL pipeline.

# CELL ********************

DIABETES_BRONZE_PATH = "Files/bronze/diabetes_uci"
DIABETES_SILVER_PATH = "Files/silver/diabetes_uci"

# Bronze inputs (read-only)
BRONZE_EXTRACT_DIR = f"{DIABETES_BRONZE_PATH}/raw_extracted"
DIABETIC_DATA_CSV = f"{BRONZE_EXTRACT_DIR}/diabetic_data.csv"
IDS_MAPPING_CSV = f"{BRONZE_EXTRACT_DIR}/IDS_mapping.csv"

# Silver outputs
LOG_DIR = f"{DIABETES_SILVER_PATH}/_logs"
BAD_RECORDS_DIR = f"{DIABETES_SILVER_PATH}/_bad_records"
TEMP_DIR = f"{DIABETES_SILVER_PATH}/_temp"

# Schema & Tables
SILVER_SCHEMA = "Silver"
SILVER_FACT_TABLE = f"{SILVER_SCHEMA}.diabetic_encounters"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create Dimension Tables (Star Schema)
# 
# Parses IDS_mapping.csv to create three dimension tables for admission types, discharge dispositions, and admission sources.

# CELL ********************

from pyspark.sql import functions as F

PIPELINE_STEP_NAME = "silver_dim__create_dimensions"

try:
    # Split IDS_mapping.csv into 3 separate CSVs by empty lines (which contain just ",")
    notebookutils.fs.mkdirs(TEMP_DIR)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_SCHEMA}")
    
    print(f"Reading from: {IDS_MAPPING_CSV}")
    print(f"Writing temp files to: {TEMP_DIR}")
    
    # Read as text
    df_text = spark.read.text(IDS_MAPPING_CSV)
    lines = [row.value for row in df_text.collect()]
    
    print(f"Total lines read: {len(lines)}")
    
    # Split by lines containing only a comma (separator between sections)
    sections = []
    current_section = []
    for line in lines:
        if line.strip() == ",":
            if current_section:
                sections.append(current_section)
                current_section = []
        else:
            current_section.append(line)
    if current_section:
        sections.append(current_section)
    
    print(f"Sections found: {len(sections)}")
    
    validation_results = {}
    
    # Process each section based on header
    for i, section in enumerate(sections):
        header = section[0].lower()
        csv_content = "\n".join(section)
        file_path = f"{TEMP_DIR}/dim_{i}.csv"
        notebookutils.fs.put(file_path, csv_content, True)
        
        print(f"\n  Section {i}: {len(section)} lines")
        print(f"    Header: {section[0]}")
        
        # Determine table type from header
        if "admission_type_id" in header:
            dim_df = (
                spark.read.format("csv").option("header", True)
                .load(file_path)
                .select(
                    F.col("admission_type_id").cast("int"),
                    F.col("description").alias("admission_type_name")
                )
                .filter(F.col("admission_type_id").isNotNull())
            )
            table_name = "dim_admission_type"
            id_col = "admission_type_id"
            
        elif "discharge_disposition_id" in header:
            dim_df = (
                spark.read.format("csv").option("header", True)
                .load(file_path)
                .select(
                    F.col("discharge_disposition_id").cast("int"),
                    F.col("description").alias("discharge_disposition_name")
                )
                .filter(F.col("discharge_disposition_id").isNotNull())
            )
            table_name = "dim_discharge_disposition"
            id_col = "discharge_disposition_id"
            
        elif "admission_source_id" in header:
            dim_df = (
                spark.read.format("csv").option("header", True)
                .load(file_path)
                .select(
                    F.col("admission_source_id").cast("int"),
                    F.col("description").alias("admission_source_name")
                )
                .filter(F.col("admission_source_id").isNotNull())
            )
            table_name = "dim_admission_source"
            id_col = "admission_source_id"
        else:
            print(f"    [WARNING] Unknown section type, skipping")
            continue
        
        # Validate
        total = dim_df.count()
        distinct = dim_df.select(id_col).distinct().count()
        validation_results[table_name] = {
            "rows": int(total),
            "distinct_ids": int(distinct),
            "duplicates": int(total - distinct)
        }
        
        # Save
        dim_df.write.mode("overwrite").saveAsTable(f"{SILVER_SCHEMA}.{table_name}")
        
        dup_flag = "[WARNING]" if validation_results[table_name]["duplicates"] > 0 else "[OK]"
        print(f"    {dup_flag} {table_name}: {total} rows, {distinct} unique IDs")
    
    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="SUCCESS",
        payload={
            "sections_parsed": len(sections),
            "validation": validation_results
        }
    )
    
    print("\n[SUCCESS] Dimension tables created & validated")

except Exception as e:
    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="FAILED",
        payload={"error": str(e)}
    )
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Fact Table Ingestion
# 
# Loads the main diabetic_data.csv file with permissive mode to capture corrupt records and track ingestion metrics.

# CELL ********************

from pyspark.sql import functions as F

PIPELINE_STEP_NAME = "silver_fact_ingest__load_csv"

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
    notebookutils.fs.mkdirs(BAD_RECORDS_DIR)
    
    # 1) Load CSV safely (keep everything as strings for now)
    df_raw = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", False)  # keep as strings; schema later
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("badRecordsPath", BAD_RECORDS_DIR)
        .load(DIABETIC_DATA_CSV)
    )

    # 2) Minimal ingestion metrics
    rows = df_raw.count()
    cols = len(df_raw.columns)
    if "_corrupt_record" in df_raw.columns:
            corrupt_rows = df_raw.filter(F.col("_corrupt_record").isNotNull()).count()
    else:
        corrupt_rows = 0
        # optional: enforce column presence for downstream consistency
        df_raw = df_raw.withColumn("_corrupt_record", F.lit(None).cast("string"))

    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="SUCCESS",
        payload={
            "input_csv": DIABETIC_DATA_CSV,
            "rows": int(rows),
            "cols": int(cols),
            "corrupt_rows": int(corrupt_rows),
            "bad_records_dir": BAD_RECORDS_DIR,
            "columns": df_raw.columns
        }
    )

    print("[SUCCESS] CSV loaded into df_raw")
    print(f"rows={rows}, cols={cols}, corrupt_rows={corrupt_rows}")

except Exception as e:
    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="FAILED",
        payload={
            "input_csv": DIABETIC_DATA_CSV,
            "bad_records_dir": BAD_RECORDS_DIR,
            "error": str(e)
        }
    )
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Schema Enforcement with Validation
# 
# Applies explicit data types to all columns, replacing "?" with null, and tracks casting failures per column.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import types as T

PIPELINE_STEP_NAME = "silver_fact_ingest__hard_schema_exact"

HARD_SCHEMA = T.StructType([
    T.StructField("encounter_id", T.LongType(), True),
    T.StructField("patient_nbr", T.LongType(), True),
    T.StructField("race", T.StringType(), True),
    T.StructField("gender", T.StringType(), True),
    T.StructField("age", T.StringType(), True),
    T.StructField("weight", T.StringType(), True),

    T.StructField("admission_type_id", T.IntegerType(), True),
    T.StructField("discharge_disposition_id", T.IntegerType(), True),
    T.StructField("admission_source_id", T.IntegerType(), True),
    T.StructField("time_in_hospital", T.IntegerType(), True),

    T.StructField("payer_code", T.StringType(), True),
    T.StructField("medical_specialty", T.StringType(), True),

    T.StructField("num_lab_procedures", T.IntegerType(), True),
    T.StructField("num_procedures", T.IntegerType(), True),
    T.StructField("num_medications", T.IntegerType(), True),

    T.StructField("number_outpatient", T.IntegerType(), True),
    T.StructField("number_emergency", T.IntegerType(), True),
    T.StructField("number_inpatient", T.IntegerType(), True),

    T.StructField("diag_1", T.StringType(), True),
    T.StructField("diag_2", T.StringType(), True),
    T.StructField("diag_3", T.StringType(), True),

    T.StructField("number_diagnoses", T.IntegerType(), True),

    T.StructField("max_glu_serum", T.StringType(), True),
    T.StructField("A1Cresult", T.StringType(), True),

    T.StructField("metformin", T.StringType(), True),
    T.StructField("repaglinide", T.StringType(), True),
    T.StructField("nateglinide", T.StringType(), True),
    T.StructField("chlorpropamide", T.StringType(), True),
    T.StructField("glimepiride", T.StringType(), True),
    T.StructField("acetohexamide", T.StringType(), True),
    T.StructField("glipizide", T.StringType(), True),
    T.StructField("glyburide", T.StringType(), True),
    T.StructField("tolbutamide", T.StringType(), True),
    T.StructField("pioglitazone", T.StringType(), True),
    T.StructField("rosiglitazone", T.StringType(), True),
    T.StructField("acarbose", T.StringType(), True),
    T.StructField("miglitol", T.StringType(), True),
    T.StructField("troglitazone", T.StringType(), True),
    T.StructField("tolazamide", T.StringType(), True),
    T.StructField("examide", T.StringType(), True),
    T.StructField("citoglipton", T.StringType(), True),
    T.StructField("insulin", T.StringType(), True),

    T.StructField("glyburide-metformin", T.StringType(), True),
    T.StructField("glipizide-metformin", T.StringType(), True),
    T.StructField("glimepiride-pioglitazone", T.StringType(), True),
    T.StructField("metformin-rosiglitazone", T.StringType(), True),
    T.StructField("metformin-pioglitazone", T.StringType(), True),

    T.StructField("change", T.StringType(), True),
    T.StructField("diabetesMed", T.StringType(), True),
    T.StructField("readmitted", T.StringType(), True),
])

try:
    df = df_raw

    # 1) global "?" -> null
    for c in df.columns:
        df = df.withColumn(
            c,
            F.when(F.trim(F.col(c)) == "?", F.lit(None)).otherwise(F.col(c))
        )

    # 2) Track casting failures BEFORE applying schema
    total_rows = df.count()
    casting_failures = {}
    
    # For numeric columns, count how many non-null values would fail casting
    for field in HARD_SCHEMA.fields:
        col_name = field.name
        
        if isinstance(field.dataType, (T.IntegerType, T.LongType)):
            # Count rows where value is NOT null AND NOT castable to number
            failed_count = df.filter(
                F.col(col_name).isNotNull() & 
                F.col(col_name).cast(field.dataType).isNull()
            ).count()
            
            if failed_count > 0:
                casting_failures[col_name] = {
                    "failed_rows": int(failed_count),
                    "target_type": field.dataType.simpleString(),
                    "failure_rate_pct": round(100 * failed_count / total_rows, 2)
                }

    # 3) Apply schema
    df_typed = df.select([
        F.col(field.name).cast(field.dataType).alias(field.name)
        for field in HARD_SCHEMA.fields
    ])

    # 4) Log with detailed metrics
    total_failures = sum(f["failed_rows"] for f in casting_failures.values())
    
    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="SUCCESS",
        payload={
            "total_rows": int(total_rows),
            "columns": len(df_typed.columns),
            "schema_exact_match": True,
            "casting_failures_total": int(total_failures),
            "casting_failures_by_column": casting_failures,
            "columns_with_failures": len(casting_failures)
        }
    )

    print("[SUCCESS] df_typed ready with validation")
    print(f"Total rows: {total_rows}")
    print(f"Columns: {len(df_typed.columns)}")
    print(f"Total casting failures: {total_failures}")
    if casting_failures:
        print("\n[WARNING] Casting failures by column:")
        for col, stats in casting_failures.items():
            print(f"    {col}: {stats['failed_rows']} rows ({stats['failure_rate_pct']}%)")
    else:
        print("[OK] No casting failures detected")

except Exception as e:
    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="FAILED",
        payload={"error": str(e)}
    )
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Duplicate Detection and Removal
# 
# Identifies and removes duplicate records based on the primary key (encounter_id).

# CELL ********************

PIPELINE_STEP_NAME = "silver_validation__duplicates"

try:
    rows_before = df_typed.count()
    
    # Remove duplicates based on encounter_id (primary key)
    df_clean = df_typed.dropDuplicates(["encounter_id"])
    
    rows_after = df_clean.count()
    duplicates_removed = rows_before - rows_after
    
    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="SUCCESS",
        payload={
            "rows_before": int(rows_before),
            "rows_after": int(rows_after),
            "duplicates_removed": int(duplicates_removed)
        }
    )
    
    print(f"[SUCCESS] Duplicates removed: {duplicates_removed}")
    print(f"Rows: {rows_before} -> {rows_after}")

except Exception as e:
    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="FAILED",
        payload={"error": str(e)}
    )
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Missing Values Analysis
# 
# Analyzes null values across all columns and calculates missing data percentages.

# CELL ********************

PIPELINE_STEP_NAME = "silver_validation__missing_values"

try:
    total = df_clean.count()
    
    # Count nulls per column
    missing_stats = {}
    for col_name in df_clean.columns:
        null_count = df_clean.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            missing_stats[col_name] = {
                "null_count": int(null_count),
                "null_pct": round(100 * null_count / total, 2)
            }
    
    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="SUCCESS",
        payload={
            "total_rows": int(total),
            "columns_with_nulls": len(missing_stats),
            "missing_values": missing_stats
        }
    )
    
    print(f"[SUCCESS] Missing values analysis complete")
    print(f"Columns with nulls: {len(missing_stats)}/{len(df_clean.columns)}")
    if missing_stats:
        print("\nTop 5 columns by null %:")
        for col, stats in sorted(missing_stats.items(), key=lambda x: x[1]["null_pct"], reverse=True)[:5]:
            print(f"  {col}: {stats['null_pct']}%")

except Exception as e:
    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="FAILED",
        payload={"error": str(e)}
    )
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Referential Integrity Validation
# 
# Validates that foreign keys in the fact table reference valid dimension table records.

# CELL ********************

PIPELINE_STEP_NAME = "silver_validation__referential_integrity"

try:
    # Load dimension tables
    dim_admission_type = spark.table(f"{SILVER_SCHEMA}.dim_admission_type")
    dim_discharge = spark.table(f"{SILVER_SCHEMA}.dim_discharge_disposition")
    dim_admission_source = spark.table(f"{SILVER_SCHEMA}.dim_admission_source")
    
    # Get valid IDs from dimensions
    valid_admission_type_ids = set([row.admission_type_id for row in dim_admission_type.select("admission_type_id").collect()])
    valid_discharge_ids = set([row.discharge_disposition_id for row in dim_discharge.select("discharge_disposition_id").collect()])
    valid_admission_source_ids = set([row.admission_source_id for row in dim_admission_source.select("admission_source_id").collect()])
    
    # Check orphaned records in fact table
    orphaned_admission_type = df_clean.filter(
        F.col("admission_type_id").isNotNull() &
        ~F.col("admission_type_id").isin(valid_admission_type_ids)
    ).count()
    
    orphaned_discharge = df_clean.filter(
        F.col("discharge_disposition_id").isNotNull() &
        ~F.col("discharge_disposition_id").isin(valid_discharge_ids)
    ).count()
    
    orphaned_admission_source = df_clean.filter(
        F.col("admission_source_id").isNotNull() &
        ~F.col("admission_source_id").isin(valid_admission_source_ids)
    ).count()
    
    fk_validation = {
        "admission_type_id": {
            "valid_dim_values": len(valid_admission_type_ids),
            "orphaned_fact_rows": int(orphaned_admission_type)
        },
        "discharge_disposition_id": {
            "valid_dim_values": len(valid_discharge_ids),
            "orphaned_fact_rows": int(orphaned_discharge)
        },
        "admission_source_id": {
            "valid_dim_values": len(valid_admission_source_ids),
            "orphaned_fact_rows": int(orphaned_admission_source)
        }
    }
    
    total_orphaned = orphaned_admission_type + orphaned_discharge + orphaned_admission_source
    
    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="SUCCESS",
        payload={
            "total_orphaned_rows": int(total_orphaned),
            "fk_validation": fk_validation
        }
    )
    
    print("[SUCCESS] Referential integrity validation complete")
    print(f"Total orphaned FK references: {total_orphaned}")
    for fk_name, stats in fk_validation.items():
        flag = "[WARNING]" if stats["orphaned_fact_rows"] > 0 else "[OK]"
        print(f"  {flag} {fk_name}: {stats['valid_dim_values']} valid IDs, {stats['orphaned_fact_rows']} orphaned")

except Exception as e:
    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="FAILED",
        payload={"error": str(e)}
    )
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Data Transformation and Feature Engineering
# 
# Standardizes categorical values, trims strings, creates age groups, and encodes readmission flags.

# CELL ********************

PIPELINE_STEP_NAME = "silver_transformation__feature_engineering"

try:
    df_transformed = df_clean
    
    # 1) Trim all string columns
    string_cols = [field.name for field in df_transformed.schema.fields 
                   if isinstance(field.dataType, T.StringType)]
    
    for col_name in string_cols:
        df_transformed = df_transformed.withColumn(
            col_name,
            F.trim(F.col(col_name))
        )
    
    # 2) Standardize categorical values (per description.csv)
    # Gender: Male/Female only, rest → NULL
    df_transformed = df_transformed.withColumn(
        "gender",
        F.when(F.lower(F.col("gender")) == "male", "Male")
         .when(F.lower(F.col("gender")) == "female", "Female")
         .otherwise(None)
    )
    
    # Race: Caucasian, Asian, AfricanAmerican, Hispanic, Other
    df_transformed = df_transformed.withColumn(
        "race",
        F.initcap(F.col("race"))
    )
    # Max_glu_serum: >200, >300, norm, none (raw data has "Norm" not "Normal")
    # Max_glu_serum: >200, >300, normal, none
    df_transformed = df_transformed.withColumn(
        "max_glu_serum",
        F.lower(F.col("max_glu_serum"))
    )
    # A1Cresult: >8, >7, norm, none (raw data has "Norm" not "Normal")
    # A1Cresult: >8, >7, normal, none
    df_transformed = df_transformed.withColumn(
        "A1Cresult",
        F.lower(F.col("A1Cresult"))
    )
    
    # Medication columns: Up, Down, Steady, No
    medication_cols = [
        "metformin", "repaglinide", "nateglinide", "chlorpropamide", "glimepiride",
        "acetohexamide", "glipizide", "glyburide", "tolbutamide", "pioglitazone",
        "rosiglitazone", "acarbose", "miglitol", "troglitazone", "tolazamide",
        "examide", "citoglipton", "insulin", "glyburide-metformin", 
        "glipizide-metformin", "glimepiride-pioglitazone", "metformin-rosiglitazone",
        "metformin-pioglitazone"
    ]
    for col_name in medication_cols:
        df_transformed = df_transformed.withColumn(
            col_name,
            F.initcap(F.col(col_name))
        )
    # Change: change, no change (map "Ch"→"change", "No"→"no change")
    df_transformed = df_transformed.withColumn(
        "change",
        F.when(F.lower(F.col("change")) == "ch", "change")
         .when(F.lower(F.col("change")) == "no", "no change")
         .otherwise(F.lower(F.col("change")))
    )
    
    # DiabetesMed: Yes, No
    df_transformed = df_transformed.withColumn(
        "diabetesMed",
        F.initcap(F.col("diabetesMed"))
    )
    
    # Readmitted: <30, >30, NO
    df_transformed = df_transformed.withColumn(
        "readmitted",
        F.upper(F.col("readmitted"))
    )
    
    # 3) Standardize diagnosis codes (remove dots for consistency)
    for diag_col in ["diag_1", "diag_2", "diag_3"]:
        df_transformed = df_transformed.withColumn(
            diag_col,
            F.regexp_replace(F.col(diag_col), r"\.", "")
        )
    
    # 4) Parse age string "[X-Y)" into numeric lower/upper bounds
    df_transformed = df_transformed.withColumn(
        "age_lower",
        F.regexp_extract(F.col("age"), r"\[(\d+)-", 1).cast("int")
    ).withColumn(
        "age_upper",
        F.regexp_extract(F.col("age"), r"-(\d+)\)", 1).cast("int")
    )
    
    # 5) Create age_group category for analytics
    df_transformed = df_transformed.withColumn(
        "age_group",
        F.when(F.col("age_lower") < 18, "pediatric")
         .when(F.col("age_lower") < 65, "adult")
         .otherwise("senior")
    )
    
    # 6) Encode readmitted for ML/analytics (ordinal: NO=0, >30=1, <30=2)
    df_transformed = df_transformed.withColumn(
        "readmitted_flag",
        F.when(F.col("readmitted") == "NO", 0)
         .when(F.col("readmitted") == ">30", 1)
         .when(F.col("readmitted") == "<30", 2)
         .otherwise(None)
    )
    rows = df_transformed.count()
    rows = df_transformed.count()
    
    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="SUCCESS",
        payload={
            "rows": int(rows),
            "string_cols_trimmed": len(string_cols),
            "categorical_cols_standardized": len(medication_cols) + 6,
            "new_columns": ["age_lower", "age_upper", "age_group", "readmitted_flag"],
            "standardized_columns": ["diag_1", "diag_2", "diag_3"]
        }
    )
    
    print("✅ Data transformation complete")
    print(f"Rows: {rows}")
    print(f"String columns trimmed: {len(string_cols)}")
    print(f"Categorical values standardized: {len(medication_cols) + 6} columns")
    print("[SUCCESS] Data transformation complete")
    print(f"New columns: age_lower, age_upper, age_group, readmitted_flag")

except Exception as e:
    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="FAILED",
        payload={"error": str(e)}
    )

    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Business Constraints Validation
# 
# Validates business rules such as positive hospital stays, non-negative counts, and valid categorical values.

# CELL ********************

PIPELINE_STEP_NAME = "silver_validation__business_constraints"

try:
    constraint_violations = {}
    
    # 1) time_in_hospital must be positive
    invalid_time = df_transformed.filter(
        F.col("time_in_hospital").isNotNull() &
        (F.col("time_in_hospital") <= 0)
    ).count()
    constraint_violations["time_in_hospital_positive"] = int(invalid_time)
    
    # 2) num_* columns must be non-negative
    for col_name in ["num_lab_procedures", "num_procedures", "num_medications"]:
        invalid = df_transformed.filter(
            F.col(col_name).isNotNull() &
            (F.col(col_name) < 0)
        ).count()
        constraint_violations[f"{col_name}_non_negative"] = int(invalid)
    
    # 3) number_* columns must be non-negative
    for col_name in ["number_outpatient", "number_emergency", "number_inpatient"]:
        invalid = df_transformed.filter(
            F.col(col_name).isNotNull() &
            (F.col(col_name) < 0)
        ).count()
        constraint_violations[f"{col_name}_non_negative"] = int(invalid)
    
    # 4) number_diagnoses must be positive (at least 1 diagnosis required)
    invalid_diag = df_transformed.filter(
        F.col("number_diagnoses").isNotNull() &
        (F.col("number_diagnoses") <= 0)
    ).count()
    constraint_violations["number_diagnoses_positive"] = int(invalid_diag)
    
    # 5) age validation (0 <= age_lower < age_upper <= 120)
    invalid_age = df_transformed.filter(
        F.col("age").isNotNull() &
        (
            F.col("age_lower").isNull() |
            F.col("age_upper").isNull() |
            (F.col("age_lower") < 0) |
            (F.col("age_upper") > 120) |
            (F.col("age_lower") >= F.col("age_upper"))
        )
    ).count()
    constraint_violations["age_valid_range"] = int(invalid_age)
    
    # 6) Categorical values validation 
    # Gender: Male, Female (rest is NULL)
    invalid_gender = df_transformed.filter(
        F.col("gender").isNotNull() &
        ~F.col("gender").isin(["Male", "Female"])
    ).count()
    constraint_violations["gender_valid_values"] = int(invalid_gender)
    
    # Max_glu_serum: >200, >300, norm, none
    invalid_glu = df_transformed.filter(
        F.col("max_glu_serum").isNotNull() &
        ~F.col("max_glu_serum").isin([">200", ">300", "norm", "none"])
    ).count()
    constraint_violations["max_glu_serum_valid_values"] = int(invalid_glu)
    
    # A1Cresult: >8, >7, norm, none
    invalid_a1c = df_transformed.filter(
        F.col("A1Cresult").isNotNull() &
        ~F.col("A1Cresult").isin([">8", ">7", "norm", "none"])
    ).count()
    constraint_violations["A1Cresult_valid_values"] = int(invalid_a1c)
    
    # Medication columns: Up, Down, Steady, No
    medication_cols = [
        "metformin", "repaglinide", "nateglinide", "chlorpropamide", "glimepiride",
        "acetohexamide", "glipizide", "glyburide", "tolbutamide", "pioglitazone",
        "rosiglitazone", "acarbose", "miglitol", "troglitazone", "tolazamide",
        "examide", "citoglipton", "insulin", "glyburide-metformin", 
        "glipizide-metformin", "glimepiride-pioglitazone", "metformin-rosiglitazone",
        "metformin-pioglitazone"
    ]
    for col_name in medication_cols:
        invalid = df_transformed.filter(
            F.col(col_name).isNotNull() &
            ~F.col(col_name).isin(["Up", "Down", "Steady", "No"])
        ).count()
        if invalid > 0:
            constraint_violations[f"{col_name}_valid_values"] = int(invalid)
    
    # Change: change, no change
    invalid_change = df_transformed.filter(
        F.col("change").isNotNull() &
        ~F.col("change").isin(["change", "no change"])
    ).count()
    constraint_violations["change_valid_values"] = int(invalid_change)
    
    # DiabetesMed: Yes, No
    invalid_diabmed = df_transformed.filter(
        F.col("diabetesMed").isNotNull() &
        ~F.col("diabetesMed").isin(["Yes", "No"])
    ).count()
    constraint_violations["diabetesMed_valid_values"] = int(invalid_diabmed)
    
    # Readmitted: <30, >30, NO
    invalid_readmit = df_transformed.filter(
        F.col("readmitted").isNotNull() &
        ~F.col("readmitted").isin(["<30", ">30", "NO"])
    ).count()
    constraint_violations["readmitted_valid_values"] = int(invalid_readmit)
    
    total_violations = sum(constraint_violations.values())
    
    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="SUCCESS",
        payload={
            "total_violations": int(total_violations),
            "violations_by_constraint": constraint_violations
        }
    )
    
    print("[SUCCESS] Business constraints validation complete")
    print(f"Total constraint violations: {total_violations}")
    if total_violations > 0:
        print("\n[WARNING] Violations by constraint:")
        for constraint, count in constraint_violations.items():
            if count > 0:
                print(f"  - {constraint}: {count:,} rows")
    else:
        print("[OK] No constraint violations detected")

except Exception as e:
    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="FAILED",
        payload={"error": str(e)}
    )
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Save to Silver Layer
# 
# Persists the cleaned and transformed data to the Silver layer as a Delta table.

# CELL ********************

PIPELINE_STEP_NAME = "silver_fact_save"

try:
    # Save transformed data to Delta table
    df_transformed.write.mode("overwrite").saveAsTable(SILVER_FACT_TABLE)
    
    rows_saved = df_transformed.count()
    cols_saved = len(df_transformed.columns)
    
    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="SUCCESS",
        payload={
            "table": SILVER_FACT_TABLE,
            "rows_saved": int(rows_saved),
            "columns_saved": int(cols_saved),
            "format": "delta"
        }
    )
    
    print(f"[SUCCESS] Data saved to {SILVER_FACT_TABLE}")
    print(f"Rows: {rows_saved}, Columns: {cols_saved}")

except Exception as e:
    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="FAILED",
        payload={"error": str(e)}
    )
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Healthcare Data Summary Metrics
# 
# Generates analytical summaries including readmission rates, diagnosis patterns, and medication impacts.

# CELL ********************

from pyspark.sql.window import Window

PIPELINE_STEP_NAME = "silver_analytics__healthcare_summary"

try:
    # Metric 1: Readmission Rates by Age Group
    readmission_by_age = (
        df_transformed
        .groupBy("age_group", "readmitted")
        .count()
        .withColumn("total", F.sum("count").over(Window.partitionBy("age_group")))
        .withColumn("percentage", F.round(100 * F.col("count") / F.col("total"), 2))
        .orderBy("age_group", "readmitted")
    )
    
    print("=" * 80)
    print("HEALTHCARE SUMMARY METRICS")
    print("=" * 80)
    
    print("\n[1] Readmission Rates by Age Group:")
    readmission_by_age.show(20, truncate=False)
    
    # Metric 2: Top 10 Most Common Primary Diagnoses
    top_diagnoses = (
        df_transformed
        .filter(F.col("diag_1").isNotNull())
        .groupBy("diag_1")
        .agg(
            F.count("*").alias("encounter_count"),
            F.countDistinct("patient_nbr").alias("unique_patients")
        )
        .orderBy(F.col("encounter_count").desc())
        .limit(10)
    )
    
    print("\n[2] Top 10 Primary Diagnoses:")
    top_diagnoses.show(10, truncate=False)
    
    # Metric 3: Average Length of Stay by Demographics
    los_demographics = (
        df_transformed
        .groupBy("age_group", "gender")
        .agg(
            F.avg("time_in_hospital").alias("avg_los_days"),
            F.count("*").alias("encounter_count"),
            F.countDistinct("patient_nbr").alias("unique_patients")
        )
        .withColumn("avg_los_days", F.round("avg_los_days", 2))
        .orderBy("age_group", "gender")
    )
    
    print("\n[3] Average Length of Stay by Demographics:")
    los_demographics.show(20, truncate=False)
        
    # Save metrics summary to log
    metrics_summary = {
        "readmission_by_age_count": readmission_by_age.count(),
        "top_diagnoses_count": top_diagnoses.count(),
        "demographic_groups": los_demographics.count()
    }
    
    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="SUCCESS",
        payload=metrics_summary
    )
    
    print("\n[SUCCESS] Healthcare summary metrics generated")
    print("=" * 80)

except Exception as e:
    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="FAILED",
        payload={"error": str(e)}
    )
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
