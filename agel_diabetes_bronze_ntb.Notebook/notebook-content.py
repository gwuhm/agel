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

DIABETES_BASE_PATH = "Files/bronze/diabetes_uci"
RAW_ZIP_PATH = f"{DIABETES_BASE_PATH}/uci_diabetes_296.zip"
LOG_DIR = f"{DIABETES_BASE_PATH}/_logs"

UCI_ZIP_URL = (
    "https://archive.ics.uci.edu/static/public/296/"
    "diabetes%2B130-us%2Bhospitals%2Bfor%2Byears%2B1999-2008.zip"
)

EXPECTED_FILES = {"diabetic_data.csv", "IDS_mapping.csv"}
EXTRACT_DIR = f"{DIABETES_BASE_PATH}/raw_extracted"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # ZIP download
# Donwload ZIP from defined path

# CELL ********************

import requests
import os 

PIPELINE_STEP_NAME = "diabetes_zip_download"

try:
    notebookutils.fs.mkdirs(DIABETES_BASE_PATH)
    
    response = requests.get(UCI_ZIP_URL, timeout=60)
    response.raise_for_status()

    # Binary write (ZIP) – notebookutils does NOT support bytes
    with open(f"/lakehouse/default/{RAW_ZIP_PATH}", "wb") as f:
        f.write(response.content)

    file_size = len(response.content)

    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="SUCCESS",
        payload={
            "file_name": "uci_diabetes_296.zip",
            "target_path": RAW_ZIP_PATH,
            "source_url": UCI_ZIP_URL,
            "size_bytes": file_size
        }
    )

except Exception as e:
    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="FAILED",
        payload={
            "file_name": "uci_diabetes_296.zip",
            "target_path": RAW_ZIP_PATH,
            "source_url": UCI_ZIP_URL,
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

# # ZIP extraction
# Extrat ZIP file to specified location

# CELL ********************

import zipfile
from io import TextIOWrapper

PIPELINE_STEP_NAME = "diabetes_zip_extract"

try:
    notebookutils.fs.mkdirs(EXTRACT_DIR)
    
    zip_local_path = f"/lakehouse/default/{RAW_ZIP_PATH}"

    extracted = []

    with zipfile.ZipFile(zip_local_path, "r") as z:
        for member in z.infolist():
            if member.is_dir():
                continue

            file_name = member.filename.split("/")[-1]

            # Export ONLY expected files
            if file_name not in EXPECTED_FILES:
                continue

            target_path = f"{EXTRACT_DIR}/{file_name}"

            with z.open(member, "r") as bf:
                text = TextIOWrapper(
                    bf, encoding="utf-8", errors="replace"
                ).read()

            notebookutils.fs.put(
                target_path,
                text,
                True
            )

            extracted.append({
                "file_name": file_name,
                "target_path": target_path,
                "size_bytes_in_zip": member.file_size,
                "type": "text/csv"
            })

    missing_expected = sorted(list(EXPECTED_FILES - set(x["file_name"] for x in extracted)))

    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="SUCCESS",
        payload={
            "zip_path": RAW_ZIP_PATH,
            "extract_dir": EXTRACT_DIR,
            "exported_files": extracted,
            "exported_files_count": len(extracted),
            "expected_missing": missing_expected
        }
    )

    print("ZIP processed successfully")
    print("Exported files:", [x["file_name"] for x in extracted])

    if missing_expected:
        print("WARNING: missing expected files:", missing_expected)

except Exception as e:
    write_pipeline_log(
        log_dir=LOG_DIR,
        step=PIPELINE_STEP_NAME,
        status="FAILED",
        payload={
            "zip_path": RAW_ZIP_PATH,
            "extract_dir": EXTRACT_DIR,
            "error": str(e)
        }
    )
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
