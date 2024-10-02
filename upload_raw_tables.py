"""
Script to read Google Sheets, upload to Google Cloud Storage, and load into BigQuery.
"""

import os
import re  # Standard library import should be first
from typing import List  # Standard library import

import yaml
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import gspread
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up environment variable for Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GCP_KEY")

# Read constants from .env file
BUCKET_NAME = os.getenv("BUCKET_NAME")
DATASET_ID = os.getenv("DATASET_ID")
GOOGLE_SHEET_URL = os.getenv("GOOGLE_SHEET_URL")
SCHEMA_FOLDER_PATH = "models/raw"  # Changed to UPPER_CASE for constant naming style

# Initialize clients
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# File table mapping (using sheet names)
SHEET_TABLE_MAPPING = {
    "salesforce_leads": "raw_salesforce_leads",
    "source1": "raw_source1",
    "source2": "raw_source2",
    "source3": "raw_source3",
}

# Create schema folder if it doesn't exist
if not os.path.exists(SCHEMA_FOLDER_PATH):
    os.makedirs(SCHEMA_FOLDER_PATH)


def clean_column_names(columns: List[str]) -> List[str]:
    """
    Clean column names by converting them to lowercase and replacing spaces with underscores.

    Args:
        columns (List[str]): List of column names.

    Returns:
        List[str]: Cleaned list of column names.
    """
    return [col.lower().replace(" ", "_") for col in columns]


def clean_field(value: str) -> str:
    """
    Clean individual fields in a DataFrame by removing unwanted characters and handling excessive
    quotes.

    Args:
        value (str): Field value to clean.

    Returns:
        str: Cleaned field value.
    """
    if isinstance(value, str):
        # Replace unwanted newlines within a field with a space
        value = re.sub(r"[\r\n]+", " ", value)
        # Remove excessive quotes around a field if not needed
        value = value.strip('"').replace('""', '"')
    return value


def generate_yaml_schema(
    sheet_name: str, table_name: str, columns: List[str], schema_folder: str
) -> str:
    """
    Generate a YAML schema file in the correct format based on provided columns.

    Args:
        sheet_name (str): Name of the Google Sheet.
        table_name (str): Corresponding BigQuery table name.
        columns (List[str]): List of cleaned column names.
        schema_folder (str): Folder path to store schema files.

    Returns:
        str: Path of the generated YAML file.
    """
    schema_content = {
        "version": 2,
        "models": [
            {
                "name": table_name,
                "description": f"Schema for {sheet_name} data",
                "columns": [
                    {"name": col, "type": "STRING", "mode": "NULLABLE"}
                    for col in columns
                ],
            }
        ],
    }

    # File path for the YAML file
    schema_file_path = os.path.join(schema_folder, f"{table_name}.yml")

    # Save the schema content to a YAML file in the specified format
    with open(schema_file_path, "w", encoding="utf-8") as schema_file:
        yaml.dump(
            schema_content, schema_file, default_flow_style=False, sort_keys=False
        )

    print(f"Schema for {table_name} saved at {schema_file_path} in the correct format.")
    return schema_file_path  # Return the path for validation


def read_and_upload_google_sheet_to_gcs(
    sheet_url: str, sheet_names: List[str], gcs_bucket_name: str, schema_folder: str
) -> None:
    """
    Read Google Sheets, preprocess columns, and directly upload CSVs to GCS.

    Args:
        sheet_url (str): URL of the Google Sheet.
        sheet_names (List[str]): List of sheet names to read.
        gcs_bucket_name (str): Name of the GCS bucket.
        schema_folder (str): Folder path to store schema files.
    """
    # Initialize gspread client using the service account key
    gc = gspread.service_account(filename=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

    # Open the Google Sheet using its URL
    spreadsheet = gc.open_by_url(sheet_url)
    bucket = storage_client.bucket(gcs_bucket_name)

    # Loop through the specified sheet names
    for sheet in sheet_names:
        # Read the sheet into a DataFrame
        worksheet = spreadsheet.worksheet(sheet)
        data = worksheet.get_all_records()
        df = pd.DataFrame(data)

        # Preprocess column names (convert to lowercase and replace spaces with underscores)
        df.columns = clean_column_names(df.columns)

        if sheet == "source2":
            df["primary_caregiver"] = (
                df["primary_caregiver"]
                .str.replace("\n \n ", "ZZZ")
                .str.replace("NEWLINE", "ZZZ")
            )

        # Clean the entire DataFrame
        df = df.applymap(clean_field)

        # Generate schema file dynamically based on cleaned column names
        cleaned_columns = df.columns.tolist()
        table = SHEET_TABLE_MAPPING[sheet]

        # Create a YAML schema file for each sheet with cleaned column names
        generate_yaml_schema(sheet, table, cleaned_columns, schema_folder)

        # Save the DataFrame as a CSV file in the current directory
        local_csv_path = f"{sheet}.csv"
        df.to_csv(local_csv_path, index=False)

        # Upload the CSV file to GCS
        blob = bucket.blob(local_csv_path)
        blob.upload_from_filename(local_csv_path)
        print(
            f"Sheet '{sheet}' uploaded to GCS as '{local_csv_path}' in bucket '{gcs_bucket_name}'."
        )

        # Remove the temporary CSV file after upload to avoid local clutter
        os.remove(local_csv_path)


def upload_csv_to_bigquery(
    gcs_bucket_name: str,
    file_name: str,
    table_name: str,
    dataset_id: str,
    schema_folder: str,
) -> None:
    """
    Load CSV files from GCS into BigQuery using a predefined schema.

    Args:
        gcs_bucket_name (str): Name of the GCS bucket.
        file_name (str): Name of the CSV file to be loaded.
        table_name (str): Name of the BigQuery table.
        dataset_id (str): BigQuery dataset ID.
        schema_folder (str): Folder path to the schema YAML files.
    """
    # Construct file path in GCS
    gcs_uri = f"gs://{gcs_bucket_name}/{file_name}"

    # Define BigQuery table reference
    table_ref = bigquery_client.dataset(dataset_id).table(table_name)

    # Load the schema file path from the saved YAML files
    schema_file_path = os.path.join(schema_folder, f"{table_name}.yml")
    with open(schema_file_path, "r", encoding="utf-8") as file:
        schema_data = yaml.safe_load(file)

    # Convert schema from YAML into a list of BigQuery SchemaField objects
    schema = [
        bigquery.SchemaField(col["name"], col["type"], mode=col.get("mode", "NULLABLE"))
        for col in schema_data["models"][0]["columns"]
    ]

    # Define the load job configuration
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, schema=schema, skip_leading_rows=1
    )

    # Start the load job
    load_job = bigquery_client.load_table_from_uri(
        gcs_uri, table_ref, job_config=job_config
    )

    print(f"Starting job to load {file_name} into {table_name}...")

    # Wait for the job to complete
    load_job.result()

    print(f"Loaded {file_name} into {table_name}.")


# Read Google Sheets and upload to GCS
SHEET_NAMES = list(SHEET_TABLE_MAPPING.keys())
read_and_upload_google_sheet_to_gcs(
    GOOGLE_SHEET_URL, SHEET_NAMES, BUCKET_NAME, SCHEMA_FOLDER_PATH
)

# Upload each file from GCS to BigQuery using the generated schema
for FILE_NAME, TABLE_NAME in SHEET_TABLE_MAPPING.items():
    upload_csv_to_bigquery(
        BUCKET_NAME, f"{FILE_NAME}.csv", TABLE_NAME, DATASET_ID, SCHEMA_FOLDER_PATH
    )
