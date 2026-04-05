from __future__ import annotations

import argparse
import logging
from pathlib import Path

from etl.extract import _find_csv_files, extract_datasets
from etl.load import load_claims_to_warehouse
from etl.transform import transform_datasets

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
LOGGER = logging.getLogger(__name__)


def run_pipeline(data_dir: str | Path = "data", database_path: str | Path = "claims_warehouse.db"):
    data_dir = Path(data_dir)
    database_path = Path(database_path)

    LOGGER.info("Starting insurance claims ETL pipeline.")
    LOGGER.info("Step 1/3 - Extract: reading beneficiary, inpatient, outpatient, and prescription event files.")
    dataset_bundle = extract_datasets(data_dir=data_dir)

    LOGGER.info("Step 2/3 - Transform: cleaning source tables, validating them, and building a warehouse-ready star schema.")
    tables = transform_datasets(dataset_bundle, output_dir=data_dir)

    LOGGER.info("Step 3/3 - Load: rebuilding the local SQLite warehouse at %s", database_path.resolve())
    row_counts = load_claims_to_warehouse(tables, database_path=database_path)

    print("\nETL pipeline finished successfully.")
    print(f"Warehouse database: {database_path.resolve()}")
    print("Loaded tables:")
    for table_name, row_count in row_counts.items():
        print(f"  - {table_name}: {row_count:,} rows")
    print("\nNext step: launch the dashboard with `python -m streamlit run dashboard/app.py`")
    return row_counts


def list_available_csv_files(data_dir: str | Path = "data") -> list[str]:
    return [path.name for path in _find_csv_files(Path(data_dir))]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a local ETL pipeline for CMS beneficiary, medical claims, and prescription drug event data."
    )
    parser.add_argument("--data-dir", default="data", help="Directory containing the CMS CSV source files.")
    parser.add_argument(
        "--db-path",
        default="claims_warehouse.db",
        help="Output SQLite database path for the star-schema warehouse.",
    )
    parser.add_argument(
        "--list-files",
        action="store_true",
        help="List CSV files available in the data directory and exit.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.list_files:
        files = list_available_csv_files(args.data_dir)
        if files:
            print("CSV files found in the data directory:")
            for file_name in files:
                print(f"  - {file_name}")
        else:
            print("No CSV files were found in the data directory.")
        raise SystemExit(0)

    try:
        run_pipeline(data_dir=args.data_dir, database_path=args.db_path)
    except Exception as exc:
        LOGGER.exception("Pipeline failed.")
        print("\nThe ETL pipeline could not finish.")
        print(f"Reason: {exc}")
        print("\nQuick checks:")
        print("  1. Make sure all four CMS source files are inside the data/ folder.")
        print("  2. Run `python run_pipeline.py --list-files` to confirm the files are present.")
        print("  3. Confirm your virtual environment is active and dependencies are installed.")
        raise SystemExit(1)
