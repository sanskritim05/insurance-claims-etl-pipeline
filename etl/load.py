from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import Column, Date, Float, Integer, MetaData, String, Table, create_engine, func, select

LOGGER = logging.getLogger(__name__)


def get_engine(database_path: str | Path = "claims_warehouse.db"):
    db_path = Path(database_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{db_path}", future=True)


def _define_tables(metadata: MetaData) -> dict[str, Table]:
    return {
        "dim_patient": Table(
            "dim_patient",
            metadata,
            Column("patient_key", Integer, primary_key=True),
            Column("patient_id", String, nullable=False, unique=True),
            Column("birth_date", Date),
            Column("age", Float),
            Column("age_group", String),
            Column("gender", String),
            Column("race", String),
            Column("state", String),
            Column("esrd_indicator", String),
            Column("chronic_conditions_count", Integer),
            Column("chronic_conditions_summary", String),
            Column("sp_alzhdmta", Integer),
            Column("sp_chf", Integer),
            Column("sp_chrnkidn", Integer),
            Column("sp_cncr", Integer),
            Column("sp_copd", Integer),
            Column("sp_depressn", Integer),
            Column("sp_diabetes", Integer),
            Column("sp_ischmcht", Integer),
            Column("sp_osteoprs", Integer),
            Column("sp_ra_oa", Integer),
            Column("sp_strketia", Integer),
        ),
        "dim_provider": Table(
            "dim_provider",
            metadata,
            Column("provider_key", Integer, primary_key=True),
            Column("provider_id", String, nullable=False, unique=True),
            Column("provider_type", String),
            Column("provider_state", String),
        ),
        "dim_date": Table(
            "dim_date",
            metadata,
            Column("date_key", Integer, primary_key=True),
            Column("full_date", Date, nullable=False, unique=True),
            Column("year", Integer),
            Column("quarter", Integer),
            Column("month", Integer),
            Column("day", Integer),
            Column("day_of_week", String),
        ),
        "dim_diagnosis": Table(
            "dim_diagnosis",
            metadata,
            Column("diagnosis_key", Integer, primary_key=True),
            Column("icd_code", String, nullable=False, unique=True),
            Column("icd_description", String),
        ),
        "dim_drug": Table(
            "dim_drug",
            metadata,
            Column("drug_key", Integer, primary_key=True),
            Column("drug_code", String, nullable=False, unique=True),
            Column("drug_name", String),
            Column("drug_category", String),
        ),
        "fact_inpatient_claims": Table(
            "fact_inpatient_claims",
            metadata,
            Column("claim_id", String, primary_key=True),
            Column("patient_key", Integer, nullable=False),
            Column("provider_key", Integer, nullable=False),
            Column("start_date_key", Integer, nullable=False),
            Column("end_date_key", Integer, nullable=False),
            Column("diagnosis_key", Integer, nullable=False),
            Column("total_cost", Float),
            Column("primary_payer_amount", Float),
            Column("claim_duration_days", Integer),
            Column("cost_per_day", Float),
            Column("procedure_code", String),
        ),
        "fact_outpatient_claims": Table(
            "fact_outpatient_claims",
            metadata,
            Column("claim_id", String, primary_key=True),
            Column("patient_key", Integer, nullable=False),
            Column("provider_key", Integer, nullable=False),
            Column("start_date_key", Integer, nullable=False),
            Column("end_date_key", Integer, nullable=False),
            Column("diagnosis_key", Integer, nullable=False),
            Column("total_cost", Float),
            Column("primary_payer_amount", Float),
            Column("claim_duration_days", Integer),
            Column("cost_per_day", Float),
            Column("procedure_code", String),
        ),
        "fact_prescriptions": Table(
            "fact_prescriptions",
            metadata,
            Column("prescription_event_id", String, primary_key=True),
            Column("patient_key", Integer, nullable=False),
            Column("drug_key", Integer, nullable=False),
            Column("service_date_key", Integer, nullable=False),
            Column("quantity_dispensed", Float),
            Column("days_supply", Float),
            Column("patient_pay_amount", Float),
            Column("total_drug_cost", Float),
            Column("cost_per_day", Float),
        ),
    }


def _prepare_tables_for_load(tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    prepared = {}
    for table_name, dataframe in tables.items():
        current = dataframe.copy()
        if table_name == "dim_date":
            current["full_date"] = pd.to_datetime(current["full_date"]).dt.date
        if table_name == "dim_patient" and "birth_date" in current.columns:
            current["birth_date"] = pd.to_datetime(current["birth_date"], errors="coerce").dt.date
        prepared[table_name] = current
    return prepared


def _insert_in_chunks(connection, table: Table, dataframe: pd.DataFrame) -> None:
    if dataframe.empty:
        LOGGER.warning("Skipping load for %s because it is empty.", table.name)
        return
    normalized = dataframe.copy().where(pd.notna(dataframe), None)
    payload = normalized.to_dict(orient="records")
    chunk_size = max(1, min(500, 900 // len(table.columns)))
    for start in range(0, len(payload), chunk_size):
        chunk = payload[start : start + chunk_size]
        connection.execute(table.insert(), chunk)


def load_claims_to_warehouse(
    tables: dict[str, pd.DataFrame],
    database_path: str | Path = "claims_warehouse.db",
) -> dict[str, int]:
    engine = get_engine(database_path)
    metadata = MetaData()
    warehouse_tables = _define_tables(metadata)
    prepared_tables = _prepare_tables_for_load(tables)

    metadata.drop_all(engine, checkfirst=True)
    metadata.create_all(engine)

    load_order = [
        "dim_patient",
        "dim_provider",
        "dim_date",
        "dim_diagnosis",
        "dim_drug",
        "fact_inpatient_claims",
        "fact_outpatient_claims",
        "fact_prescriptions",
    ]

    with engine.begin() as connection:
        for table_name in load_order:
            _insert_in_chunks(connection, warehouse_tables[table_name], prepared_tables[table_name])
        row_counts = {
            table_name: connection.execute(
                select(func.count()).select_from(warehouse_tables[table_name])
            ).scalar_one()
            for table_name in load_order
        }

    LOGGER.info("Load complete. Warehouse row counts: %s", row_counts)
    return row_counts
