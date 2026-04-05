from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from etl.quality_checks import run_medical_quality_checks, run_prescription_quality_checks

LOGGER = logging.getLogger(__name__)

CHRONIC_CONDITION_COLUMNS = [
    "sp_alzhdmta",
    "sp_chf",
    "sp_chrnkidn",
    "sp_cncr",
    "sp_copd",
    "sp_depressn",
    "sp_diabetes",
    "sp_ischmcht",
    "sp_osteoprs",
    "sp_ra_oa",
    "sp_strketia",
]

CHRONIC_CONDITION_LABELS = {
    "sp_alzhdmta": "Alzheimer's",
    "sp_chf": "Heart Failure",
    "sp_chrnkidn": "Kidney Disease",
    "sp_cncr": "Cancer",
    "sp_copd": "COPD",
    "sp_depressn": "Depression",
    "sp_diabetes": "Diabetes",
    "sp_ischmcht": "Ischemic Heart Disease",
    "sp_osteoprs": "Osteoporosis",
    "sp_ra_oa": "Rheumatoid Arthritis / Osteoarthritis",
    "sp_strketia": "Stroke / TIA",
}

RACE_CODE_MAP = {
    "1": "White",
    "2": "Black",
    "3": "Other",
    "5": "Hispanic",
}

GENDER_CODE_MAP = {
    "1": "Male",
    "2": "Female",
}


def _stable_integer_key(*values: object) -> int:
    joined = "||".join("" if value is None else str(value) for value in values)
    digest = hashlib.md5(joined.encode("utf-8"), usedforsecurity=False).hexdigest()[:12]
    return int(digest, 16)


def _parse_date_series(series: pd.Series) -> pd.Series:
    normalized = (
        series.astype("string")
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .replace({"<NA>": pd.NA, "nan": pd.NA, "None": pd.NA, "": pd.NA})
    )
    parsed = pd.to_datetime(normalized, format="%Y%m%d", errors="coerce")
    fallback_mask = parsed.isna() & normalized.notna()
    if fallback_mask.any():
        parsed.loc[fallback_mask] = pd.to_datetime(normalized.loc[fallback_mask], errors="coerce")
    return parsed


def _derive_age_group(age_series: pd.Series) -> pd.Series:
    age_bins = [-1, 17, 34, 49, 64, 79, 200]
    age_labels = ["0-17", "18-34", "35-49", "50-64", "65-79", "80+"]
    age_group = pd.cut(age_series, bins=age_bins, labels=age_labels)
    return age_group.astype("object").fillna("Unknown")


def _normalize_beneficiary(beneficiary_df: pd.DataFrame) -> pd.DataFrame:
    df = beneficiary_df.copy()
    df["patient_id"] = df["desynpuf_id"].astype(str).str.strip()
    df["birth_date"] = _parse_date_series(df["bene_birth_dt"])
    df["gender"] = df["bene_sex_ident_cd"].astype("string").map(GENDER_CODE_MAP).fillna("Unknown")
    df["race"] = df["bene_race_cd"].astype("string").map(RACE_CODE_MAP).fillna("Unknown")
    df["state"] = df["sp_state_code"].astype("string").fillna("Unknown")
    df["esrd_indicator"] = df.get("bene_esrd_ind", pd.Series(index=df.index, dtype="object")).astype("string").fillna("Unknown")

    chronic_flags = df.reindex(columns=CHRONIC_CONDITION_COLUMNS, fill_value=0).copy()
    chronic_flags = chronic_flags.apply(pd.to_numeric, errors="coerce").fillna(0)
    chronic_flags = (chronic_flags == 1).astype(int)
    df["chronic_conditions_count"] = (chronic_flags == 1).sum(axis=1)
    df["chronic_conditions_summary"] = chronic_flags.apply(
        lambda row: ", ".join(
            CHRONIC_CONDITION_LABELS[column]
            for column, value in row.items()
            if value == 1
        )
        or "None Reported",
        axis=1,
    )
    for column in CHRONIC_CONDITION_COLUMNS:
        df[column] = chronic_flags[column]

    reference_date = pd.Timestamp("2008-12-31")
    df["age"] = ((reference_date - df["birth_date"]).dt.days / 365.25).round()
    df["age_group"] = _derive_age_group(df["age"])
    df["patient_key"] = df["patient_id"].map(lambda value: _stable_integer_key("patient", value))

    dim_patient = df[
        [
            "patient_key",
            "patient_id",
            "birth_date",
            "age",
            "age_group",
            "gender",
            "race",
            "state",
            "esrd_indicator",
            "chronic_conditions_count",
            "chronic_conditions_summary",
            *CHRONIC_CONDITION_COLUMNS,
        ]
    ].drop_duplicates(subset=["patient_id"])
    return dim_patient.reset_index(drop=True)


def _normalize_medical_claims(raw_df: pd.DataFrame, claim_type: str) -> pd.DataFrame:
    df = raw_df.copy()
    df["patient_id"] = df["desynpuf_id"].astype(str).str.strip()
    df["claim_id"] = df["clm_id"].astype(str).str.strip()
    df["provider_id"] = df["prvdr_num"].astype(str).str.strip()
    df["claim_start_date"] = _parse_date_series(df["clm_from_dt"])
    df["claim_end_date"] = _parse_date_series(df["clm_thru_dt"])
    df["total_cost"] = pd.to_numeric(df["clm_pmt_amt"], errors="coerce")
    df["primary_payer_amount"] = pd.to_numeric(df.get("nch_prmry_pyr_clm_pd_amt"), errors="coerce").fillna(0.0)
    df["diagnosis_code"] = (
        df.get("icd9_dgns_cd_1")
        .astype("string")
        .fillna(df.get("admtng_icd9_dgns_cd").astype("string"))
        .fillna("UNKNOWN")
        .str.strip()
    )
    df["diagnosis_description"] = "Diagnosis " + df["diagnosis_code"].astype(str)
    df["procedure_code"] = df.get("icd9_prcdr_cd_1", pd.Series(index=df.index, dtype="object")).astype("string")
    df["provider_type"] = "Inpatient Facility" if claim_type == "inpatient" else "Outpatient Facility"
    df["claim_type"] = claim_type
    df["claim_duration_days"] = (df["claim_end_date"] - df["claim_start_date"]).dt.days
    df["cost_per_day"] = np.where(
        df["claim_duration_days"] > 0,
        df["total_cost"] / df["claim_duration_days"],
        df["total_cost"],
    )

    df = df.dropna(subset=["patient_id", "claim_id", "provider_id", "claim_start_date", "claim_end_date", "total_cost"]).copy()
    df = df.loc[df["total_cost"] >= 0].copy()
    df = df.loc[df["claim_duration_days"] >= 0].copy()
    df = df.sort_values(["claim_id", "claim_end_date"]).drop_duplicates(subset=["claim_id"], keep="last")
    return df[
        [
            "patient_id",
            "claim_id",
            "provider_id",
            "claim_start_date",
            "claim_end_date",
            "total_cost",
            "primary_payer_amount",
            "diagnosis_code",
            "diagnosis_description",
            "procedure_code",
            "provider_type",
            "claim_type",
            "claim_duration_days",
            "cost_per_day",
        ]
    ].reset_index(drop=True)


def _normalize_prescriptions(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()
    df["patient_id"] = df["desynpuf_id"].astype(str).str.strip()
    df["prescription_event_id"] = df["pde_id"].astype(str).str.strip()
    df["service_date"] = _parse_date_series(df["srvc_dt"])
    df["drug_code"] = df["prod_srvc_id"].astype("string").fillna("UNKNOWN").str.strip()
    df["drug_name"] = "Drug " + df["drug_code"].astype(str)
    df["drug_category"] = "Category " + df["drug_code"].astype(str).str.slice(0, 4).fillna("UNKN")
    df["quantity_dispensed"] = pd.to_numeric(df["qty_dspnsd_num"], errors="coerce").fillna(0.0)
    df["days_supply"] = pd.to_numeric(df["days_suply_num"], errors="coerce").fillna(0.0)
    df["patient_pay_amount"] = pd.to_numeric(df["ptnt_pay_amt"], errors="coerce").fillna(0.0)
    df["total_drug_cost"] = pd.to_numeric(df["tot_rx_cst_amt"], errors="coerce").fillna(0.0)
    df["cost_per_day"] = np.where(df["days_supply"] > 0, df["total_drug_cost"] / df["days_supply"], df["total_drug_cost"])

    df = df.dropna(subset=["patient_id", "prescription_event_id", "service_date"]).copy()
    df = df.loc[df["days_supply"] >= 0].copy()
    df = df.loc[df["total_drug_cost"] >= 0].copy()
    df = df.sort_values(["prescription_event_id", "service_date"]).drop_duplicates(subset=["prescription_event_id"], keep="last")
    return df[
        [
            "patient_id",
            "prescription_event_id",
            "service_date",
            "drug_code",
            "drug_name",
            "drug_category",
            "quantity_dispensed",
            "days_supply",
            "patient_pay_amount",
            "total_drug_cost",
            "cost_per_day",
        ]
    ].reset_index(drop=True)


def _ensure_patients_exist(dim_patient: pd.DataFrame, patient_ids: pd.Series) -> pd.DataFrame:
    existing_ids = set(dim_patient["patient_id"])
    missing_ids = sorted(set(patient_ids.dropna().astype(str)) - existing_ids)
    if not missing_ids:
        return dim_patient

    additions = pd.DataFrame(
        {
            "patient_key": [_stable_integer_key("patient", patient_id) for patient_id in missing_ids],
            "patient_id": missing_ids,
            "birth_date": pd.NaT,
            "age": np.nan,
            "age_group": "Unknown",
            "gender": "Unknown",
            "race": "Unknown",
            "state": "Unknown",
            "esrd_indicator": "Unknown",
            "chronic_conditions_count": 0,
            "chronic_conditions_summary": "Unknown",
            **{column: 0 for column in CHRONIC_CONDITION_COLUMNS},
        }
    )
    return pd.concat([dim_patient, additions], ignore_index=True)


def _build_dim_provider(inpatient_df: pd.DataFrame, outpatient_df: pd.DataFrame, dim_patient: pd.DataFrame) -> pd.DataFrame:
    provider_df = pd.concat(
        [
            inpatient_df[["provider_id", "provider_type", "patient_id"]],
            outpatient_df[["provider_id", "provider_type", "patient_id"]],
        ],
        ignore_index=True,
    ).drop_duplicates()
    provider_df = provider_df.merge(dim_patient[["patient_id", "state"]], on="patient_id", how="left")
    provider_type_summary = (
        provider_df.groupby("provider_id")["provider_type"]
        .agg(lambda values: "Mixed Medical Provider" if len(set(values)) > 1 else next(iter(set(values))))
        .reset_index()
    )
    provider_state_summary = (
        provider_df.groupby(["provider_id", "state"])
        .size()
        .reset_index(name="encounters")
        .sort_values(["provider_id", "encounters", "state"], ascending=[True, False, True])
        .drop_duplicates(subset=["provider_id"], keep="first")
        .rename(columns={"state": "provider_state"})
    )
    provider_type_summary = provider_type_summary.merge(provider_state_summary[["provider_id", "provider_state"]], on="provider_id", how="left")
    provider_type_summary["provider_state"] = provider_type_summary["provider_state"].fillna("Unknown")
    provider_type_summary["provider_key"] = provider_type_summary["provider_id"].map(
        lambda value: _stable_integer_key("provider", value)
    )
    provider_df = provider_type_summary
    return provider_df[["provider_key", "provider_id", "provider_type", "provider_state"]].reset_index(drop=True)


def _build_dim_date(inpatient_df: pd.DataFrame, outpatient_df: pd.DataFrame, prescriptions_df: pd.DataFrame) -> pd.DataFrame:
    all_dates = pd.concat(
        [
            inpatient_df["claim_start_date"],
            inpatient_df["claim_end_date"],
            outpatient_df["claim_start_date"],
            outpatient_df["claim_end_date"],
            prescriptions_df["service_date"],
        ],
        ignore_index=True,
    ).dropna().drop_duplicates().sort_values()

    dim_date = pd.DataFrame({"full_date": pd.to_datetime(all_dates)})
    dim_date["date_key"] = dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["year"] = dim_date["full_date"].dt.year
    dim_date["quarter"] = dim_date["full_date"].dt.quarter
    dim_date["month"] = dim_date["full_date"].dt.month
    dim_date["day"] = dim_date["full_date"].dt.day
    dim_date["day_of_week"] = dim_date["full_date"].dt.day_name()
    return dim_date[["date_key", "full_date", "year", "quarter", "month", "day", "day_of_week"]].reset_index(drop=True)


def _build_dim_diagnosis(inpatient_df: pd.DataFrame, outpatient_df: pd.DataFrame) -> pd.DataFrame:
    diagnosis_df = pd.concat(
        [
            inpatient_df[["diagnosis_code", "diagnosis_description"]],
            outpatient_df[["diagnosis_code", "diagnosis_description"]],
        ],
        ignore_index=True,
    ).drop_duplicates()
    diagnosis_df["diagnosis_key"] = diagnosis_df["diagnosis_code"].map(lambda value: _stable_integer_key("diagnosis", value))
    diagnosis_df = diagnosis_df.rename(columns={"diagnosis_code": "icd_code", "diagnosis_description": "icd_description"})
    return diagnosis_df[["diagnosis_key", "icd_code", "icd_description"]].reset_index(drop=True)


def _build_dim_drug(prescriptions_df: pd.DataFrame) -> pd.DataFrame:
    dim_drug = prescriptions_df[["drug_code", "drug_name", "drug_category"]].drop_duplicates().copy()
    dim_drug["drug_key"] = dim_drug["drug_code"].map(lambda value: _stable_integer_key("drug", value))
    return dim_drug[["drug_key", "drug_code", "drug_name", "drug_category"]].reset_index(drop=True)


def _build_medical_fact(
    claims_df: pd.DataFrame,
    dim_patient: pd.DataFrame,
    dim_provider: pd.DataFrame,
    dim_diagnosis: pd.DataFrame,
) -> pd.DataFrame:
    fact = claims_df.merge(dim_patient[["patient_key", "patient_id"]], on="patient_id", how="left")
    fact = fact.merge(dim_provider[["provider_key", "provider_id"]], on="provider_id", how="left")
    fact = fact.merge(dim_diagnosis[["diagnosis_key", "icd_code"]], left_on="diagnosis_code", right_on="icd_code", how="left")
    fact["start_date_key"] = fact["claim_start_date"].dt.strftime("%Y%m%d").astype(int)
    fact["end_date_key"] = fact["claim_end_date"].dt.strftime("%Y%m%d").astype(int)
    fact = fact.drop(columns=["icd_code"])
    return fact


def _build_prescription_fact(
    prescriptions_df: pd.DataFrame,
    dim_patient: pd.DataFrame,
    dim_drug: pd.DataFrame,
) -> pd.DataFrame:
    fact = prescriptions_df.merge(dim_patient[["patient_key", "patient_id"]], on="patient_id", how="left")
    fact = fact.merge(dim_drug[["drug_key", "drug_code"]], on="drug_code", how="left")
    fact["service_date_key"] = fact["service_date"].dt.strftime("%Y%m%d").astype(int)
    return fact


def _write_table_outputs(base_dir: Path, tables: dict[str, pd.DataFrame], quality_summary: dict) -> None:
    processed_dir = base_dir / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    export_tables = {}
    for table_name, dataframe in tables.items():
        export_df = dataframe.copy()
        for column in export_df.columns:
            if pd.api.types.is_datetime64_any_dtype(export_df[column]):
                export_df[column] = export_df[column].dt.strftime("%Y-%m-%d")
        export_df.to_csv(processed_dir / f"{table_name}.csv", index=False)
        export_tables[table_name] = export_df

    metadata = {
        "quality_summary": quality_summary,
        "row_counts": {table_name: int(len(df)) for table_name, df in tables.items()},
        "sources": {
            "beneficiary": "Patient demographics, age, gender, race, chronic conditions",
            "inpatient_claims": "Hospital stays, diagnosis codes, procedure codes, costs, dates",
            "outpatient_claims": "Outpatient visits, diagnosis codes, costs, dates",
            "prescription_events": "Prescription drug events, drug codes, costs, days supply",
        },
    }
    (processed_dir / "transform_metadata.json").write_text(json.dumps(metadata, indent=2, default=str), encoding="utf-8")


def transform_datasets(dataset_bundle: dict[str, pd.DataFrame], output_dir: str | Path = "data") -> dict[str, pd.DataFrame]:
    beneficiary_df = _normalize_beneficiary(dataset_bundle["beneficiary"])
    inpatient_df = _normalize_medical_claims(dataset_bundle["inpatient_claims"], claim_type="inpatient")
    outpatient_df = _normalize_medical_claims(dataset_bundle["outpatient_claims"], claim_type="outpatient")
    prescriptions_df = _normalize_prescriptions(dataset_bundle["prescription_events"])

    all_patient_ids = pd.concat(
        [beneficiary_df["patient_id"], inpatient_df["patient_id"], outpatient_df["patient_id"], prescriptions_df["patient_id"]],
        ignore_index=True,
    )
    dim_patient = _ensure_patients_exist(beneficiary_df, all_patient_ids)
    dim_provider = _build_dim_provider(inpatient_df, outpatient_df, dim_patient)
    dim_date = _build_dim_date(inpatient_df, outpatient_df, prescriptions_df)
    dim_diagnosis = _build_dim_diagnosis(inpatient_df, outpatient_df)
    dim_drug = _build_dim_drug(prescriptions_df)

    fact_inpatient_claims = _build_medical_fact(inpatient_df, dim_patient, dim_provider, dim_diagnosis)[
        [
            "claim_id",
            "patient_key",
            "provider_key",
            "start_date_key",
            "end_date_key",
            "diagnosis_key",
            "total_cost",
            "primary_payer_amount",
            "claim_duration_days",
            "cost_per_day",
            "procedure_code",
            "claim_start_date",
            "claim_end_date",
        ]
    ]
    fact_outpatient_claims = _build_medical_fact(outpatient_df, dim_patient, dim_provider, dim_diagnosis)[
        [
            "claim_id",
            "patient_key",
            "provider_key",
            "start_date_key",
            "end_date_key",
            "diagnosis_key",
            "total_cost",
            "primary_payer_amount",
            "claim_duration_days",
            "cost_per_day",
            "procedure_code",
            "claim_start_date",
            "claim_end_date",
        ]
    ]
    fact_prescriptions = _build_prescription_fact(prescriptions_df, dim_patient, dim_drug)[
        [
            "prescription_event_id",
            "patient_key",
            "drug_key",
            "service_date_key",
            "quantity_dispensed",
            "days_supply",
            "patient_pay_amount",
            "total_drug_cost",
            "cost_per_day",
        ]
    ]

    quality_summary = {
        "success": True,
        "datasets": [
            run_medical_quality_checks("inpatient_claims", fact_inpatient_claims),
            run_medical_quality_checks("outpatient_claims", fact_outpatient_claims),
            run_prescription_quality_checks(fact_prescriptions),
        ],
    }

    tables = {
        "dim_patient": dim_patient,
        "dim_provider": dim_provider,
        "dim_date": dim_date,
        "dim_diagnosis": dim_diagnosis,
        "dim_drug": dim_drug,
        "fact_inpatient_claims": fact_inpatient_claims.drop(columns=["claim_start_date", "claim_end_date"]),
        "fact_outpatient_claims": fact_outpatient_claims.drop(columns=["claim_start_date", "claim_end_date"]),
        "fact_prescriptions": fact_prescriptions,
    }
    _write_table_outputs(Path(output_dir), tables, quality_summary)
    LOGGER.info("Transformation complete. Output row counts: %s", {name: len(df) for name, df in tables.items()})
    return tables
