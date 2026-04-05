from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATABASE_PATH = PROJECT_ROOT / "claims_warehouse.db"
PROCESSED_METADATA_PATH = PROJECT_ROOT / "data" / "processed" / "transform_metadata.json"

st.set_page_config(page_title="Insurance Claims ETL Dashboard", layout="wide")


def _run_query(query: str, params: tuple = ()) -> pd.DataFrame:
    with sqlite3.connect(DATABASE_PATH) as connection:
        return pd.read_sql_query(query, connection, params=params)


def _load_metadata() -> dict:
    if not PROCESSED_METADATA_PATH.exists():
        return {}
    return json.loads(PROCESSED_METADATA_PATH.read_text(encoding="utf-8"))


def _medical_union_subquery(claim_type: str) -> str:
    parts = []
    if claim_type in {"All", "inpatient"}:
        parts.append(
            """
            SELECT
                'Inpatient' AS claim_source,
                fi.claim_id AS event_id,
                fi.patient_key,
                fi.provider_key,
                fi.diagnosis_key,
                fi.start_date_key AS date_key,
                fi.total_cost AS cost,
                fi.claim_duration_days,
                fi.cost_per_day
            FROM fact_inpatient_claims fi
            """
        )
    if claim_type in {"All", "outpatient"}:
        parts.append(
            """
            SELECT
                'Outpatient' AS claim_source,
                fo.claim_id AS event_id,
                fo.patient_key,
                fo.provider_key,
                fo.diagnosis_key,
                fo.start_date_key AS date_key,
                fo.total_cost AS cost,
                fo.claim_duration_days,
                fo.cost_per_day
            FROM fact_outpatient_claims fo
            """
        )
    if not parts:
        return ""
    return " UNION ALL ".join(parts)


def _event_union_subquery(claim_type: str) -> str:
    parts = []
    medical_union = _medical_union_subquery(claim_type)
    if medical_union:
        parts.append(
            f"""
            SELECT
                claim_source,
                event_id,
                patient_key,
                provider_key,
                diagnosis_key,
                date_key,
                cost,
                claim_duration_days,
                cost_per_day
            FROM ({medical_union})
            """
        )
    if claim_type in {"All", "prescription"}:
        parts.append(
            """
            SELECT
                'Prescription' AS claim_source,
                fp.prescription_event_id AS event_id,
                fp.patient_key,
                NULL AS provider_key,
                NULL AS diagnosis_key,
                fp.service_date_key AS date_key,
                fp.total_drug_cost AS cost,
                NULL AS claim_duration_days,
                fp.cost_per_day
            FROM fact_prescriptions fp
            """
        )
    return " UNION ALL ".join(parts)


def _filter_clause(selected_year: str | int, age_group: str, state: str, date_alias: str = "dd", patient_alias: str = "dp") -> tuple[str, tuple]:
    clauses = []
    params: list[object] = []
    if selected_year != "All":
        clauses.append(f"{date_alias}.year = ?")
        params.append(selected_year)
    if age_group != "All":
        clauses.append(f"{patient_alias}.age_group = ?")
        params.append(age_group)
    if state != "All":
        clauses.append(f"{patient_alias}.state = ?")
        params.append(state)
    if not clauses:
        return "", ()
    return " WHERE " + " AND ".join(clauses), tuple(params)


@st.cache_data
def load_filter_options() -> dict[str, list]:
    years = _run_query("SELECT DISTINCT year FROM dim_date ORDER BY year")
    age_groups = _run_query("SELECT DISTINCT age_group FROM dim_patient ORDER BY age_group")
    states = _run_query("SELECT DISTINCT state FROM dim_patient ORDER BY state")
    return {
        "years": years["year"].dropna().astype(int).tolist(),
        "age_groups": [value for value in age_groups["age_group"].dropna().tolist()],
        "states": [value for value in states["state"].dropna().tolist()],
    }


def load_kpis(selected_year: str | int, claim_type: str, age_group: str, state: str) -> dict[str, object]:
    event_union = _event_union_subquery(claim_type)
    where_clause, params = _filter_clause(selected_year, age_group, state)

    totals_df = _run_query(
        f"""
        SELECT
            COUNT(*) AS total_claims,
            COALESCE(SUM(events.cost), 0) AS total_cost,
            COALESCE(AVG(events.cost), 0) AS average_cost_per_claim,
            COUNT(DISTINCT events.patient_key) AS total_unique_patients,
            COUNT(DISTINCT events.provider_key) AS total_unique_providers
        FROM ({event_union}) events
        JOIN dim_patient dp ON events.patient_key = dp.patient_key
        JOIN dim_date dd ON events.date_key = dd.date_key
        {where_clause}
        """,
        params,
    )
    avg_stay_df = _run_query(
        f"""
        SELECT COALESCE(AVG(events.claim_duration_days), 0) AS avg_stay
        FROM ({_medical_union_subquery('inpatient')}) events
        JOIN dim_patient dp ON events.patient_key = dp.patient_key
        JOIN dim_date dd ON events.date_key = dd.date_key
        {where_clause}
        """,
        params,
    )
    totals = totals_df.iloc[0]
    avg_stay = avg_stay_df["avg_stay"].iloc[0] if not avg_stay_df.empty else 0
    return {
        "total_claims": int(totals["total_claims"]),
        "total_cost": float(totals["total_cost"]),
        "average_cost_per_claim": float(totals["average_cost_per_claim"]),
        "average_length_of_stay": float(avg_stay) if pd.notna(avg_stay) else 0.0,
        "total_unique_patients": int(totals["total_unique_patients"]),
        "total_unique_providers": int(totals["total_unique_providers"]),
    }


def load_age_group_distribution(selected_year: str | int, claim_type: str, age_group: str, state: str) -> pd.DataFrame:
    event_union = _event_union_subquery(claim_type)
    where_clause, params = _filter_clause(selected_year, age_group, state)
    return _run_query(
        f"""
        SELECT dp.age_group, COUNT(*) AS claim_count
        FROM ({event_union}) events
        JOIN dim_patient dp ON events.patient_key = dp.patient_key
        JOIN dim_date dd ON events.date_key = dd.date_key
        {where_clause}
        GROUP BY dp.age_group
        ORDER BY claim_count DESC
        """,
        params,
    )


def load_gender_breakdown(selected_year: str | int, claim_type: str, age_group: str, state: str) -> pd.DataFrame:
    event_union = _event_union_subquery(claim_type)
    where_clause, params = _filter_clause(selected_year, age_group, state)
    return _run_query(
        f"""
        SELECT dp.gender, COUNT(DISTINCT dp.patient_id) AS patient_count
        FROM ({event_union}) events
        JOIN dim_patient dp ON events.patient_key = dp.patient_key
        JOIN dim_date dd ON events.date_key = dd.date_key
        {where_clause}
        GROUP BY dp.gender
        ORDER BY patient_count DESC
        """,
        params,
    )


def load_top_states_by_claim_volume(selected_year: str | int, claim_type: str, age_group: str, state: str) -> pd.DataFrame:
    event_union = _event_union_subquery(claim_type)
    where_clause, params = _filter_clause(selected_year, age_group, state)
    return _run_query(
        f"""
        SELECT dp.state, COUNT(*) AS claim_volume
        FROM ({event_union}) events
        JOIN dim_patient dp ON events.patient_key = dp.patient_key
        JOIN dim_date dd ON events.date_key = dd.date_key
        {where_clause}
        GROUP BY dp.state
        ORDER BY claim_volume DESC
        LIMIT 10
        """,
        params,
    )


def load_chronic_prevalence(selected_year: str | int, claim_type: str, age_group: str, state: str) -> pd.DataFrame:
    event_union = _event_union_subquery(claim_type)
    where_clause, params = _filter_clause(selected_year, age_group, state)
    patients_df = _run_query(
        f"""
        SELECT DISTINCT
            dp.sp_alzhdmta,
            dp.sp_chf,
            dp.sp_chrnkidn,
            dp.sp_cncr,
            dp.sp_copd,
            dp.sp_depressn,
            dp.sp_diabetes,
            dp.sp_ischmcht,
            dp.sp_osteoprs,
            dp.sp_ra_oa,
            dp.sp_strketia
        FROM ({event_union}) events
        JOIN dim_patient dp ON events.patient_key = dp.patient_key
        JOIN dim_date dd ON events.date_key = dd.date_key
        {where_clause}
        """,
        params,
    )
    if patients_df.empty:
        return pd.DataFrame(columns=["condition", "patient_count"])
    label_map = {
        "sp_alzhdmta": "Alzheimer's",
        "sp_chf": "Heart Failure",
        "sp_chrnkidn": "Kidney Disease",
        "sp_cncr": "Cancer",
        "sp_copd": "COPD",
        "sp_depressn": "Depression",
        "sp_diabetes": "Diabetes",
        "sp_ischmcht": "Ischemic Heart Disease",
        "sp_osteoprs": "Osteoporosis",
        "sp_ra_oa": "RA / OA",
        "sp_strketia": "Stroke / TIA",
    }
    prevalence = patients_df.sum(numeric_only=True).reset_index()
    prevalence.columns = ["condition_code", "patient_count"]
    prevalence["condition"] = prevalence["condition_code"].map(label_map)
    prevalence = prevalence.sort_values("patient_count", ascending=False)[["condition", "patient_count"]]
    return prevalence


def load_average_cost_by_age_group(selected_year: str | int, claim_type: str, age_group: str, state: str) -> pd.DataFrame:
    event_union = _event_union_subquery(claim_type)
    where_clause, params = _filter_clause(selected_year, age_group, state)
    return _run_query(
        f"""
        SELECT dp.age_group, AVG(events.cost) AS average_cost
        FROM ({event_union}) events
        JOIN dim_patient dp ON events.patient_key = dp.patient_key
        JOIN dim_date dd ON events.date_key = dd.date_key
        {where_clause}
        GROUP BY dp.age_group
        ORDER BY average_cost DESC
        """,
        params,
    )


def load_claim_volume_by_month(selected_year: str | int, claim_type: str, age_group: str, state: str) -> pd.DataFrame:
    medical_union = _medical_union_subquery("All" if claim_type == "All" else claim_type)
    if not medical_union:
        return pd.DataFrame(columns=["month_start", "claim_volume"])
    where_clause, params = _filter_clause(selected_year, age_group, state)
    return _run_query(
        f"""
        SELECT substr(dd.full_date, 1, 7) AS month_start, COUNT(*) AS claim_volume
        FROM ({medical_union}) events
        JOIN dim_patient dp ON events.patient_key = dp.patient_key
        JOIN dim_date dd ON events.date_key = dd.date_key
        {where_clause}
        GROUP BY month_start
        ORDER BY month_start
        """,
        params,
    )


def load_inpatient_outpatient_split(selected_year: str | int, age_group: str, state: str, claim_type: str) -> pd.DataFrame:
    medical_union = _medical_union_subquery("All" if claim_type == "All" else claim_type)
    if not medical_union:
        return pd.DataFrame(columns=["claim_source", "claim_count"])
    where_clause, params = _filter_clause(selected_year, age_group, state)
    return _run_query(
        f"""
        SELECT events.claim_source, COUNT(*) AS claim_count
        FROM ({medical_union}) events
        JOIN dim_patient dp ON events.patient_key = dp.patient_key
        JOIN dim_date dd ON events.date_key = dd.date_key
        {where_clause}
        GROUP BY events.claim_source
        ORDER BY claim_count DESC
        """,
        params,
    )


def load_average_claim_duration_by_type(selected_year: str | int, age_group: str, state: str, claim_type: str) -> pd.DataFrame:
    medical_union = _medical_union_subquery("All" if claim_type == "All" else claim_type)
    if not medical_union:
        return pd.DataFrame(columns=["claim_source", "average_duration_days"])
    where_clause, params = _filter_clause(selected_year, age_group, state)
    return _run_query(
        f"""
        SELECT events.claim_source, AVG(events.claim_duration_days) AS average_duration_days
        FROM ({medical_union}) events
        JOIN dim_patient dp ON events.patient_key = dp.patient_key
        JOIN dim_date dd ON events.date_key = dd.date_key
        {where_clause}
        GROUP BY events.claim_source
        ORDER BY average_duration_days DESC
        """,
        params,
    )


def load_cost_distribution(selected_year: str | int, age_group: str, state: str, claim_type: str) -> pd.DataFrame:
    medical_union = _medical_union_subquery("All" if claim_type == "All" else claim_type)
    if not medical_union:
        return pd.DataFrame(columns=["cost"])
    where_clause, params = _filter_clause(selected_year, age_group, state)
    return _run_query(
        f"""
        SELECT events.cost
        FROM ({medical_union}) events
        JOIN dim_patient dp ON events.patient_key = dp.patient_key
        JOIN dim_date dd ON events.date_key = dd.date_key
        {where_clause}
        """,
        params,
    )


def load_top_expensive_claims(selected_year: str | int, age_group: str, state: str, claim_type: str) -> pd.DataFrame:
    medical_union = _medical_union_subquery("All" if claim_type == "All" else claim_type)
    if not medical_union:
        return pd.DataFrame(columns=["claim_source", "event_id", "cost", "claim_duration_days", "patient_id", "provider_id", "provider_type"])
    where_clause, params = _filter_clause(selected_year, age_group, state)
    return _run_query(
        f"""
        SELECT
            events.claim_source,
            events.event_id AS claim_id,
            events.cost AS total_cost,
            events.claim_duration_days,
            dp.patient_id,
            prv.provider_id,
            prv.provider_type
        FROM ({medical_union}) events
        JOIN dim_patient dp ON events.patient_key = dp.patient_key
        JOIN dim_provider prv ON events.provider_key = prv.provider_key
        JOIN dim_date dd ON events.date_key = dd.date_key
        {where_clause}
        ORDER BY total_cost DESC
        LIMIT 10
        """,
        params,
    )


def load_top_diagnoses_by_cost(selected_year: str | int, age_group: str, state: str, claim_type: str) -> pd.DataFrame:
    medical_union = _medical_union_subquery("All" if claim_type == "All" else claim_type)
    if not medical_union:
        return pd.DataFrame(columns=["icd_description", "total_cost"])
    where_clause, params = _filter_clause(selected_year, age_group, state)
    return _run_query(
        f"""
        SELECT dg.icd_description, SUM(events.cost) AS total_cost
        FROM ({medical_union}) events
        JOIN dim_patient dp ON events.patient_key = dp.patient_key
        JOIN dim_diagnosis dg ON events.diagnosis_key = dg.diagnosis_key
        JOIN dim_date dd ON events.date_key = dd.date_key
        {where_clause}
        GROUP BY dg.icd_description
        ORDER BY total_cost DESC
        LIMIT 10
        """,
        params,
    )


def load_top_diagnoses_by_count(selected_year: str | int, age_group: str, state: str, claim_type: str) -> pd.DataFrame:
    medical_union = _medical_union_subquery("All" if claim_type == "All" else claim_type)
    if not medical_union:
        return pd.DataFrame(columns=["icd_description", "claim_count"])
    where_clause, params = _filter_clause(selected_year, age_group, state)
    return _run_query(
        f"""
        SELECT dg.icd_description, COUNT(*) AS claim_count
        FROM ({medical_union}) events
        JOIN dim_patient dp ON events.patient_key = dp.patient_key
        JOIN dim_diagnosis dg ON events.diagnosis_key = dg.diagnosis_key
        JOIN dim_date dd ON events.date_key = dd.date_key
        {where_clause}
        GROUP BY dg.icd_description
        ORDER BY claim_count DESC
        LIMIT 10
        """,
        params,
    )


def load_average_cost_per_diagnosis(selected_year: str | int, age_group: str, state: str, claim_type: str) -> pd.DataFrame:
    medical_union = _medical_union_subquery("All" if claim_type == "All" else claim_type)
    if not medical_union:
        return pd.DataFrame(columns=["icd_description", "average_cost"])
    where_clause, params = _filter_clause(selected_year, age_group, state)
    return _run_query(
        f"""
        SELECT dg.icd_code, dg.icd_description, AVG(events.cost) AS average_cost
        FROM ({medical_union}) events
        JOIN dim_patient dp ON events.patient_key = dp.patient_key
        JOIN dim_diagnosis dg ON events.diagnosis_key = dg.diagnosis_key
        JOIN dim_date dd ON events.date_key = dd.date_key
        {where_clause}
        GROUP BY dg.icd_code, dg.icd_description
        ORDER BY average_cost DESC
        LIMIT 25
        """,
        params,
    )


@st.cache_data
def load_icd_lookup() -> pd.DataFrame:
    return _run_query("SELECT icd_code, icd_description FROM dim_diagnosis ORDER BY icd_code")


def load_top_providers_by_volume(selected_year: str | int, age_group: str, state: str, claim_type: str) -> pd.DataFrame:
    medical_union = _medical_union_subquery("All" if claim_type == "All" else claim_type)
    if not medical_union:
        return pd.DataFrame(columns=["provider_id", "claims_volume"])
    where_clause, params = _filter_clause(selected_year, age_group, state)
    return _run_query(
        f"""
        SELECT prv.provider_id, COUNT(*) AS claims_volume
        FROM ({medical_union}) events
        JOIN dim_patient dp ON events.patient_key = dp.patient_key
        JOIN dim_provider prv ON events.provider_key = prv.provider_key
        JOIN dim_date dd ON events.date_key = dd.date_key
        {where_clause}
        GROUP BY prv.provider_id
        ORDER BY claims_volume DESC
        LIMIT 10
        """,
        params,
    )


def load_top_providers_by_cost(selected_year: str | int, age_group: str, state: str, claim_type: str) -> pd.DataFrame:
    medical_union = _medical_union_subquery("All" if claim_type == "All" else claim_type)
    if not medical_union:
        return pd.DataFrame(columns=["provider_id", "total_cost"])
    where_clause, params = _filter_clause(selected_year, age_group, state)
    return _run_query(
        f"""
        SELECT prv.provider_id, SUM(events.cost) AS total_cost
        FROM ({medical_union}) events
        JOIN dim_patient dp ON events.patient_key = dp.patient_key
        JOIN dim_provider prv ON events.provider_key = prv.provider_key
        JOIN dim_date dd ON events.date_key = dd.date_key
        {where_clause}
        GROUP BY prv.provider_id
        ORDER BY total_cost DESC
        LIMIT 10
        """,
        params,
    )


def load_provider_state_breakdown(selected_year: str | int, age_group: str, state: str, claim_type: str) -> pd.DataFrame:
    medical_union = _medical_union_subquery("All" if claim_type == "All" else claim_type)
    if not medical_union:
        return pd.DataFrame(columns=["provider_state", "claim_volume"])
    where_clause, params = _filter_clause(selected_year, age_group, state)
    return _run_query(
        f"""
        SELECT prv.provider_state, COUNT(*) AS claim_volume
        FROM ({medical_union}) events
        JOIN dim_patient dp ON events.patient_key = dp.patient_key
        JOIN dim_provider prv ON events.provider_key = prv.provider_key
        JOIN dim_date dd ON events.date_key = dd.date_key
        {where_clause}
        GROUP BY prv.provider_state
        ORDER BY claim_volume DESC
        LIMIT 20
        """,
        params,
    )


def load_top_prescribed_drugs(selected_year: str | int, age_group: str, state: str, claim_type: str) -> pd.DataFrame:
    if claim_type not in {"All", "prescription"}:
        return pd.DataFrame(columns=["drug_code", "prescription_volume"])
    where_clause, params = _filter_clause(selected_year, age_group, state)
    return _run_query(
        f"""
        SELECT dr.drug_code, COUNT(*) AS prescription_volume
        FROM fact_prescriptions fp
        JOIN dim_patient dp ON fp.patient_key = dp.patient_key
        JOIN dim_drug dr ON fp.drug_key = dr.drug_key
        JOIN dim_date dd ON fp.service_date_key = dd.date_key
        {where_clause}
        GROUP BY dr.drug_code
        ORDER BY prescription_volume DESC
        LIMIT 10
        """,
        params,
    )


def load_drug_cost_over_time(selected_year: str | int, age_group: str, state: str, claim_type: str) -> pd.DataFrame:
    if claim_type not in {"All", "prescription"}:
        return pd.DataFrame(columns=["month_start", "total_drug_cost"])
    where_clause, params = _filter_clause(selected_year, age_group, state)
    return _run_query(
        f"""
        SELECT substr(dd.full_date, 1, 7) AS month_start, SUM(fp.total_drug_cost) AS total_drug_cost
        FROM fact_prescriptions fp
        JOIN dim_patient dp ON fp.patient_key = dp.patient_key
        JOIN dim_date dd ON fp.service_date_key = dd.date_key
        {where_clause}
        GROUP BY month_start
        ORDER BY month_start
        """,
        params,
    )


def load_average_days_supply_by_category(selected_year: str | int, age_group: str, state: str, claim_type: str) -> pd.DataFrame:
    if claim_type not in {"All", "prescription"}:
        return pd.DataFrame(columns=["drug_category", "average_days_supply"])
    where_clause, params = _filter_clause(selected_year, age_group, state)
    return _run_query(
        f"""
        SELECT dr.drug_category, AVG(fp.days_supply) AS average_days_supply
        FROM fact_prescriptions fp
        JOIN dim_patient dp ON fp.patient_key = dp.patient_key
        JOIN dim_drug dr ON fp.drug_key = dr.drug_key
        JOIN dim_date dd ON fp.service_date_key = dd.date_key
        {where_clause}
        GROUP BY dr.drug_category
        ORDER BY average_days_supply DESC
        LIMIT 15
        """,
        params,
    )


def _pie_chart(dataframe: pd.DataFrame, theta: str, color: str, title: str):
    return alt.Chart(dataframe).mark_arc().encode(theta=theta, color=color, tooltip=list(dataframe.columns)).properties(title=title)


def _histogram_chart(dataframe: pd.DataFrame, column: str, title: str):
    return alt.Chart(dataframe).mark_bar().encode(
        alt.X(f"{column}:Q", bin=alt.Bin(maxbins=30), title=title),
        y="count():Q",
        tooltip=["count():Q"],
    ).properties(height=320)


st.title("Insurance Claims ETL Dashboard")
st.caption("Combined CMS beneficiary, inpatient, outpatient, and prescription data warehouse.")

if not DATABASE_PATH.exists():
    st.warning("The claims warehouse database does not exist yet.")
    st.code("python run_pipeline.py")
    st.stop()

filter_options = load_filter_options()
metadata = _load_metadata()

with st.sidebar:
    st.header("Filters")
    selected_year = st.selectbox("Year", options=["All"] + filter_options["years"], index=0)
    selected_claim_type = st.selectbox("Claim Type", options=["All", "inpatient", "outpatient", "prescription"], index=0)
    selected_age_group = st.selectbox("Age Group", options=["All"] + filter_options["age_groups"], index=0)
    selected_state = st.selectbox("State", options=["All"] + filter_options["states"], index=0)

kpis = load_kpis(selected_year, selected_claim_type, selected_age_group, selected_state)
table_counts_df = _run_query(
    """
    SELECT 'dim_patient' AS table_name, COUNT(*) AS row_count FROM dim_patient
    UNION ALL SELECT 'dim_provider', COUNT(*) FROM dim_provider
    UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date
    UNION ALL SELECT 'dim_diagnosis', COUNT(*) FROM dim_diagnosis
    UNION ALL SELECT 'dim_drug', COUNT(*) FROM dim_drug
    UNION ALL SELECT 'fact_inpatient_claims', COUNT(*) FROM fact_inpatient_claims
    UNION ALL SELECT 'fact_outpatient_claims', COUNT(*) FROM fact_outpatient_claims
    UNION ALL SELECT 'fact_prescriptions', COUNT(*) FROM fact_prescriptions
    """
)

kpi_row = st.columns(6)
kpi_row[0].metric("Total Claims", f"{kpis['total_claims']:,}")
kpi_row[1].metric("Total Cost", f"${kpis['total_cost']:,.2f}")
kpi_row[2].metric("Average Cost per Claim", f"${kpis['average_cost_per_claim']:,.2f}")
kpi_row[3].metric("Average Length of Stay", f"{kpis['average_length_of_stay']:.2f} days")
kpi_row[4].metric("Total Unique Patients", f"{kpis['total_unique_patients']:,}")
kpi_row[5].metric("Total Unique Providers", f"{kpis['total_unique_providers']:,}")

if metadata:
    quality_df = pd.DataFrame(
        [
            {
                "dataset": item.get("dataset"),
                "checks_run": item.get("expectation_count", 0),
                "status": "Passed" if item.get("success", False) else "Failed",
            }
            for item in metadata.get("quality_summary", {}).get("datasets", [])
        ]
    )
    st.dataframe(quality_df, use_container_width=True, hide_index=True)

tab_patient, tab_claims, tab_diagnosis, tab_provider, tab_prescription = st.tabs(
    ["Patient", "Claims", "Diagnosis", "Provider", "Prescription"]
)

with tab_patient:
    patient_col1, patient_col2 = st.columns(2)

    age_group_df = load_age_group_distribution(selected_year, selected_claim_type, selected_age_group, selected_state)
    patient_col1.subheader("Age Group Distribution")
    if age_group_df.empty:
        patient_col1.info("No data for the selected filters.")
    else:
        patient_col1.bar_chart(age_group_df.set_index("age_group")["claim_count"])

    gender_df = load_gender_breakdown(selected_year, selected_claim_type, selected_age_group, selected_state)
    patient_col2.subheader("Gender Breakdown")
    if gender_df.empty:
        patient_col2.info("No data for the selected filters.")
    else:
        patient_col2.altair_chart(_pie_chart(gender_df, "patient_count:Q", "gender:N", "Gender Breakdown"), use_container_width=True)

    patient_col3, patient_col4 = st.columns(2)
    top_states_df = load_top_states_by_claim_volume(selected_year, selected_claim_type, selected_age_group, selected_state)
    patient_col3.subheader("Top 10 States by Claim Volume")
    if top_states_df.empty:
        patient_col3.info("No data for the selected filters.")
    else:
        patient_col3.bar_chart(top_states_df.set_index("state")["claim_volume"])

    avg_cost_age_df = load_average_cost_by_age_group(selected_year, selected_claim_type, selected_age_group, selected_state)
    patient_col4.subheader("Average Cost by Age Group")
    if avg_cost_age_df.empty:
        patient_col4.info("No data for the selected filters.")
    else:
        patient_col4.bar_chart(avg_cost_age_df.set_index("age_group")["average_cost"])

    st.subheader("Chronic Condition Prevalence")
    chronic_df = load_chronic_prevalence(selected_year, selected_claim_type, selected_age_group, selected_state)
    if chronic_df.empty:
        st.info("No data for the selected filters.")
    else:
        st.bar_chart(chronic_df.set_index("condition")["patient_count"])

with tab_claims:
    claims_col1, claims_col2 = st.columns(2)

    monthly_claims_df = load_claim_volume_by_month(selected_year, selected_claim_type, selected_age_group, selected_state)
    claims_col1.subheader("Claim Volume by Month")
    if monthly_claims_df.empty:
        claims_col1.info("No medical claims for the selected filters.")
    else:
        monthly_claims_df["month_start"] = pd.to_datetime(monthly_claims_df["month_start"])
        claims_col1.line_chart(monthly_claims_df, x="month_start", y="claim_volume")

    split_df = load_inpatient_outpatient_split(selected_year, selected_age_group, selected_state, selected_claim_type)
    claims_col2.subheader("Inpatient vs Outpatient Claim Split")
    if split_df.empty:
        claims_col2.info("No medical claims for the selected filters.")
    else:
        claims_col2.altair_chart(_pie_chart(split_df, "claim_count:Q", "claim_source:N", "Claim Type Split"), use_container_width=True)

    claims_col3, claims_col4 = st.columns(2)
    avg_duration_df = load_average_claim_duration_by_type(selected_year, selected_age_group, selected_state, selected_claim_type)
    claims_col3.subheader("Average Claim Duration by Claim Type")
    if avg_duration_df.empty:
        claims_col3.info("No medical claims for the selected filters.")
    else:
        claims_col3.bar_chart(avg_duration_df.set_index("claim_source")["average_duration_days"])

    cost_distribution_df = load_cost_distribution(selected_year, selected_age_group, selected_state, selected_claim_type)
    claims_col4.subheader("Cost Distribution")
    if cost_distribution_df.empty:
        claims_col4.info("No medical claims for the selected filters.")
    else:
        claims_col4.altair_chart(_histogram_chart(cost_distribution_df, "cost", "Claim Cost"), use_container_width=True)

    st.subheader("Top 10 Most Expensive Individual Claims")
    top_claims_df = load_top_expensive_claims(selected_year, selected_age_group, selected_state, selected_claim_type)
    if top_claims_df.empty:
        st.info("No medical claims for the selected filters.")
    else:
        st.dataframe(top_claims_df, use_container_width=True, hide_index=True)

with tab_diagnosis:
    diag_col1, diag_col2 = st.columns(2)
    diag_cost_df = load_top_diagnoses_by_cost(selected_year, selected_age_group, selected_state, selected_claim_type)
    diag_col1.subheader("Top 10 Diagnoses by Total Cost")
    if diag_cost_df.empty:
        diag_col1.info("No diagnosis data for the selected filters.")
    else:
        diag_col1.bar_chart(diag_cost_df.set_index("icd_description")["total_cost"])

    diag_count_df = load_top_diagnoses_by_count(selected_year, selected_age_group, selected_state, selected_claim_type)
    diag_col2.subheader("Top 10 Diagnoses by Claim Count")
    if diag_count_df.empty:
        diag_col2.info("No diagnosis data for the selected filters.")
    else:
        diag_col2.bar_chart(diag_count_df.set_index("icd_description")["claim_count"])

    st.subheader("Average Cost per Diagnosis")
    avg_diag_df = load_average_cost_per_diagnosis(selected_year, selected_age_group, selected_state, selected_claim_type)
    if avg_diag_df.empty:
        st.info("No diagnosis data for the selected filters.")
    else:
        st.dataframe(avg_diag_df, use_container_width=True, hide_index=True)

    st.subheader("ICD Code Lookup")
    icd_search = st.text_input("Search ICD code or description")
    icd_df = load_icd_lookup()
    if icd_search:
        mask = icd_df["icd_code"].str.contains(icd_search, case=False, na=False) | icd_df["icd_description"].str.contains(icd_search, case=False, na=False)
        icd_df = icd_df.loc[mask]
    st.dataframe(icd_df.head(200), use_container_width=True, hide_index=True)

with tab_provider:
    provider_col1, provider_col2 = st.columns(2)
    providers_volume_df = load_top_providers_by_volume(selected_year, selected_age_group, selected_state, selected_claim_type)
    provider_col1.subheader("Top 10 Providers by Claims Volume")
    if providers_volume_df.empty:
        provider_col1.info("No provider data for the selected filters.")
    else:
        provider_col1.bar_chart(providers_volume_df.set_index("provider_id")["claims_volume"])

    providers_cost_df = load_top_providers_by_cost(selected_year, selected_age_group, selected_state, selected_claim_type)
    provider_col2.subheader("Top 10 Providers by Total Cost")
    if providers_cost_df.empty:
        provider_col2.info("No provider data for the selected filters.")
    else:
        provider_col2.bar_chart(providers_cost_df.set_index("provider_id")["total_cost"])

    st.subheader("Provider State Breakdown")
    provider_state_df = load_provider_state_breakdown(selected_year, selected_age_group, selected_state, selected_claim_type)
    if provider_state_df.empty:
        st.info("No provider state data for the selected filters.")
    else:
        st.bar_chart(provider_state_df.set_index("provider_state")["claim_volume"])
        st.caption("Provider state is inferred from the most common patient state seen for each provider in the claims data.")

with tab_prescription:
    rx_col1, rx_col2 = st.columns(2)

    top_drugs_df = load_top_prescribed_drugs(selected_year, selected_age_group, selected_state, selected_claim_type)
    rx_col1.subheader("Top 10 Most Prescribed Drugs by Volume")
    if top_drugs_df.empty:
        rx_col1.info("No prescription data for the selected filters.")
    else:
        rx_col1.bar_chart(top_drugs_df.set_index("drug_code")["prescription_volume"])

    drug_cost_time_df = load_drug_cost_over_time(selected_year, selected_age_group, selected_state, selected_claim_type)
    rx_col2.subheader("Total Drug Cost Over Time")
    if drug_cost_time_df.empty:
        rx_col2.info("No prescription data for the selected filters.")
    else:
        drug_cost_time_df["month_start"] = pd.to_datetime(drug_cost_time_df["month_start"])
        rx_col2.line_chart(drug_cost_time_df, x="month_start", y="total_drug_cost")

    st.subheader("Average Days Supply by Drug Category")
    avg_days_df = load_average_days_supply_by_category(selected_year, selected_age_group, selected_state, selected_claim_type)
    if avg_days_df.empty:
        st.info("No prescription data for the selected filters.")
    else:
        st.bar_chart(avg_days_df.set_index("drug_category")["average_days_supply"])
