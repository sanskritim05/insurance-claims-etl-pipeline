# insurance-claims-etl-pipeline

## Overview

`insurance-claims-etl-pipeline` is a local Python ETL project built on CMS synthetic Medicare sample data. It extracts four source files, transforms them into a clean analytics model, validates the output with Great Expectations, and loads the results into a SQLite warehouse for analysis in Streamlit.

The project is designed to clearly demonstrate a real ETL workflow:

- `Extract`: read beneficiary, inpatient, outpatient, and prescription event source files from the local `data/` folder
- `Transform`: standardize dates and identifiers, derive patient and claim metrics, model dimensions and facts, and run data quality checks
- `Load`: write the final warehouse tables into `claims_warehouse.db`

## Data Sources

Place these CMS files in the `data/` directory:

- `2008 Beneficiary Summary File`
  Contains patient demographics, age, gender, race, ESRD indicator, and chronic condition flags
- `Inpatient Claims`
  Contains hospital stays, diagnosis codes, procedure codes, claim dates, and claim costs
- `Outpatient Claims`
  Contains outpatient visits, diagnosis codes, claim dates, and claim costs
- `Prescription Drug Events`
  Contains drug codes, quantity dispensed, days supply, patient payment, and total drug cost

## Project Structure

```text
insurance-claims-etl-pipeline/
├── data/
│   └── .gitkeep
├── etl/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── quality_checks.py
├── dashboard/
│   └── app.py
├── run_pipeline.py
├── requirements.txt
├── README.md
└── .gitignore
```

Generated artifacts such as the SQLite database and processed CSV outputs are created only after the pipeline runs and are intentionally excluded from source control.

## Warehouse Design

The pipeline builds a local star-schema style warehouse with these tables:

### Dimensions

- `dim_patient`
  Patient demographics, age, age group, race, state, ESRD indicator, and chronic condition flags
- `dim_provider`
  Provider identifier, provider type, and inferred provider state
- `dim_date`
  Calendar dimension for analysis by year, quarter, month, and day
- `dim_diagnosis`
  ICD diagnosis code and description
- `dim_drug`
  Drug code, derived drug name, and derived drug category

### Facts

- `fact_inpatient_claims`
  One row per inpatient claim
- `fact_outpatient_claims`
  One row per outpatient claim
- `fact_prescriptions`
  One row per prescription event

## Features

### ETL Pipeline

- Multi-source extraction from four CMS files
- Cleaning and standardization of source fields
- Derived metrics such as:
  - age group
  - chronic condition count
  - claim duration in days
  - cost per day
- Great Expectations validation for medical claims and prescription events
- Local SQLite warehouse loading with a reproducible rebuild process

### Streamlit Dashboard

- Always-visible KPI cards
- Global filters for:
  - year
  - claim type
  - age group
  - state
- Tabbed analysis for:
  - patient insights
  - claims analysis
  - diagnosis analysis
  - provider analysis
  - prescription analysis

## Setup

### 1. Create a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

### 2. Install dependencies

```bash
python -m pip install -r requirements.txt
```

### 3. Confirm the CMS files are present

```bash
python run_pipeline.py --list-files
```

## Running the ETL Pipeline

From the project root:

```bash
python run_pipeline.py
```

This creates:

- `claims_warehouse.db`
- processed CSV exports in `data/processed/`
- transformation metadata in `data/processed/transform_metadata.json`

## Running the Dashboard

After the ETL completes:

```bash
python -m streamlit run dashboard/app.py
```

## Dashboard Views

### KPI Cards

- Total number of claims
- Total cost across all claims
- Average cost per claim
- Average length of hospital stay in days
- Total unique patients
- Total unique providers

### Patient Tab

- Age group distribution
- Gender breakdown
- Top states by claim volume
- Chronic condition prevalence
- Average cost by age group

### Claims Tab

- Claim volume by month
- Inpatient vs outpatient split
- Average claim duration by claim type
- Cost distribution histogram
- Top 10 most expensive individual claims

### Diagnosis Tab

- Top diagnoses by total cost
- Top diagnoses by claim count
- Average cost per diagnosis
- Searchable ICD lookup table

### Provider Tab

- Top providers by claims volume
- Top providers by total cost
- Provider state breakdown

### Prescription Tab

- Top prescribed drugs by volume
- Total drug cost over time
- Average days supply by drug category

## Example SQL Queries

### Top inpatient diagnoses by total cost

```sql
SELECT
    dg.icd_code,
    dg.icd_description,
    ROUND(SUM(fi.total_cost), 2) AS total_inpatient_cost
FROM fact_inpatient_claims fi
JOIN dim_diagnosis dg ON fi.diagnosis_key = dg.diagnosis_key
GROUP BY dg.icd_code, dg.icd_description
ORDER BY total_inpatient_cost DESC
LIMIT 10;
```

### Outpatient claim counts by month

```sql
SELECT
    dd.year,
    dd.month,
    COUNT(*) AS outpatient_claim_count
FROM fact_outpatient_claims fo
JOIN dim_date dd ON fo.start_date_key = dd.date_key
GROUP BY dd.year, dd.month
ORDER BY dd.year, dd.month;
```

### Top drugs by total cost

```sql
SELECT
    dr.drug_code,
    dr.drug_name,
    ROUND(SUM(fp.total_drug_cost), 2) AS total_drug_cost
FROM fact_prescriptions fp
JOIN dim_drug dr ON fp.drug_key = dr.drug_key
GROUP BY dr.drug_code, dr.drug_name
ORDER BY total_drug_cost DESC
LIMIT 10;
```

### Patients with the highest chronic condition burden

```sql
SELECT
    patient_id,
    age_group,
    gender,
    race,
    chronic_conditions_count,
    chronic_conditions_summary
FROM dim_patient
ORDER BY chronic_conditions_count DESC
LIMIT 10;
```

## Notes

- Everything runs locally with free, open source tools only.
- No API keys, no cloud services, and no paid infrastructure are required.
- `provider_state` is inferred from the most common patient state associated with each provider in the claims data.
- `drug_name` and `drug_category` are derived from the available product service code because the CMS sample does not ship with a user-friendly drug catalog.
