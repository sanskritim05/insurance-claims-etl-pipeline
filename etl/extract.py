from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

LOGGER = logging.getLogger(__name__)

SOURCE_PATTERNS = {
    "beneficiary": "*Beneficiary_Summary*.csv",
    "inpatient_claims": "*Inpatient_Claims*.csv",
    "outpatient_claims": "*Outpatient_Claims*.csv",
    "prescription_events": "*Prescription_Drug_Events*.csv",
}

SOURCE_USE_COLUMNS = {
    "beneficiary": {
        "desynpuf_id",
        "bene_birth_dt",
        "bene_seath_dt",
        "bene_death_dt",
        "bene_sex_ident_cd",
        "bene_race_cd",
        "bene_esrd_ind",
        "sp_state_code",
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
    },
    "inpatient_claims": {
        "desynpuf_id",
        "clm_id",
        "clm_from_dt",
        "clm_thru_dt",
        "prvdr_num",
        "clm_pmt_amt",
        "nch_prmry_pyr_clm_pd_amt",
        "icd9_dgns_cd_1",
        "admtng_icd9_dgns_cd",
        "icd9_prcdr_cd_1",
    },
    "outpatient_claims": {
        "desynpuf_id",
        "clm_id",
        "clm_from_dt",
        "clm_thru_dt",
        "prvdr_num",
        "clm_pmt_amt",
        "nch_prmry_pyr_clm_pd_amt",
        "icd9_dgns_cd_1",
        "admtng_icd9_dgns_cd",
        "icd9_prcdr_cd_1",
    },
    "prescription_events": {
        "desynpuf_id",
        "pde_id",
        "srvc_dt",
        "prod_srvc_id",
        "qty_dspnsd_num",
        "days_suply_num",
        "ptnt_pay_amt",
        "tot_rx_cst_amt",
    },
}

REQUIRED_SOURCE_COLUMNS = {
    "beneficiary": {"desynpuf_id", "bene_birth_dt", "bene_sex_ident_cd", "bene_race_cd", "sp_state_code"},
    "inpatient_claims": {"desynpuf_id", "clm_id", "clm_from_dt", "clm_thru_dt", "prvdr_num", "clm_pmt_amt"},
    "outpatient_claims": {"desynpuf_id", "clm_id", "clm_from_dt", "clm_thru_dt", "prvdr_num", "clm_pmt_amt"},
    "prescription_events": {"desynpuf_id", "pde_id", "srvc_dt", "prod_srvc_id", "days_suply_num", "tot_rx_cst_amt"},
}


def to_snake_case(value: str) -> str:
    cleaned = value.strip().replace("/", " ").replace("-", " ").replace(".", " ")
    tokens = [token for token in cleaned.replace("__", "_").split() if token]
    return "_".join(tokens).lower()


def _find_csv_files(data_dir: Path) -> list[Path]:
    return sorted(path for path in data_dir.glob("*.csv") if path.is_file())


def _find_source_file(data_dir: Path, pattern: str, source_name: str) -> Path:
    matches = sorted(path for path in data_dir.glob(pattern) if path.is_file())
    if not matches:
        raise FileNotFoundError(
            f"Could not find the {source_name} CSV in {data_dir.resolve()}. "
            f"Expected a file matching {pattern}."
        )
    if len(matches) > 1:
        LOGGER.warning(
            "Multiple files matched %s for %s. Using %s.",
            pattern,
            source_name,
            matches[0].name,
        )
    return matches[0]


def _validate_source_columns(source_name: str, dataframe: pd.DataFrame) -> None:
    available = set(dataframe.columns)
    required = REQUIRED_SOURCE_COLUMNS[source_name]
    missing = sorted(required - available)
    if missing:
        raise ValueError(
            f"{source_name} is missing required columns: {missing}. "
            f"Available columns: {sorted(available)}"
        )


def _read_source_csv(csv_path: Path, source_name: str) -> pd.DataFrame:
    LOGGER.info("Reading %s from %s", source_name, csv_path.resolve())
    source_use_columns = SOURCE_USE_COLUMNS[source_name]
    dataframe = pd.read_csv(
        csv_path,
        usecols=lambda column: to_snake_case(column) in source_use_columns,
        low_memory=False,
    )
    dataframe = dataframe.rename(columns={column: to_snake_case(column) for column in dataframe.columns})
    _validate_source_columns(source_name, dataframe)

    LOGGER.info("%s shape: %s", source_name, dataframe.shape)
    LOGGER.info("%s columns: %s", source_name, list(dataframe.columns))
    LOGGER.info("%s sample rows:\n%s", source_name, dataframe.head(5).to_string(index=False))
    return dataframe


def extract_datasets(data_dir: str | Path = "data") -> dict[str, pd.DataFrame]:
    data_path = Path(data_dir)
    if not data_path.exists():
        raise FileNotFoundError(f"Data directory does not exist: {data_path.resolve()}")

    available_files = _find_csv_files(data_path)
    if not available_files:
        raise FileNotFoundError(
            f"No CSV files found in {data_path.resolve()}. "
            "Place the CMS source files in the data/ folder."
        )

    dataset_bundle: dict[str, pd.DataFrame] = {}
    for source_name, pattern in SOURCE_PATTERNS.items():
        source_file = _find_source_file(data_path, pattern, source_name)
        dataset_bundle[source_name] = _read_source_csv(source_file, source_name)
    return dataset_bundle
