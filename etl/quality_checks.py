from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from great_expectations.dataset import PandasDataset

LOGGER = logging.getLogger(__name__)


class ClaimsDataset(PandasDataset):
    """Lightweight Great Expectations dataset wrapper for runtime validation."""


def _summarize_results(results: list[dict[str, Any]], dataset_name: str) -> dict[str, Any]:
    failures = [result for result in results if not result.get("success", False)]
    if failures:
        LOGGER.error("%s quality validation failed: %s", dataset_name, failures)
        raise ValueError(f"Great Expectations validation failed for {dataset_name}.")
    return {
        "dataset": dataset_name,
        "success": True,
        "expectation_count": len(results),
        "checks": results,
    }


def run_medical_quality_checks(dataset_name: str, claims_df: pd.DataFrame) -> dict[str, Any]:
    if claims_df.empty:
        raise ValueError(f"Cannot run data quality checks on an empty dataset: {dataset_name}")

    dataset = ClaimsDataset(claims_df.copy())
    results = []
    for column in [
        "claim_id",
        "patient_key",
        "provider_key",
        "start_date_key",
        "end_date_key",
        "diagnosis_key",
        "total_cost",
        "claim_start_date",
        "claim_end_date",
    ]:
        results.append(dataset.expect_column_values_to_not_be_null(column))

    results.append(dataset.expect_column_values_to_be_between("total_cost", min_value=0, strict_min=False))
    results.append(
        dataset.expect_column_values_to_be_between(
            "claim_duration_days",
            min_value=0,
            max_value=3650,
            strict_min=False,
        )
    )

    date_logic = claims_df["claim_start_date"] <= claims_df["claim_end_date"]
    results.append(
        {
            "success": bool(date_logic.all()),
            "expectation_config": {"expectation_type": "expect_start_date_before_end_date"},
            "result": {
                "unexpected_count": int((~date_logic).sum()),
                "element_count": int(len(date_logic)),
                "success_ratio": float(date_logic.mean()) if len(date_logic) else 0.0,
            },
        }
    )
    return _summarize_results(results, dataset_name)


def run_prescription_quality_checks(prescription_df: pd.DataFrame) -> dict[str, Any]:
    if prescription_df.empty:
        raise ValueError("Cannot run data quality checks on an empty prescription dataset.")

    dataset = ClaimsDataset(prescription_df.copy())
    results = []
    for column in [
        "prescription_event_id",
        "patient_key",
        "drug_key",
        "service_date_key",
        "days_supply",
        "total_drug_cost",
    ]:
        results.append(dataset.expect_column_values_to_not_be_null(column))

    results.append(dataset.expect_column_values_to_be_between("days_supply", min_value=0, strict_min=False))
    results.append(dataset.expect_column_values_to_be_between("total_drug_cost", min_value=0, strict_min=False))
    results.append(dataset.expect_column_values_to_be_between("patient_pay_amount", min_value=0, strict_min=False))
    return _summarize_results(results, "prescription_events")
