"""Transform step of the ETL pipeline.

This module reads the raw NYC Taxi parquet dataset and produces a cleaned and
feature-enriched dataframe ready to be persisted in the processed zone.
"""

import logging
from pathlib import Path
from typing import Tuple

import pandas as pd
import pandera.errors as errors

from schemas import PROCESSED_SCHEMA, RAW_SCHEMA
from utils import SchemaValidationFailed, _summarize_pandera_exception

logger = logging.getLogger(__name__)


def remove_invalid_passengers(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows with non-positive passenger counts."""
    return df[df["passenger_count"] > 0]


def remove_invalid_distances(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows with non-positive trip distances."""
    return df[df["trip_distance"] > 0]


def remove_invalid_durations(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows where dropoff happens before pickup."""
    return df[df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"]]


def clean_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict[str, int]]:
    """Apply basic data quality filters and return the cleaned df with per-rule drop counts."""
    before = len(df)

    df = remove_invalid_passengers(df)
    after_passengers = len(df)

    df = remove_invalid_distances(df)
    after_distances = len(df)

    df = remove_invalid_durations(df)
    after_durations = len(df)

    dropped = {
        "invalid_passengers": before - after_passengers,
        "invalid_distances": after_passengers - after_distances,
        "invalid_durations": after_distances - after_durations,
    }

    return df, dropped


def add_trip_duration(df: pd.DataFrame) -> pd.DataFrame:
    """Add trip duration in minutes as `trip_duration_minutes`."""
    df = df.copy()

    df["trip_duration_minutes"] = (
        df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60

    return df


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add pickup hour and weekday name features."""
    df = df.copy()

    df["hour_of_day"] = df["tpep_pickup_datetime"].dt.hour
    df["day_of_week"] = df["tpep_pickup_datetime"].dt.day_name()

    return df


def normalize_payment_type(df: pd.DataFrame) -> pd.DataFrame:
    """Map numeric `payment_type` codes into human-readable labels."""
    mapping = {1: "credit_card", 2: "cash", 3: "no_charge", 4: "dispute"}

    df = df.copy()

    df["payment_type"] = df["payment_type"].map(mapping)

    return df


def transform(data_path: Path) -> Tuple[pd.DataFrame, Path, int, dict[str, int]]:
    """Read raw parquet, clean it, and add engineered features.

    Args:
        data_path: Path to the raw parquet file.

    Returns:
        A tuple `(transformed_df, data_path)` so downstream steps can keep track
        of the source file name.

    Raises:
        OSError: If the parquet file cannot be read.
        SchemaValidationFailed: If Pandera schema validation fails (message is a short summary).
    """
    logger.info(f"Transforming data: {data_path}...")

    try:
        original_df = RAW_SCHEMA.validate(pd.read_parquet(data_path), lazy=True)
        rows_in = len(original_df)
        logger.info("Schema validation succesful after reading source file.")
        logger.info("Data succesfully read.")
    except (errors.SchemaError, errors.SchemaErrors) as e:
        summary = _summarize_pandera_exception(e)
        logger.error("Schema validation failed after reading parquet.\n%s", summary)
        raise SchemaValidationFailed(summary) from None

    try:
        clean_df, dropped = clean_data(original_df)
        clean_df = RAW_SCHEMA.validate(clean_df, lazy=True)
        logger.info("Schema validation succesful after cleaning.")
        logger.info("Data succesfully cleaned.")
    except (errors.SchemaError, errors.SchemaErrors) as e:
        summary = _summarize_pandera_exception(e)
        logger.error("Schema validation failed after cleaning.\n%s", summary)
        raise SchemaValidationFailed(summary) from None

    transformed_df = add_trip_duration(clean_df)
    transformed_df = add_time_features(transformed_df)
    transformed_df = normalize_payment_type(transformed_df)

    try:
        transformed_df = PROCESSED_SCHEMA.validate(transformed_df)
    except (errors.SchemaError, errors.SchemaErrors) as e:
        summary = _summarize_pandera_exception(e)
        logger.error("Schema validation failed on processed dataframe.\n%s", summary)
        raise SchemaValidationFailed(summary) from None

    logger.info(f"Data succesfully transformed: {data_path}")

    return (transformed_df, data_path, rows_in, dropped)
