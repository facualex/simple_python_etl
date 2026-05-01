import pandera.pandas as pa
from pandera import Check, Column, DataFrameSchema

# After we read the raw Parquet, and before heavy transforms
RAW_SCHEMA = DataFrameSchema(
    {
        # ... we only add the columns we truly depend on for the transformation phase
        "VendorID": Column(int, required=False, nullable=True),
        "tpep_pickup_datetime": Column(pa.DateTime, nullable=False),
        "tpep_dropoff_datetime": Column(pa.DateTime, nullable=False),
        "passenger_count": Column(float, nullable=True, checks=Check.ge(0)),
        "trip_distance": Column(float, nullable=True, checks=Check.ge(0)),
        "fare_amount": Column(float, nullable=True),
    },
    strict=False,  # True if you want to reject unexpected columns
    coerce=True,  # try to coerce dtypes before checks run
)

# After feature engineering, before load
PROCESSED_SCHEMA = DataFrameSchema(
    {
        "tpep_pickup_datetime": Column(pa.DateTime, nullable=False),
        "trip_duration_minutes": Column(float, nullable=False, checks=Check.gt(0)),
        "hour_of_day": Column(int, nullable=False, checks=Check.in_range(0, 23)),
        "day_of_week": Column(
            str,
            nullable=False,
            checks=Check.isin(
                [
                    "Monday",
                    "Tuesday",
                    "Wednesday",
                    "Thursday",
                    "Friday",
                    "Saturday",
                    "Sunday",
                ]
            ),
        ),
        "payment_type": Column(str, nullable=True),
    },
    strict=False,
    coerce=True,
)
