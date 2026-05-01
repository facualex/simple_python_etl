import pandas as pd

from transform import (
    normalize_payment_type,
    remove_invalid_distances,
    remove_invalid_durations,
    remove_invalid_passengers,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
# These small builder functions keep each test focused on the assertion rather
# than on DataFrame construction. They only include the columns each function
# actually reads, so a schema change elsewhere won't break unrelated tests.


def make_passenger_df(counts):
    return pd.DataFrame({"passenger_count": counts})


def make_distance_df(distances):
    return pd.DataFrame({"trip_distance": distances})


def make_duration_df(pickups, dropoffs):
    return pd.DataFrame(
        {
            "tpep_pickup_datetime": pd.to_datetime(pickups),
            "tpep_dropoff_datetime": pd.to_datetime(dropoffs),
        }
    )


# ---------------------------------------------------------------------------
# remove_invalid_passengers
# ---------------------------------------------------------------------------


def test_remove_invalid_passengers_keeps_positive_counts():
    df = make_passenger_df([1, 2, 3])
    result = remove_invalid_passengers(df)
    assert len(result) == 3


def test_remove_invalid_passengers_drops_zero():
    # Zero passengers is not a valid trip — the check is strict (> 0, not >= 0).
    df = make_passenger_df([0, 1, 2])
    result = remove_invalid_passengers(df)
    assert len(result) == 2
    assert (result["passenger_count"] > 0).all()


def test_remove_invalid_passengers_drops_negative():
    df = make_passenger_df([-1, 1])
    result = remove_invalid_passengers(df)
    assert len(result) == 1


# ---------------------------------------------------------------------------
# remove_invalid_distances
# ---------------------------------------------------------------------------


def test_remove_invalid_distances_keeps_positive():
    df = make_distance_df([0.5, 1.0, 10.0])
    result = remove_invalid_distances(df)
    assert len(result) == 3


def test_remove_invalid_distances_drops_zero():
    # Zero distance is not a valid trip (same as passengers: strict > 0).
    df = make_distance_df([0.0, 1.5])
    result = remove_invalid_distances(df)
    assert len(result) == 1


def test_remove_invalid_distances_drops_negative():
    df = make_distance_df([-1.0, 2.0])
    result = remove_invalid_distances(df)
    assert len(result) == 1


# ---------------------------------------------------------------------------
# remove_invalid_durations
# ---------------------------------------------------------------------------


def test_remove_invalid_durations_keeps_valid_trips():
    df = make_duration_df(
        ["2024-01-01 10:00", "2024-01-01 12:00"],
        ["2024-01-01 11:00", "2024-01-01 13:00"],
    )
    result = remove_invalid_durations(df)
    assert len(result) == 2


def test_remove_invalid_durations_drops_when_dropoff_before_pickup():
    # This catches data entry errors and clock-skew artifacts in the source data.
    df = make_duration_df(
        ["2024-01-01 10:00"],
        ["2024-01-01 09:00"],
    )
    result = remove_invalid_durations(df)
    assert len(result) == 0


def test_remove_invalid_durations_drops_when_equal():
    # Pickup == dropoff means zero duration, which is not a real trip.
    df = make_duration_df(
        ["2024-01-01 10:00"],
        ["2024-01-01 10:00"],
    )
    result = remove_invalid_durations(df)
    assert len(result) == 0


# ---------------------------------------------------------------------------
# normalize_payment_type
# ---------------------------------------------------------------------------


def test_normalize_payment_type_maps_all_known_codes():
    df = pd.DataFrame({"payment_type": [1, 2, 3, 4]})
    result = normalize_payment_type(df)
    assert result["payment_type"].tolist() == [
        "credit_card",
        "cash",
        "no_charge",
        "dispute",
    ]


def test_normalize_payment_type_produces_nan_for_unknown_code():
    # pandas .map() returns NaN for keys not found in the mapping dict.
    # This is the intended behavior — unknown codes become NaN rather than
    # raising an error, so the pipeline continues and the bad value surfaces
    # in downstream null checks or reports.
    df = pd.DataFrame({"payment_type": [1, 99]})
    result = normalize_payment_type(df)
    assert result["payment_type"].iloc[0] == "credit_card"
    assert pd.isna(result["payment_type"].iloc[1])


def test_normalize_payment_type_does_not_mutate_input():
    # Each transform function explicitly works on a copy (df = df.copy()).
    # This test guards against regressions where that copy is accidentally removed,
    # which would cause silent mutations when DataFrames are shared across steps.
    df = pd.DataFrame({"payment_type": [1, 2]})
    original = df["payment_type"].tolist()
    normalize_payment_type(df)
    assert df["payment_type"].tolist() == original
