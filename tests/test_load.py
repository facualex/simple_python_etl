import pandas as pd
from pathlib import Path

from load import load


def make_minimal_df():
    # load() only calls df.to_parquet(), so the DataFrame schema does not matter here.
    # One row is enough to produce a valid parquet file.
    return pd.DataFrame({"col": [1]})


def test_load_creates_output_file(tmp_path):
    # tmp_path is a pytest built-in fixture that provides a fresh temporary directory
    # for each test. Files written there are automatically cleaned up — no manual teardown.
    df = make_minimal_df()
    raw_path = Path("yellow_tripdata_2026-03.parquet")

    load((df, raw_path), destination=tmp_path)

    expected = tmp_path / "prcsd_yellow_tripdata_2026-03.parquet.gzip"
    assert expected.exists()


def test_load_derives_filename_from_raw_path(tmp_path):
    # The output filename is the lineage contract: it ties every processed file back
    # to its exact source. If this derivation changes, traceability breaks silently.
    df = make_minimal_df()
    raw_path = Path("yellow_tripdata_2025-11.parquet")

    load((df, raw_path), destination=tmp_path)

    files = list(tmp_path.iterdir())
    assert len(files) == 1
    assert files[0].name == "prcsd_yellow_tripdata_2025-11.parquet.gzip"
