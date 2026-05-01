import pytest
from unittest.mock import MagicMock, patch

from extract import build_url, get_latest_data_url


BASE_URL_EXAMPLE = "https://example.com/yellow_tripdata"


# ---------------------------------------------------------------------------
# build_url
# ---------------------------------------------------------------------------

def test_build_url_zero_pads_single_digit_month(monkeypatch):
    # Remote filenames use two-digit months (_2024-01, not _2024-1).
    # A missing zero-pad produces a silently wrong URL that returns 404 at download time.
    monkeypatch.setenv("BASE_URL", BASE_URL_EXAMPLE)
    url = build_url(2024, 1)
    assert url.endswith("_2024-01.parquet")


def test_build_url_constructs_full_url(monkeypatch):
    monkeypatch.setenv("BASE_URL", BASE_URL_EXAMPLE)
    url = build_url(2026, 3)
    assert url == f"{BASE_URL_EXAMPLE}_2026-03.parquet"


def test_build_url_raises_when_base_url_is_missing(monkeypatch):
    monkeypatch.delenv("BASE_URL", raising=False)
    # build_url() calls load_dotenv() internally, which would reload BASE_URL from
    # the .env file and make this test pass even without the env var being set.
    # Patching dotenv.load_dotenv makes it a no-op so the test is hermetic.
    with patch("dotenv.load_dotenv"):
        with pytest.raises(ValueError, match="BASE_URL"):
            build_url(2026, 3)


# ---------------------------------------------------------------------------
# get_latest_data_url
# ---------------------------------------------------------------------------
# patch("extract.requests.head") is the correct patch target because extract.py
# does `import requests` at the top — we need to replace `requests` as the
# extract module sees it, not the original module in sys.modules.

def test_get_latest_data_url_returns_first_available(monkeypatch):
    monkeypatch.setenv("BASE_URL", BASE_URL_EXAMPLE)

    ok = MagicMock()
    ok.status_code = 200

    with patch("extract.requests.head", return_value=ok):
        url = get_latest_data_url()

    assert url.startswith(BASE_URL_EXAMPLE)


def test_get_latest_data_url_skips_unavailable_months(monkeypatch):
    monkeypatch.setenv("BASE_URL", BASE_URL_EXAMPLE)

    # Simulates the common upstream lag: current month not yet published (404),
    # but the previous month is available (200).
    not_found = MagicMock()
    not_found.status_code = 404
    found = MagicMock()
    found.status_code = 200

    # side_effect lets us return a different response on each successive call.
    with patch("extract.requests.head", side_effect=[not_found, found]):
        url = get_latest_data_url()

    assert url.startswith(BASE_URL_EXAMPLE)


def test_get_latest_data_url_raises_when_threshold_exhausted(monkeypatch):
    monkeypatch.setenv("BASE_URL", BASE_URL_EXAMPLE)

    not_found = MagicMock()
    not_found.status_code = 404

    # All probes return 404 — the function must stop and raise rather than loop forever.
    # max_backwards_threshold=3 keeps the test fast (3 HEAD calls instead of the default 6).
    with patch("extract.requests.head", return_value=not_found):
        with pytest.raises(RuntimeError, match="Could not find"):
            get_latest_data_url(max_backwards_threshold=3)
