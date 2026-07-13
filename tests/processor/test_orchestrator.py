"""Unit tests for idi_shareholder_tracker.orchestrator pure helpers.

Covers argument parsing/validation and date resolution. The heavier ``main()``
end-to-end wiring lives in tests/integration/test_orchestrator.py.
"""

import argparse
import datetime

import pandas as pd
import pytest

from idi_shareholder_tracker import orchestrator

ORCH = "idi_shareholder_tracker.orchestrator"
_START = datetime.date(2024, 1, 1)
_END = datetime.date(2024, 1, 31)


def _ns(**overrides):
    """Build an argparse Namespace with the fields validate_args/get_dates read."""
    base = {
        "start_date": None,
        "end_date": None,
        "daily": False,
        "look_back": None,
        "sec_bucket_prefix": "bucket/sec",
    }
    base.update(overrides)
    return argparse.Namespace(**base)


class TestValidDate:
    """Tests for the valid_date argparse type."""

    def test_parses_iso_date(self):
        assert orchestrator.valid_date("2024-03-05") == datetime.date(2024, 3, 5)

    @pytest.mark.parametrize("bad", ["2024/03/05", "not-a-date", "2024-13-01", ""])
    def test_rejects_invalid(self, bad):
        with pytest.raises(argparse.ArgumentTypeError):
            orchestrator.valid_date(bad)


class TestValidateArgs:
    """Tests for validate_args cross-field validation."""

    @pytest.fixture
    def parser(self):
        # A real parser: parser.error() prints usage and raises SystemExit.
        return argparse.ArgumentParser()

    def test_start_without_end_errors(self, parser):
        with pytest.raises(SystemExit):
            orchestrator.validate_args(_ns(start_date=_START), parser)

    def test_daily_with_end_date_errors(self, parser):
        with pytest.raises(SystemExit):
            orchestrator.validate_args(_ns(daily=True, end_date=_END), parser)

    def test_end_before_start_errors(self, parser):
        with pytest.raises(SystemExit):
            orchestrator.validate_args(_ns(start_date=_END, end_date=_START), parser)

    def test_look_back_without_daily_errors(self, parser):
        with pytest.raises(SystemExit):
            orchestrator.validate_args(_ns(look_back=5), parser)

    def test_look_back_with_range_errors(self, parser):
        with pytest.raises(SystemExit):
            orchestrator.validate_args(_ns(daily=True, look_back=5, start_date=_START), parser)

    def test_daily_applies_default_look_back(self, parser):
        args = _ns(daily=True)
        orchestrator.validate_args(args, parser)
        assert args.look_back == orchestrator.DEFAULT_LOOK_BACK

    def test_daily_keeps_explicit_look_back(self, parser):
        args = _ns(daily=True, look_back=3)
        orchestrator.validate_args(args, parser)
        assert args.look_back == 3

    def test_valid_explicit_range_passes(self, parser):
        args = _ns(start_date=_START, end_date=_END)
        orchestrator.validate_args(args, parser)  # no raise
        assert args.look_back is None


class TestGetDates:
    """Tests for get_dates range resolution."""

    def test_explicit_range_returned_as_is(self):
        args = _ns(start_date=_START, end_date=_END)
        assert orchestrator.get_dates(args) == (_START, _END)

    def test_explicit_range_does_not_read_manifest(self, mocker):
        read = mocker.patch(f"{ORCH}.pd.read_parquet")
        orchestrator.get_dates(_ns(start_date=_START, end_date=_END))
        read.assert_not_called()

    def test_daily_reads_latest_and_looks_back(self, mocker):
        manifest = pd.DataFrame(
            {"filing_date": pd.to_datetime(["2024-06-01", "2024-06-10", "2024-06-05"])}
        )
        mocker.patch(f"{ORCH}.pd.read_parquet", return_value=manifest)

        start, end = orchestrator.get_dates(_ns(daily=True, look_back=7))

        assert end == datetime.date(2024, 6, 10)
        assert start == datetime.date(2024, 6, 3)

    def test_daily_reads_from_sec_bucket_prefix(self, mocker):
        manifest = pd.DataFrame({"filing_date": pd.to_datetime(["2024-06-10"])})
        read = mocker.patch(f"{ORCH}.pd.read_parquet", return_value=manifest)

        orchestrator.get_dates(_ns(daily=True, look_back=7, sec_bucket_prefix="my-bucket/sec"))

        read.assert_called_once_with("s3://my-bucket/sec/manifest.parquet")

    def test_daily_empty_manifest_raises(self, mocker):
        manifest = pd.DataFrame({"filing_date": pd.to_datetime([pd.NaT])})
        mocker.patch(f"{ORCH}.pd.read_parquet", return_value=manifest)

        with pytest.raises(ValueError, match="no usable filing_date"):
            orchestrator.get_dates(_ns(daily=True, look_back=7))
