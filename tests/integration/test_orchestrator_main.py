"""Integration tests for orchestrator.main end-to-end argument wiring.

These exercise the full CLI path — argv parsing, validation, date resolution,
``PipelineConfig`` construction, and pipeline dispatch — with only the pipeline's
run boundary mocked (no S3, no network). Marked ``integration`` because they wire
several modules together rather than testing one unit in isolation.
"""

import datetime

import pandas as pd
import pytest

from idi_shareholder_tracker import orchestrator
from idi_shareholder_tracker.types import PipelineConfig

pytestmark = pytest.mark.integration

ORCH = "idi_shareholder_tracker.orchestrator"


@pytest.fixture
def pipeline_cls(mocker):
    """Patch ShareholderPipeline so main() never touches S3, and capture the config."""
    return mocker.patch(f"{ORCH}.ShareholderPipeline")


def _argv(tmp_path, *extra):
    return [
        "shareholder-tracker",
        "--output-file",
        str(tmp_path / "out.parquet"),
        "--failure-file",
        str(tmp_path / "failures.json"),
        "--sec-bucket-prefix",
        "my-bucket/sec",
        *extra,
    ]


class TestMainExplicitRange:
    """main() driven by an explicit --start-date/--end-date range."""

    def test_builds_config_and_runs_pipeline(self, tmp_path, pipeline_cls, monkeypatch):
        monkeypatch.setattr(
            "sys.argv",
            _argv(tmp_path, "--start-date", "2024-01-01", "--end-date", "2024-01-31"),
        )

        orchestrator.main()

        # Pipeline constructed once with a PipelineConfig, then run() called.
        pipeline_cls.assert_called_once()
        config = pipeline_cls.call_args.kwargs["config"]
        assert isinstance(config, PipelineConfig)
        assert config.start_date == datetime.date(2024, 1, 1)
        assert config.end_date == datetime.date(2024, 1, 31)
        # sec_bucket is the bucket portion of the prefix (before the first slash).
        assert config.sec_bucket == "my-bucket"
        pipeline_cls.return_value.run.assert_called_once()

    def test_forwards_tuning_flags(self, tmp_path, pipeline_cls, monkeypatch):
        monkeypatch.setattr(
            "sys.argv",
            _argv(
                tmp_path,
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-31",
                "--rate-limit",
                "0.5",
                "--num-workers",
                "4",
                "--fail-flush-every",
                "25",
            ),
        )

        orchestrator.main()

        config = pipeline_cls.call_args.kwargs["config"]
        assert config.rate_limit == 0.5
        assert config.num_workers == 4
        assert config.failure_flush_every == 25


class TestMainDailyMode:
    """main() driven by --daily, resolving the range from the manifest."""

    def test_daily_resolves_range_from_manifest(self, tmp_path, pipeline_cls, monkeypatch, mocker):
        manifest = pd.DataFrame({"filing_date": pd.to_datetime(["2024-06-10"])})
        mocker.patch(f"{ORCH}.pd.read_parquet", return_value=manifest)
        monkeypatch.setattr("sys.argv", _argv(tmp_path, "--daily"))

        orchestrator.main()

        config = pipeline_cls.call_args.kwargs["config"]
        assert config.end_date == datetime.date(2024, 6, 10)
        # Default look-back is 7 days.
        assert config.start_date == datetime.date(2024, 6, 3)


class TestMainArgErrors:
    """main() surfaces argparse validation failures as SystemExit."""

    def test_missing_mode_exits(self, tmp_path, monkeypatch):
        # Neither --daily nor --start-date supplied (mutually exclusive, required).
        monkeypatch.setattr("sys.argv", _argv(tmp_path))
        with pytest.raises(SystemExit):
            orchestrator.main()

    def test_daily_with_end_date_exits(self, tmp_path, monkeypatch):
        monkeypatch.setattr("sys.argv", _argv(tmp_path, "--daily", "--end-date", "2024-01-31"))
        with pytest.raises(SystemExit):
            orchestrator.main()
