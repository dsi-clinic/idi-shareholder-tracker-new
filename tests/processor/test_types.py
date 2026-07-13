"""Tests for idi_shareholder_tracker.types — config, stats, Filing, path helpers."""

import datetime
import threading

import pytest

from idi_shareholder_tracker.failures import FailureType
from idi_shareholder_tracker.types import (
    TARGET_FORM_TYPES,
    Filing,
    PipelineConfig,
    PipelineStats,
    _is_local,
)

_START_DATE = datetime.date(2024, 1, 1)
_END_DATE = datetime.date(2024, 1, 2)


class TestPipelineConfig:
    """Tests for PipelineConfig construction and validation."""

    def test_valid_config_local_files(self, tmp_path):
        config = PipelineConfig(
            failure_file=str(tmp_path / "failures.json"),
            output_file=str(tmp_path / "investments.parquet"),
            start_date=_START_DATE,
            end_date=_END_DATE,
            sec_bucket="test-bucket",
        )

        assert config.sec_bucket == "test-bucket"
        assert config.start_date == _START_DATE
        assert config.end_date == _END_DATE
        assert config.rate_limit == 0.2  # default
        assert config.num_workers == 10  # default

    def test_creates_failure_directory_if_missing(self, tmp_path):
        failure_file = tmp_path / "nonexistent_dir" / "failures.json"
        PipelineConfig(
            failure_file=str(failure_file),
            output_file=str(tmp_path / "investments.parquet"),
            start_date=_START_DATE,
            end_date=_END_DATE,
            sec_bucket="test-bucket",
        )

        assert failure_file.parent.exists()

    def test_creates_output_directory_if_missing(self, tmp_path):
        output_file = tmp_path / "new_dir" / "investments.parquet"
        PipelineConfig(
            failure_file=str(tmp_path / "failures.json"),
            output_file=str(output_file),
            start_date=_START_DATE,
            end_date=_END_DATE,
            sec_bucket="test-bucket",
        )

        assert output_file.parent.exists()

    def test_skips_validation_for_s3_paths(self, tmp_path):
        """Remote (s3://) paths must not trigger local directory creation."""
        config = PipelineConfig(
            failure_file="s3://my-bucket/failures.json",
            output_file="s3://my-bucket/investments.parquet",
            start_date=_START_DATE,
            end_date=_END_DATE,
            sec_bucket="test-bucket",
        )

        assert config.output_file == "s3://my-bucket/investments.parquet"
        assert not (tmp_path / "my-bucket").exists()

    def test_custom_rate_limit_and_num_workers(self, tmp_path):
        config = PipelineConfig(
            failure_file=str(tmp_path / "failures.json"),
            output_file=str(tmp_path / "investments.parquet"),
            start_date=_START_DATE,
            end_date=_END_DATE,
            sec_bucket="test-bucket",
            rate_limit=0.5,
            num_workers=4,
        )

        assert config.rate_limit == 0.5
        assert config.num_workers == 4


class TestIsLocal:
    """Tests for the _is_local path-scheme helper."""

    def test_local_paths_are_local(self):
        assert _is_local("/data/failures.json") is True
        assert _is_local("relative/path.parquet") is True

    @pytest.mark.parametrize(
        "path",
        [
            "s3://bucket/key.parquet",
            "https://example.com/file.json",
            "http://example.com/file.json",
            "gs://bucket/key.parquet",
        ],
    )
    def test_remote_paths_are_not_local(self, path):
        assert _is_local(path) is False


class TestFiling:
    """Tests for the Filing dataclass."""

    def test_defaults(self):
        filing = Filing(
            cik="0000320193",
            filing_date="2024-01-15",
            form_type="13F-HR",
            accession_number="0000320193-24-000001",
            primary_document="https://sec.gov/index.html",
        )

        assert filing.company_name == ""
        assert filing.exhibit_document is None

    def test_exhibit_document_is_assignable(self, make_scraped_document):
        filing = Filing(
            cik="0000320193",
            filing_date="2024-01-15",
            form_type="13F-HR",
            accession_number="0000320193-24-000001",
            primary_document="https://sec.gov/index.html",
        )
        doc = make_scraped_document()
        filing.exhibit_document = doc

        assert filing.exhibit_document is doc


class TestTargetFormTypes:
    """Tests for the TARGET_FORM_TYPES constant."""

    def test_targets_13f_hr_variants(self):
        assert TARGET_FORM_TYPES == ["13F-HR", "13F-HR/A"]


class TestPipelineStats:
    """Tests for the thread-safe PipelineStats counters."""

    def test_flat_counters_start_at_zero(self):
        stats = PipelineStats()
        assert stats.total_filings == 0
        assert stats.skipped_filings == 0
        assert stats.output_rows == 0

    def test_failures_dict_seeded_from_enum(self):
        stats = PipelineStats()
        # Every FailureType gets a counter, all starting at zero.
        assert set(stats.failures) == set(FailureType)
        assert all(count == 0 for count in stats.failures.values())

    def test_increment_flat_counter(self):
        stats = PipelineStats()
        stats.increment("total_filings")
        stats.increment("total_filings", 4)
        assert stats.total_filings == 5

    def test_increment_failure_type_routes_to_failures_dict(self):
        stats = PipelineStats()
        stats.increment(FailureType.RATE_LIMIT)
        stats.increment(FailureType.RATE_LIMIT, 2)

        assert stats.failures[FailureType.RATE_LIMIT] == 3
        # A FailureType increment must not create a same-named flat attribute.
        assert not hasattr(stats, "rate_limit")

    def test_increment_is_thread_safe(self):
        stats = PipelineStats()
        iterations = 1000
        threads = 8

        def worker():
            for _ in range(iterations):
                stats.increment("total_filings")
                stats.increment(FailureType.API_ERROR)

        workers = [threading.Thread(target=worker) for _ in range(threads)]
        for t in workers:
            t.start()
        for t in workers:
            t.join()

        assert stats.total_filings == iterations * threads
        assert stats.failures[FailureType.API_ERROR] == iterations * threads
