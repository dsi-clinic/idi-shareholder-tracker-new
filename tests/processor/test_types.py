"""Tests for idi_shareholder_tracker.types — PipelineConfig and path helpers."""

import datetime

import pytest

from idi_shareholder_tracker.types import PipelineConfig, _is_local

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
