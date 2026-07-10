"""Tests for processor.types — dataclasses and PipelineStats thread safety."""

import datetime
import threading

from idi_corporate_structure.types import (
    CompanyMeta,
    Filing,
    PipelineConfig,
    PipelineStats,
    Subsidiary,
)

_START_DATE = datetime.date(2024, 1, 1)
_END_DATE = datetime.date(2024, 1, 2)


class TestPipelineConfig:
    """Tests for PipelineConfig validation."""

    def test_valid_config_local_files(self, tmp_path):
        config = PipelineConfig(
            failure_file=str(tmp_path / "failures.json"),
            output_file=str(tmp_path / "subsidiaries.parquet"),
            start_date=_START_DATE,
            end_date=_END_DATE,
            sec_bucket="test-bucket",
        )

        assert config.sec_bucket == "test-bucket"
        assert config.num_workers == 10  # default
        assert config.rate_limit == 0.2  # default

    def test_creates_failure_directory_if_missing(self, tmp_path):
        failure_file = tmp_path / "nonexistent_dir" / "failures.json"
        PipelineConfig(
            failure_file=str(failure_file),
            output_file=str(tmp_path / "subsidiaries.parquet"),
            start_date=_START_DATE,
            end_date=_END_DATE,
            sec_bucket="test-bucket",
        )

        assert failure_file.parent.exists()

    def test_creates_output_directory_if_missing(self, tmp_path):
        output_file = tmp_path / "new_dir" / "subsidiaries.parquet"
        PipelineConfig(
            failure_file=str(tmp_path / "failures.json"),
            output_file=str(output_file),
            start_date=_START_DATE,
            end_date=_END_DATE,
            sec_bucket="test-bucket",
        )

        assert output_file.parent.exists()

    def test_skips_validation_for_s3_paths(self, tmp_path):
        """S3 paths should not trigger local directory creation."""
        config = PipelineConfig(
            failure_file="s3://my-bucket/failures.json",
            output_file="s3://my-bucket/subsidiaries.parquet",
            start_date=_START_DATE,
            end_date=_END_DATE,
            sec_bucket="test-bucket",
        )

        assert config.output_file == "s3://my-bucket/subsidiaries.parquet"
        assert not (tmp_path / "my-bucket").exists()

    def test_custom_num_workers(self, tmp_path):
        config = PipelineConfig(
            failure_file=str(tmp_path / "failures.json"),
            output_file=str(tmp_path / "subsidiaries.parquet"),
            start_date=_START_DATE,
            end_date=_END_DATE,
            sec_bucket="test-bucket",
            num_workers=4,
        )
        assert config.num_workers == 4
