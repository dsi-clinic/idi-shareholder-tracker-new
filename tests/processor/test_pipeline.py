"""Tests for idi_shareholder_tracker.pipeline — ShareholderPipeline loading logic.

The heavy I/O boundaries (``iter_filings_by_form_type`` reading S3 and
``pandas.read_parquet`` reading the output file) are mocked so these run as fast,
network-free unit tests. The failure registry is exercised for real against a
temp-dir JSON file.
"""

import pandas as pd
import pytest

from idi_shareholder_tracker.failures import FailureType
from idi_shareholder_tracker.pipeline import ShareholderPipeline

PIPELINE = "idi_shareholder_tracker.pipeline"


@pytest.fixture
def pipeline(pipeline_config):
    """Return a ShareholderPipeline backed by temp-dir config files."""
    return ShareholderPipeline(config=pipeline_config)


class TestSelectInfoTable:
    """Tests for the _select_info_table document selector."""

    def test_selects_information_table(self, make_scraped_filing, make_scraped_document):
        info = make_scraped_document(type="INFORMATION TABLE")
        filing = make_scraped_filing(
            documents=[make_scraped_document(type="COVER PAGE"), info],
        )
        assert ShareholderPipeline._select_info_table(filing) is info

    @pytest.mark.parametrize("raw_type", ["information table", "  Information Table  "])
    def test_match_is_case_and_whitespace_insensitive(
        self, make_scraped_filing, make_scraped_document, raw_type
    ):
        info = make_scraped_document(type=raw_type)
        filing = make_scraped_filing(documents=[info])
        assert ShareholderPipeline._select_info_table(filing) is info

    def test_returns_none_when_absent(self, make_scraped_filing, make_scraped_document):
        filing = make_scraped_filing(documents=[make_scraped_document(type="COVER PAGE")])
        assert ShareholderPipeline._select_info_table(filing) is None

    def test_returns_none_for_no_documents(self, make_scraped_filing):
        filing = make_scraped_filing(documents=[])
        assert ShareholderPipeline._select_info_table(filing) is None

    def test_returns_first_match(self, make_scraped_filing, make_scraped_document):
        first = make_scraped_document(type="INFORMATION TABLE", filename="a.xml")
        second = make_scraped_document(type="INFORMATION TABLE", filename="b.xml")
        filing = make_scraped_filing(documents=[first, second])
        assert ShareholderPipeline._select_info_table(filing) is first


class TestLoadProcessedAccessions:
    """Tests for _load_processed_accessions resume support."""

    def test_returns_empty_set_when_output_missing(self, pipeline, mocker):
        mocker.patch(f"{PIPELINE}.pd.read_parquet", side_effect=FileNotFoundError("no file"))
        assert pipeline._load_processed_accessions() == set()

    def test_returns_unique_accessions(self, pipeline, mocker):
        df = pd.DataFrame({"accession_number": ["a", "b", "a", "c"]})
        mocker.patch(f"{PIPELINE}.pd.read_parquet", return_value=df)
        assert pipeline._load_processed_accessions() == {"a", "b", "c"}


class TestShouldSkip:
    """Tests for the _should_skip resume/failure gate."""

    def test_skips_already_processed(self, pipeline, make_scraped_filing):
        filing = _to_filing(make_scraped_filing(accession_number="acc-1"))
        assert pipeline._should_skip(filing, {"acc-1"}) is True

    def test_skips_previously_failed(self, pipeline, make_scraped_filing):
        filing = _to_filing(make_scraped_filing(cik="c1", accession_number="acc-1"))
        # Permanent failure -> persisted in the registry -> skipped next run.
        pipeline.failure_registry.add(("c1", "acc-1"), FailureType.NO_INFORMATION_TABLE)
        assert pipeline._should_skip(filing, set()) is True

    def test_does_not_skip_new_filing(self, pipeline, make_scraped_filing):
        filing = _to_filing(make_scraped_filing(cik="c1", accession_number="acc-1"))
        assert pipeline._should_skip(filing, {"other-acc"}) is False


class TestRecordFailure:
    """Tests for the _record_failure helper."""

    def test_increments_type_and_flat_counters_and_registers(self, pipeline):
        pipeline._record_failure(
            ("c1", "acc-1"),
            FailureType.NO_INFORMATION_TABLE,
            "warning",
            "boom %s",
            "acc-1",
        )

        assert pipeline.stats.failures[FailureType.NO_INFORMATION_TABLE] == 1
        assert pipeline.stats.failed_filings == 1
        # Permanent failure is persisted for skip-on-resume.
        assert ("c1", "acc-1") in pipeline.failure_registry

    def test_transient_failure_not_persisted(self, pipeline):
        pipeline._record_failure(
            ("c1", "acc-1"),
            FailureType.RATE_LIMIT,
            "warning",
            "rate limited",
        )
        # Counted, but a retryable failure must not be registered as do-not-retry.
        assert pipeline.stats.failures[FailureType.RATE_LIMIT] == 1
        assert ("c1", "acc-1") not in pipeline.failure_registry

    def test_custom_stat_keys(self, pipeline):
        pipeline._record_failure(
            ("c1", "acc-1"),
            FailureType.UNPARSABLE_TABLE,
            "warning",
            "bad table",
            stat_keys=("failed_filings", "dropped_holdings"),
        )
        assert pipeline.stats.failed_filings == 1
        assert pipeline.stats.dropped_holdings == 1


class TestLoadInput:
    """Tests for load_input filing selection, skipping, and failure recording."""

    def test_includes_filing_with_info_table(self, pipeline, make_scraped_filing, mocker):
        filing = make_scraped_filing(accession_number="acc-1")
        _patch_source(mocker, pipeline, filings=[filing], processed=set())

        result = pipeline.load_input()

        assert len(result) == 1
        assert result[0].accession_number == "acc-1"
        assert result[0].exhibit_document is not None
        assert pipeline.stats.total_filings == 1

    def test_skips_already_processed_filing(self, pipeline, make_scraped_filing, mocker):
        filing = make_scraped_filing(accession_number="acc-1")
        _patch_source(mocker, pipeline, filings=[filing], processed={"acc-1"})

        result = pipeline.load_input()

        assert result == []
        assert pipeline.stats.skipped_filings == 1
        assert pipeline.stats.failed_filings == 0

    def test_records_failure_when_no_info_table(
        self, pipeline, make_scraped_filing, make_scraped_document, mocker
    ):
        filing = make_scraped_filing(
            cik="c1",
            accession_number="acc-1",
            documents=[make_scraped_document(type="COVER PAGE")],
        )
        _patch_source(mocker, pipeline, filings=[filing], processed=set())

        result = pipeline.load_input()

        assert result == []
        assert pipeline.stats.failed_filings == 1
        assert pipeline.stats.failures[FailureType.NO_INFORMATION_TABLE] == 1
        assert ("c1", "acc-1") in pipeline.failure_registry

    def test_skip_takes_precedence_over_missing_info_table(
        self, pipeline, make_scraped_filing, make_scraped_document, mocker
    ):
        # A filing that is BOTH already-processed AND missing its info table must
        # be counted as skipped, not re-recorded as a failure on every run.
        filing = make_scraped_filing(
            cik="c1",
            accession_number="acc-1",
            documents=[make_scraped_document(type="COVER PAGE")],
        )
        _patch_source(mocker, pipeline, filings=[filing], processed={"acc-1"})

        result = pipeline.load_input()

        assert result == []
        assert pipeline.stats.skipped_filings == 1
        assert pipeline.stats.failed_filings == 0
        assert pipeline.stats.failures[FailureType.NO_INFORMATION_TABLE] == 0

    def test_mixed_batch(self, pipeline, make_scraped_filing, make_scraped_document, mocker):
        good = make_scraped_filing(accession_number="good")
        done = make_scraped_filing(accession_number="done")
        no_table = make_scraped_filing(
            cik="c9",
            accession_number="no-table",
            documents=[make_scraped_document(type="COVER PAGE")],
        )
        _patch_source(mocker, pipeline, filings=[good, done, no_table], processed={"done"})

        result = pipeline.load_input()

        assert [f.accession_number for f in result] == ["good"]
        assert pipeline.stats.total_filings == 3
        assert pipeline.stats.skipped_filings == 1
        assert pipeline.stats.failed_filings == 1

    def test_passes_target_form_types_and_bucket(self, pipeline, make_scraped_filing, mocker):
        mocker.patch.object(pipeline, "_load_processed_accessions", return_value=set())
        iter_mock = mocker.patch(f"{PIPELINE}.iter_filings_by_form_type", return_value=[])

        pipeline.load_input()

        _, kwargs = iter_mock.call_args
        assert kwargs["form_types"] == ["13F-HR", "13F-HR/A"]
        assert kwargs["bucket"] == "test-bucket"
        assert kwargs["include_failures"] is True
        assert kwargs["start_date"] == pipeline.config.start_date
        assert kwargs["end_date"] == pipeline.config.end_date


class TestRun:
    """Tests for the ShareholderPipeline.run lifecycle."""

    def test_flushes_failures_even_when_no_input(self, pipeline, mocker):
        mocker.patch.object(pipeline, "load_input", return_value=[])
        flush = mocker.patch.object(pipeline.failure_registry, "flush")
        process = mocker.patch.object(pipeline, "process")

        pipeline.run()

        # No input -> process() is skipped, but the registry is still flushed.
        process.assert_not_called()
        flush.assert_called_once()

    def test_flushes_failures_on_exception(self, pipeline, mocker):
        mocker.patch.object(pipeline, "load_input", side_effect=RuntimeError("boom"))
        flush = mocker.patch.object(pipeline.failure_registry, "flush")

        with pytest.raises(RuntimeError, match="boom"):
            pipeline.run()

        flush.assert_called_once()


# --- helpers -----------------------------------------------------------------


def _to_filing(scraped_filing):
    """Build a Filing from a ScrapedFiling the way load_input does (for gate tests)."""
    from idi_shareholder_tracker.types import Filing

    return Filing(
        cik=scraped_filing.cik,
        filing_date=scraped_filing.filing_date,
        form_type=scraped_filing.form_type,
        accession_number=scraped_filing.accession_number,
        primary_document=scraped_filing.index_url,
        company_name=scraped_filing.company_name,
    )


def _patch_source(mocker, pipeline, *, filings, processed):
    """Patch the two I/O boundaries load_input depends on."""
    mocker.patch.object(pipeline, "_load_processed_accessions", return_value=processed)
    mocker.patch(f"{PIPELINE}.iter_filings_by_form_type", return_value=iter(filings))
