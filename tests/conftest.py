"""Shared pytest fixtures and factories for the shareholder-tracker test suite."""

import datetime

import pytest
from idi_ftm2j_shared.types import ScrapedDocument, ScrapedFiling

from idi_shareholder_tracker.types import PipelineConfig

_START_DATE = datetime.date(2024, 1, 1)
_END_DATE = datetime.date(2024, 1, 31)


@pytest.fixture
def make_scraped_document():
    """Return a factory that builds ``ScrapedDocument`` instances with sane defaults."""

    def _make(
        *,
        filename="info.xml",
        url="https://sec.gov/info.xml",
        description="",
        type="INFORMATION TABLE",
        seq="1",
        s3_key="sec/key/info.xml",
    ):
        return ScrapedDocument(
            filename=filename,
            url=url,
            description=description,
            type=type,
            seq=seq,
            s3_key=s3_key,
        )

    return _make


@pytest.fixture
def make_scraped_filing(make_scraped_document):
    """Return a factory that builds ``ScrapedFiling`` manifests with sane defaults.

    By default the filing carries a single INFORMATION TABLE document. Pass
    ``documents=[]`` for a filing with no exhibit, or supply your own list.
    """

    def _make(
        *,
        cik="0000320193",
        accession_number="0000320193-24-000001",
        form_type="13F-HR",
        filing_date="2024-01-15",
        last_scraped_at="2024-01-16T00:00:00Z",
        index_url="https://sec.gov/index.html",
        company_name="Example Capital LLC",
        failure_reason="",
        documents=None,
    ):
        if documents is None:
            documents = [make_scraped_document()]
        return ScrapedFiling(
            cik=cik,
            accession_number=accession_number,
            form_type=form_type,
            filing_date=filing_date,
            last_scraped_at=last_scraped_at,
            index_url=index_url,
            company_name=company_name,
            failure_reason=failure_reason,
            documents=documents,
        )

    return _make


@pytest.fixture
def pipeline_config(tmp_path):
    """Return a ``PipelineConfig`` pointed at temp-dir local files."""
    return PipelineConfig(
        failure_file=str(tmp_path / "failures.json"),
        output_file=str(tmp_path / "investments.parquet"),
        start_date=_START_DATE,
        end_date=_END_DATE,
        sec_bucket="test-bucket",
        failure_flush_every=50,
    )
