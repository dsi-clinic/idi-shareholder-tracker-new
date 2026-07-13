"""Shared data types and configuration for the shareholder-tracker processor.

Defines the dataclasses passed between pipeline stages -- notably
``PipelineConfig`` (input/output paths, SEC bucket, date range, static-file
locations, rate limit, worker count, cache settings) and ``PipelineStats``
(thread-safe run counters) -- plus any enums and constants (e.g. the target form
type ``13F-HR``) used across the package.
"""

# Standard application imports
import datetime
import pathlib
import threading
from dataclasses import dataclass

# Third party imports
from idi_ftm2j_shared.sec import ScrapedDocument

# Application imports
from idi_shareholder_tracker.failures import FailureType

_REMOTE_SCHEMES = ("s3://", "https://", "http://", "gs://")
TARGET_FORM_TYPES = ["13F-HR", "13F-HR/A"]


def _is_local(path: str) -> bool:
    """Return True if the path refers to a local filesystem location.

    A path is considered remote when it begins with one of the known URI
    schemes in ``_REMOTE_SCHEMES`` (``s3://``, ``https://``, ``http://``,
    ``gs://``).

    Args:
        path: File path or URI string to test.

    Returns:
        True if the path does not start with a known remote scheme,
        False otherwise.
    """
    return not path.startswith(_REMOTE_SCHEMES)


@dataclass
class Filing:
    """Represents a single SEC 10-K filing with its metadata and document URLs."""

    cik: str
    filing_date: str
    form_type: str
    accession_number: str
    primary_document: str
    company_name: str = ""
    exhibit_document: ScrapedDocument | None = None


@dataclass
class TableData:
    """Represents the extracted raw info table contents paired with URL."""

    url: str
    raw_info_table: bytes


@dataclass
class PipelineConfig:
    """Configuration for the subsidiary pipeline."""

    failure_file: str
    output_file: str
    start_date: datetime.date
    end_date: datetime.date
    sec_bucket: str
    rate_limit: float = 0.2
    num_workers: int = 10
    failure_flush_every: int = 50

    def __post_init__(self) -> None:
        """Validate existence of local files."""
        if _is_local(self.failure_file) and not pathlib.Path(self.failure_file).parent.exists():
            pathlib.Path(self.failure_file).parent.mkdir(parents=True, exist_ok=True)
        if _is_local(self.output_file) and not pathlib.Path(self.output_file).parent.exists():
            pathlib.Path(self.output_file).parent.mkdir(parents=True, exist_ok=True)


@dataclass
class PipelineStats:
    """Thread-safe counters tracking pipeline progress and failures.

    Progress and data-quality counters are flat integer fields. Every failure
    mode instead lives in the ``failures`` dict, seeded from :class:`FailureType`
    so each type is guaranteed a counter and the two can never drift apart -- this
    lets ``display_stats`` iterate the enum to audit a run. Bump either kind of
    counter through :meth:`increment` -- pass a field name for a flat counter or a
    :class:`FailureType` for a failure counter.
    """

    # --- Filing level (one 13F-HR filing / accession) ---
    total_filings: int = 0  # loaded from S3 for the date range
    skipped_filings: int = 0  # already in output parquet (resume)
    queued_filings: int = 0  # queued for processing, info table extraction
    processed_filings: int = 0  # produced >=1 holding row
    failed_filings: int = 0  # skipped this run due to a failure (any type)

    # --- Document selection (cover page is supplementary, so not a FailureType) ---
    missing_cover_page: int = 0  # no cover-page doc; holdings still emitted
    unparsable_cover_page: int = 0  # cover-page failed to parse; holdings still emitted

    # --- Holding level (StockResult) ---
    total_holdings: int = 0  # holdings parsed across all filings
    dropped_holdings: int = 0  # dropped as invalid/duplicate in normalize
    missing_value: int = 0  # holding missing value_x1000
    missing_shares: int = 0  # holding missing shares/prn amount

    # --- Manager resolution (cover-page numbers -> names) ---
    total_managers: int = 0  # ManagerResult entries parsed
    unresolved_manager_numbers: int = 0  # other-manager # with no name on cover page

    # --- Normalization grounding ---
    unmapped_place_codes: int = 0  # EDGAR state/country code with no mapping

    # --- Enrichment (static sources merged in) ---
    nbim_rows: int = 0  # rows merged from NBIM
    pension_fund_rows: int = 0  # rows merged from pension funds
    aggregated_securities: int = 0  # distinct securities after aggregate-by-security

    # --- Output ---
    output_rows: int = 0  # rows written to Parquet

    def __post_init__(self) -> None:
        """Initialize the lock and one counter per failure type."""
        self._lock = threading.Lock()
        # Seeded from the enum so every FailureType has a counter and adding a
        # new type automatically adds its counter -- no hand-maintained fields.
        self.failures: dict[FailureType, int] = dict.fromkeys(FailureType, 0)

    def increment(self, field: str | FailureType, n: int = 1) -> None:
        """Increment a counter by ``n``.

        Accepts either a flat progress-counter name (e.g. ``"total_filings"``) or
        a :class:`FailureType` member, so the same call bumps both kinds of stat.
        FailureType values are routed to the enum-seeded ``failures`` dict.

        Args:
            field: A flat progress-counter attribute name, or a FailureType.
            n: The amount to increment by.
        """
        with self._lock:
            if isinstance(field, FailureType):
                self.failures[field] += n
            else:
                setattr(self, field, getattr(self, field) + n)
