"""Pipeline that builds the shareholder-tracker release from SEC 13F-HR filings.

Subclasses the shared ``Pipeline`` ABC (``load_input`` -> ``process`` ->
``save_output`` -> ``display_stats``). Reads already-scraped 13F-HR filings from
the shared S3 bucket via ``iter_filings_by_form_type``, parses each filing's
information table and cover page, resolves each holding's "other manager" numbers
to names, enriches by merging the static NBIM and pension-fund sources, and writes
the normalized release to Parquet. Uses the shared ``FailureRegistry`` for
resumable, failure-aware runs.
"""

# Standard application imports
import threading
from abc import ABC, abstractmethod

# Third party imports
import pandas as pd
from idi_ftm2j_shared.failures import FailureRegistry
from idi_ftm2j_shared.logs import get_logger
from idi_ftm2j_shared.sec import ScrapedDocument, ScrapedFiling, iter_filings_by_form_type

# Application imports
from idi_shareholder_tracker.failures import FailureType, ShareholderFailureClassifier
from idi_shareholder_tracker.types import TARGET_FORM_TYPES, Filing, PipelineConfig, PipelineStats


class CompanyMetaFetchError(Exception):
    """Raised when the SEC submissions request for a CIK fails.

    Signals a failed request (as opposed to a successful-but-sparse response) so
    callers can skip the filing and retry it on a later run instead of caching and
    persisting a blank ``CompanyMeta``.
    """


class Pipeline(ABC):
    """Baseline class for processing pipelines."""

    def __init__(
        self,
        config: PipelineConfig,
    ) -> None:
        """Initialize the pipeline with config.

        Args:
            config: Pipeline configuration including input/output paths and tuning
                parameters.
        """
        self.config = config
        self.stats = PipelineStats()
        self.logger = get_logger(type(self).__name__)

    @abstractmethod
    def load_input(self) -> list:
        """Load input data and return a list of items to process.

        Returns:
            List of input items. The concrete element type is defined by each
            subclass (e.g. ``list[Filing]``).
        """
        ...

    @abstractmethod
    def process(self, input_list: list) -> list:
        """Process each item in the input list and return a list of results.

        Args:
            input_list: Items returned by :meth:`load_input`.

        Returns:
            List of processed results. The concrete element type is defined by
            each subclass (e.g. ``list[Subsidiary]``).
        """
        ...

    @abstractmethod
    def save_output(self, processed_list: list) -> None:
        """Persist the processed results to the configured output destination.

        Args:
            processed_list: Items returned by :meth:`process`.

        Returns:
            None
        """
        ...

    @abstractmethod
    def display_stats(self) -> None:
        """Log or display a summary of pipeline processing statistics.

        Returns:
            None
        """

    def run(self) -> None:
        """Execute the full pipeline: load → process → save → display stats.

        Calls :meth:`load_input`, :meth:`process`, :meth:`save_output`, and
        :meth:`display_stats` in sequence, then logs the total elapsed time.

        Returns:
            None
        """
        input_data = self.load_input()
        self.logger.info("Located %d filings with exhibits to process", len(input_data))

        if input_data:
            results = self.process(input_data)
            self.save_output(results)
        else:
            self.logger.info("No input data found, skipping pipeline")

        self.display_stats()


class ShareholderPipeline(Pipeline):
    """Pipeline that fetches Exhibit 13F-HR filings from SEC EDGAR and extracts shareholder data."""

    def __init__(self, config: PipelineConfig) -> None:
        """Initialize the subsidiary pipeline with failure registry.

        Args:
            config: Pipeline configuration including input/output paths, rate limit,
                worker count, and failure flush threshold.
        """
        super().__init__(config)
        self.failure_registry = FailureRegistry(
            config.failure_file,
            classifier=ShareholderFailureClassifier(),
            flush_every=config.failure_flush_every,
        )
        self._results_lock = threading.Lock()
        self.rows = []

    def _load_processed_accessions(self) -> set[str]:
        """Return accession numbers already present in the output parquet file.

        Returns:
            Set of accession numbers, or an empty set if the output file
            does not exist yet.
        """
        try:
            output_df = pd.read_parquet(self.config.output_file, columns=["accession_number"])
        except FileNotFoundError:
            return set()
        return set(output_df["accession_number"].unique())

    def _record_failure(
        self,
        key: tuple[str, str],
        failure_type: FailureType,
        log_level: str,
        message: str,
        *log_args: object,
        stat_keys: tuple[str, ...] = ("failed_filings",),
    ) -> None:
        """Log a failure, increment stats, and register it in the failure registry.

        Args:
            key: Registry key tuple, typically ``(cik, accession_number)``.
            failure_type: Classified failure type. Counted per type and, if
                permanent, persisted to the registry.
            log_level: Logger method name (``"warning"``, ``"error"``, or
                ``"exception"``). ``"exception"`` behaves like ``"error"`` but
                also attaches the current traceback — only valid inside an ``except``.
            message: ``%s``-style log message.
            *log_args: Arguments to substitute into ``message``.
            stat_keys: Extra flat progress counters to increment (default
                ``("failed_filings",)``). Do NOT list the failure type here —
                it is counted automatically.
        """
        getattr(self.logger, log_level)(message, *log_args)
        self.stats.increment(failure_type)  # <-- per-type counter (FailureType branch)
        for key_ in stat_keys:  # extra flat progress counters
            self.stats.increment(key_)
        self.failure_registry.add(key, failure_type)

    @staticmethod
    def _select_info_table(scraped_filing: ScrapedFiling) -> ScrapedDocument | None:
        """Return the scraped documents matching the filing's exhibit type.

        Args:
            scraped_filing: Manifest whose documents are filtered.

        Returns:
            Documents whose ``type`` equals "INFORMATION TABLE"
        """
        info_table = next(
            (
                d
                for d in scraped_filing.documents
                if (d.type or "").strip().upper() == "INFORMATION TABLE"
            ),
            None,
        )
        return info_table

    def _should_skip(self, filing: Filing, processed_accessions: set[str]) -> bool:
        """Return True if the filing was already processed or previously failed.

        Args:
            filing: Filing being considered for processing.
            processed_accessions: Accession numbers already present in the
                output file.

        Returns:
            True if the filing's accession number is in
            ``processed_accessions`` or already recorded in the failure
            registry.
        """
        return (
            filing.accession_number in processed_accessions
            or (filing.cik, filing.accession_number) in self.failure_registry
        )

    def load_input(self) -> list[Filing]:
        """Load input data from the SEC and return a list of filings.

        Filings with no matching information-table document are recorded as
        ``NO_INFORMATION_TABLE`` failures and excluded from the returned list, so
        the count reflects filings that actually have exhibit content to fetch.
        Filings already present in the output file or the failure registry are
        skipped before this check.

        Returns:
            A list of Filing objects
        """
        processed_accessions = self._load_processed_accessions()

        scraped_filings = iter_filings_by_form_type(
            form_types=TARGET_FORM_TYPES,
            start_date=self.config.start_date,
            end_date=self.config.end_date,
            bucket=self.config.sec_bucket,
            include_failures=True,
        )

        filings = []
        for scraped_filing in scraped_filings:
            self.stats.increment("total_filings")
            filing = Filing(
                cik=scraped_filing.cik,
                filing_date=scraped_filing.filing_date,
                form_type=scraped_filing.form_type,
                accession_number=scraped_filing.accession_number,
                primary_document=scraped_filing.index_url,
                company_name=scraped_filing.company_name,
            )
            filing.exhibit_document = self._select_info_table(scraped_filing)

            if self._should_skip(filing, processed_accessions):
                self.stats.increment("skipped_filings")
                continue

            if not filing.exhibit_document:
                self._record_failure(
                    (scraped_filing.cik, scraped_filing.accession_number),
                    FailureType.NO_INFORMATION_TABLE,
                    "warning",
                    "No information table found for filing: %s - %s - %s (%s)",
                    scraped_filing.cik,
                    scraped_filing.accession_number,
                    scraped_filing.filing_date,
                    scraped_filing.index_url,
                    stat_keys=("failed_filings",),
                )
                continue

            filings.append(filing)

        return filings

    def process(self, input_list: list) -> list:
        """Process each item in the input list and return a list of results.

        Args:
            input_list: Items returned by :meth:`load_input`.

        Returns:
            List of processed results. The concrete element type is defined by
            each subclass (e.g. ``list[Subsidiary]``).
        """
        return []

    def save_output(self, processed_list: list) -> None:
        """Persist the processed results to the configured output destination.

        Args:
            processed_list: Items returned by :meth:`process`.

        Returns:
            None
        """
        ...

    def display_stats(self) -> None:
        """Log a formatted summary of pipeline statistics on completion.

        Writes filing, holding, manager, enrichment, and output totals, then a
        per-``FailureType`` audit grouped by retry policy and reconciled against
        the failure registry.

        Returns:
            None
        """
        s = self.stats
        log = self.logger.info
        bar = "=" * 44

        def row(label: str, value: int) -> None:
            log("    %-24s %s", label + ":", f"{value:,}")

        log(bar)
        log("Shareholder Pipeline Stats")
        log(bar)

        log("  Filings")
        row("Total", s.total_filings)
        row("Skipped (already done)", s.skipped_filings)
        row("Processed", s.processed_filings)
        row("Failed", s.failed_filings)

        log("  Documents")
        row("Missing cover page", s.missing_cover_page)
        row("Unparsable cover page", s.unparsable_cover_page)

        log("  Holdings")
        row("Total", s.total_holdings)
        row("Dropped", s.dropped_holdings)
        row("Missing value", s.missing_value)
        row("Missing shares", s.missing_shares)

        log("  Managers")
        row("Total", s.total_managers)
        row("Unresolved numbers", s.unresolved_manager_numbers)

        log("  Normalization")
        row("Unmapped place codes", s.unmapped_place_codes)

        log("  Enrichment")
        row("NBIM rows", s.nbim_rows)
        row("Pension fund rows", s.pension_fund_rows)
        row("Aggregated securities", s.aggregated_securities)

        log("  Output")
        row("Rows written", s.output_rows)

        # --- Failure audit: iterate the enum, so new FailureTypes show up for free ---
        log("  Failures (by type)")
        permanent = transient = 0
        for ft in FailureType:
            count = s.failures[ft]
            retryable = self.failure_registry._classifier.is_retryable(ft)
            kind = "retry" if retryable else "permanent"
            log("    %-24s %s  (%s)", ft.value + ":", f"{count:,}", kind)
            transient += count if retryable else 0
            permanent += 0 if retryable else count

        row("Permanent total", permanent)
        row("Transient total", transient)

        log(bar)

    def run(self) -> None:
        """Run the pipeline, flushing any buffered failures on completion."""
        try:
            super().run()
        finally:
            self.failure_registry.flush()
