"""Pipeline that builds the shareholder-tracker release from SEC 13F-HR filings.

Subclasses the shared ``Pipeline`` ABC (``load_input`` -> ``process`` ->
``save_output`` -> ``display_stats``). Reads already-scraped 13F-HR filings from
the shared S3 bucket via ``iter_filings_by_form_type``, parses each filing's
information table and cover page, resolves each holding's "other manager" numbers
to names, enriches by merging the static NBIM and pension-fund sources, and writes
the normalized release to Parquet. Uses the shared ``FailureRegistry`` for
resumable, failure-aware runs.
"""
