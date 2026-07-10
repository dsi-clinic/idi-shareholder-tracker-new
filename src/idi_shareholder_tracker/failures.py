"""Failure classification for the shareholder-tracker processor.

Defines the ``FailureType`` enum for this processor's failure modes (e.g. a
missing information-table document, un-parsable HTML, a table that yields no
holdings) and a ``FailureClassifier`` subclass that marks which types are
permanent (never retried) versus transient. Consumed by the shared
``FailureRegistry`` so un-retryable filings are skipped on subsequent runs.
"""

# Standard library imports
from enum import StrEnum

# Third-party imports
from idi_ftm2j_shared.failures import FailureClassifier

_HTTP_RATE_LIMIT = 429


class FailureType(StrEnum):
    """Failure modes for the shareholder-tracker pipeline."""

    # --- Permanent: properties of the scraped S3 artifact (retry won't help) ---
    NO_INFORMATION_TABLE = "no_information_table"  # filing manifest has no info-table document
    NO_DOCUMENT_CONTENT = "no_document_content"    # info-table S3 object missing/empty
    UNPARSABLE_TABLE = "unparsable_table"        # info-table HTML/XML failed to parse
    MISMATCHED_LENGTHS = "mismatched_lengths"      # parsed parallel columns unequal length
    NO_FORM_DATA = "no_form_data"                  # table present but no data rows
    NO_HOLDINGS = "no_holdings"                    # table parsed to zero holdings

    # --- Transient: live SEC submissions API for company metadata ---
    API_ERROR = "api_error"                        # HTTP failure fetching company metadata
    RATE_LIMIT = "rate_limit"                      # SEC rate limit (429)


class ShareholderFailureClassifier(FailureClassifier):
    """Classifies failures for the shareholder-tracker pipeline."""

    _DO_NOT_RETRY = frozenset(
        {
            FailureType.NO_INFORMATION_TABLE,
            FailureType.NO_DOCUMENT_CONTENT,
            FailureType.UNPARSABLE_TABLE,
            FailureType.MISMATCHED_LENGTHS,
            FailureType.NO_FORM_DATA,
            FailureType.NO_HOLDINGS,
        }
    )

    @property
    def do_not_retry(self) -> frozenset:
        """Return the set of failure types that should not be retried."""
        return self._DO_NOT_RETRY

    def classify_from_response(self, response: dict, **kwargs) -> FailureType:
        """Classify a failure from an SEC submissions HTTP response.

        Args:
            response: Response dict with status_code and optional error.
            **kwargs: Additional keyword arguments (unused).

        Returns:
            The classified FailureType.
        """
        status_code = response.get("status_code")
        has_error = "error" in response

        if has_error or status_code is None:
            return FailureType.API_ERROR
        if status_code == _HTTP_RATE_LIMIT:
            return FailureType.RATE_LIMIT
        return FailureType.API_ERROR