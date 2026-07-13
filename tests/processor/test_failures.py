"""Tests for idi_shareholder_tracker.failures — FailureType and the classifier."""

import pytest

from idi_shareholder_tracker.failures import FailureType, ShareholderFailureClassifier

_HTTP_OK = 200
_HTTP_RATE_LIMIT = 429
_HTTP_SERVER_ERROR = 500

_PERMANENT_TYPES = [
    FailureType.NO_INFORMATION_TABLE,
    FailureType.NO_DOCUMENT_CONTENT,
    FailureType.UNPARSABLE_TABLE,
    FailureType.MISMATCHED_LENGTHS,
    FailureType.NO_FORM_DATA,
    FailureType.NO_HOLDINGS,
]
_TRANSIENT_TYPES = [FailureType.API_ERROR, FailureType.RATE_LIMIT]


class TestFailureType:
    """Tests for the FailureType enum."""

    def test_is_str_enum(self):
        # StrEnum members compare/format as their string value.
        assert FailureType.NO_INFORMATION_TABLE == "no_information_table"
        assert f"{FailureType.RATE_LIMIT}" == "rate_limit"

    def test_values_are_unique(self):
        values = [ft.value for ft in FailureType]
        assert len(values) == len(set(values))

    def test_covers_permanent_and_transient(self):
        # Guards against a member being dropped from either bucket in the test data.
        assert set(_PERMANENT_TYPES) | set(_TRANSIENT_TYPES) == set(FailureType)


class TestShareholderFailureClassifier:
    """Tests for ShareholderFailureClassifier retry policy and classification."""

    @pytest.fixture
    def classifier(self):
        return ShareholderFailureClassifier()

    @pytest.mark.parametrize("failure_type", _PERMANENT_TYPES)
    def test_permanent_types_are_not_retryable(self, classifier, failure_type):
        assert classifier.is_retryable(failure_type) is False
        assert failure_type in classifier.do_not_retry

    @pytest.mark.parametrize("failure_type", _TRANSIENT_TYPES)
    def test_transient_types_are_retryable(self, classifier, failure_type):
        assert classifier.is_retryable(failure_type) is True
        assert failure_type not in classifier.do_not_retry

    def test_do_not_retry_is_frozen(self, classifier):
        assert isinstance(classifier.do_not_retry, frozenset)

    def test_classify_rate_limit(self, classifier):
        result = classifier.classify_from_response({"status_code": _HTTP_RATE_LIMIT})
        assert result == FailureType.RATE_LIMIT

    def test_classify_error_key_takes_precedence(self, classifier):
        # An explicit error is an API_ERROR even when a status code is present.
        result = classifier.classify_from_response(
            {"status_code": _HTTP_RATE_LIMIT, "error": "boom"}
        )
        assert result == FailureType.API_ERROR

    def test_classify_missing_status_code_is_api_error(self, classifier):
        assert classifier.classify_from_response({}) == FailureType.API_ERROR

    @pytest.mark.parametrize("status_code", [_HTTP_OK, _HTTP_SERVER_ERROR])
    def test_classify_other_status_codes_are_api_error(self, classifier, status_code):
        result = classifier.classify_from_response({"status_code": status_code})
        assert result == FailureType.API_ERROR

    def test_classify_accepts_and_ignores_kwargs(self, classifier):
        result = classifier.classify_from_response(
            {"status_code": _HTTP_RATE_LIMIT}, cik="123", extra="ignored"
        )
        assert result == FailureType.RATE_LIMIT
