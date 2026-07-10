"""Failure classification for the shareholder-tracker processor.

Defines the ``FailureType`` enum for this processor's failure modes (e.g. missing
information-table or cover-page document, unparseable HTML) and a
``FailureClassifier`` subclass that marks which types are permanent (never
retried) versus transient. Consumed by the shared ``FailureRegistry`` so
unretryable filings are skipped on subsequent runs.
"""
