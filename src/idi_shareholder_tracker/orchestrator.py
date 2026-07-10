"""Command-line entry point for the shareholder-tracker processor.

Parses CLI arguments (date range / ``--daily``, SEC bucket prefix, output and
failure file paths, static-input paths, rate limit, worker count), reads secrets
from the environment (``SEC_USER_AGENT``), builds the ``SecClient`` and
``PipelineConfig``, and runs the pipeline. Exposed as the ``shareholder-tracker``
console script via ``[project.scripts]``.
"""
