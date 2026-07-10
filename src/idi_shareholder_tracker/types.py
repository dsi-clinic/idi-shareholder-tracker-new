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
from dataclasses import dataclass

_REMOTE_SCHEMES = ("s3://", "https://", "http://", "gs://")


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
class PipelineConfig:
    """Configuration for the subsidiary pipeline."""

    failure_file: str
    output_file: str
    start_date: datetime.date
    end_date: datetime.date
    sec_bucket: str
    rate_limit: float = 0.2
    num_workers: int = 10

    def __post_init__(self) -> None:
        """Validate existence of local files."""
        if _is_local(self.failure_file) and not pathlib.Path(self.failure_file).parent.exists():
            pathlib.Path(self.failure_file).parent.mkdir(parents=True, exist_ok=True)
        if _is_local(self.output_file) and not pathlib.Path(self.output_file).parent.exists():
            pathlib.Path(self.output_file).parent.mkdir(parents=True, exist_ok=True)
