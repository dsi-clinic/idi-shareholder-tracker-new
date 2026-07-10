"""Shared data types and configuration for the shareholder-tracker processor.

Defines the dataclasses passed between pipeline stages -- notably
``PipelineConfig`` (input/output paths, SEC bucket, date range, static-file
locations, rate limit, worker count, cache settings) and ``PipelineStats``
(thread-safe run counters) -- plus any enums and constants (e.g. the target form
type ``13F-HR``) used across the package.
"""
