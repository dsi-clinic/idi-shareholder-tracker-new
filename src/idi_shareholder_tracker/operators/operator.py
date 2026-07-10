"""Post-processing operators that transform parsed results into the release.

Provides:
- ``clean_normalize_results`` -- resolves each holding's other-manager numbers to
  names, standardizes text, maps EDGAR place codes to country/region, splits
  SH/PRN amounts, and produces normalized ``ShareholderResult`` rows.
- ``enrich_results`` -- merges the cached, normalized static sources (NBIM and
  pension funds) with the SEC holdings and aggregates by security to form the
  final dataset.
"""
