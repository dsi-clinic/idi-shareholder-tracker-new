"""Parsers for scraped SEC 13F-HR filing documents.

Pure parsing over already-downloaded HTML bytes (no network I/O). Provides:
- ``FilingScraper`` -- selects the information-table and cover-page documents from
  a scraped filing's manifest (``filing.documents``) by document type.
- ``TableInfoStockScraper`` -- parses the information-table HTML into per-holding
  ``StockResult`` records.
- ``CoverPageManagerScraper`` -- parses the cover page into ``ManagerResult``
  records (other-manager name and sequence number).
"""
