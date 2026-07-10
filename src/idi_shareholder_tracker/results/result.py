"""Result dataclasses produced by the scraper and pipeline stages.

Defines the records that flow through the pipeline:
- ``StockResult`` -- one holding parsed from a 13F information table.
- ``ManagerResult`` -- one "other included manager" (name and number) from a
  cover page.
- ``ShareholderResult`` -- one normalized holding row in the common release
  schema, after manager resolution and standardization.
"""
