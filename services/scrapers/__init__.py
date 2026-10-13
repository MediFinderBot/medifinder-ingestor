"""Scrapers package for MediFinderBot ingestion.

Contains automated extraction modules for official Peruvian healthcare data sources:
- DIGEMID: Pharmaceutical product catalog via direct microservice query.
- SISMED: Public health facility medicine stock via MINSA Power BI extraction.
"""

from .digemid_scraper import DigemidScraper
from .sismed_scraper import SismedScraper

__all__ = ["DigemidScraper", "SismedScraper"]
