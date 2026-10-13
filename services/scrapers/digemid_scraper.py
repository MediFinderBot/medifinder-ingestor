import io
import logging
import time
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

DEFAULT_DIGEMID_URL = "https://ms-opm.minsa.gob.pe/msopmcovid/producto/catalogoproductos"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Content-Type": "application/json",
    "Origin": "https://opm-digemid.minsa.gob.pe",
    "Referer": "https://opm-digemid.minsa.gob.pe/",
    "Accept": "application/json, text/plain, */*",
}


class DigemidScraper:
    """Automated scraper for MINSA DIGEMID product catalog via REST microservice."""

    def __init__(self, endpoint_url: str = DEFAULT_DIGEMID_URL, timeout: int = 120):
        self.endpoint_url = endpoint_url
        self.timeout = timeout

    def download_catalog_bytes(self) -> bytes:
        """Download raw Excel catalog bytes from the DIGEMID endpoint.

        Returns:
            bytes: The downloaded binary content of the Excel workbook.
        """
        logger.info(f"Initiating catalog download from {self.endpoint_url}")
        payload = {"filtro": {"situacion": "ACT"}}
        start_time = time.time()

        response = requests.post(
            self.endpoint_url,
            json=payload,
            headers=DEFAULT_HEADERS,
            timeout=self.timeout,
        )
        response.raise_for_status()

        elapsed = time.time() - start_time
        size_mb = len(response.content) / (1024 * 1024)
        logger.info(f"Downloaded {size_mb:.2f} MB in {elapsed:.2f}s")
        return response.content

    def download_to_file(self, target_path: str) -> str:
        """Download and save the catalog workbook to a local file.

        Args:
            target_path: Destination filesystem path.

        Returns:
            str: Destination path where the file was saved.
        """
        data = self.download_catalog_bytes()
        with open(target_path, "wb") as f:
            f.write(data)
        logger.info(f"Catalog saved to {target_path}")
        return target_path

    def get_dataframe(self) -> pd.DataFrame:
        """Download the catalog and parse it directly into a pandas DataFrame.

        Returns:
            pd.DataFrame: Cleaned DataFrame containing all active pharmaceutical products.
        """
        data = self.download_catalog_bytes()
        logger.info("Parsing Excel workbook into pandas DataFrame...")
        excel_io = io.BytesIO(data)
        df = pd.read_excel(excel_io, sheet_name=0)
        logger.info(f"Parsed {len(df):,} products with {len(df.columns)} columns")
        return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scraper = DigemidScraper()
    df = scraper.get_dataframe()
    print(df.head(5))
