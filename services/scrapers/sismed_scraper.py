import io
import logging
import os
import time
from typing import Optional, Tuple

import pandas as pd
from playwright.sync_api import sync_playwright

logger = logging.getLogger(__name__)

DEFAULT_POWERBI_URL = (
    "https://app.powerbi.com/view?r="
    "eyJrIjoiMjBjNmVkMjUtMzVmOC00MDRiLTg4NDEtZDBlNGQwODEwMDhlIiwidCI6Ijc5MDVjMWZjLTkzM2MtNDUyYS04YjgzLWIyZTU2NDU1ZDE2YSIsImMiOjR9"
)


class SismedScraper:
    """Automated scraper for MINSA SISMED daily stock reporting via Power BI service."""

    def __init__(self, powerbi_url: str = DEFAULT_POWERBI_URL, headless: bool = True):
        self.powerbi_url = powerbi_url
        self.headless = headless

    def scrape_region(
        self,
        region: str = "TUMBES",
        max_batches: int = 5,
        save_path: Optional[str] = None,
    ) -> pd.DataFrame:
        """Execute headless browser automation to extract regional inventory data.

        Args:
            region: Region/DIRESA name to filter (e.g., 'TUMBES', 'PIURA').
            max_batches: Maximum pagination batches for the Power BI custom visual.
            save_path: Optional path to save the extracted raw HTML/Excel workbook.

        Returns:
            pd.DataFrame: Cleaned DataFrame of extracted medicine inventory records.
        """
        logger.info(f"Starting SISMED extraction for region: {region} (headless={self.headless})")
        start_time = time.time()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = browser.new_context(viewport={"width": 1920, "height": 1080})
            page = context.new_page()

            logger.info("Step 1: Navigating to Power BI dashboard...")
            page.goto(self.powerbi_url, wait_until="load", timeout=90000)
            page.wait_for_timeout(20000)

            logger.info("Step 2: Opening filters pane...")
            page.mouse.click(100, 220)
            page.wait_for_timeout(4000)

            logger.info("Step 3: Opening DIRESA slicer dropdown...")
            for c in page.query_selector_all(".visual-container-component"):
                try:
                    txt = c.inner_text()
                    if "DIRESA" in txt or "Macroregión" in txt:
                        restatement = c.query_selector(".slicer-restatement, .slicer-dropdown")
                        if restatement:
                            restatement.click()
                        else:
                            c.click()
                        break
                except Exception:
                    pass
            page.wait_for_timeout(3000)

            logger.info(f"Step 4: Filtering slicer by '{region}'...")
            page.mouse.click(200, 238)
            page.wait_for_timeout(500)
            page.keyboard.type(region.upper())
            page.wait_for_timeout(2500)

            logger.info("Step 5: Selecting region checkbox...")
            page.mouse.click(385, 315)
            page.wait_for_timeout(3000)

            # Close dropdown
            page.mouse.click(100, 140)
            page.wait_for_timeout(1000)

            logger.info("Step 6: Closing filters pane...")
            page.mouse.click(1781, 16)
            page.wait_for_timeout(4000)

            logger.info("Step 7: Opening Export interface...")
            page.mouse.click(880, 990)
            page.wait_for_timeout(6000)

            target_frame = None
            for f in page.frames:
                try:
                    content = f.content()
                    if "añadir" in content.lower() or "circlecard" in f.url.lower():
                        target_frame = f
                        break
                except Exception:
                    pass

            if not target_frame:
                browser.close()
                raise RuntimeError("Failed to locate custom visual iframe for export.")

            logger.info("Step 8: Waiting for data export generation...")
            for _ in range(30):
                time.sleep(3)
                info = target_frame.evaluate("""() => {
                    const a = document.querySelector('a');
                    const p = document.querySelector('p');
                    return {
                        download: a ? a.getAttribute('download') : '',
                        rows: p ? p.innerText : ''
                    };
                }""")
                if info.get("download", "").startswith("File_D") and info.get("rows") != "Cargando...":
                    break

            logger.info("Step 9: Accumulating incremental pagination batches...")
            for batch in range(1, max_batches + 1):
                rows_text = target_frame.evaluate(
                    "() => document.querySelector('p') ? document.querySelector('p').innerText : ''"
                )
                logger.info(f"  Batch {batch}: Current accumulated rows: {rows_text}")

                target_frame.evaluate(
                    "() => { const b = document.querySelector('button'); if(b) b.click(); }"
                )
                for _ in range(12):
                    time.sleep(3)
                    new_text = target_frame.evaluate(
                        "() => document.querySelector('p') ? document.querySelector('p').innerText : ''"
                    )
                    if new_text != rows_text and new_text != "Cargando...":
                        break

            logger.info("Step 10: Extracting in-memory Blob via JS fetch...")
            html_content = target_frame.evaluate("""async () => {
                const a = document.querySelector('a');
                const resp = await fetch(a.href);
                return await resp.text();
            }""")

            browser.close()

        if save_path:
            with open(save_path, "w", encoding="utf-8") as f:
                f.write(html_content)
            logger.info(f"Raw dataset saved to {save_path}")

        logger.info("Step 11: Parsing extracted HTML table with pandas...")
        dfs = pd.read_html(io.StringIO(html_content), header=3)
        if not dfs:
            raise ValueError("No table found in extracted Power BI HTML.")

        df = dfs[0]
        # Clean numeric columns
        if "STOCK" in df.columns:
            df["STOCK_NUM"] = pd.to_numeric(
                df["STOCK"].astype(str).str.replace(",", "").str.replace(" ", ""),
                errors="coerce",
            ).fillna(0)
        if "PRECIO" in df.columns:
            df["PRECIO_NUM"] = pd.to_numeric(
                df["PRECIO"].astype(str).str.replace(",", "").str.replace(" ", ""),
                errors="coerce",
            ).fillna(0)

        elapsed = time.time() - start_time
        logger.info(
            f"Extraction completed in {elapsed:.1f}s. Total records: {len(df):,}"
        )
        return df

    @staticmethod
    def extract_latest_snapshot(df: pd.DataFrame) -> Tuple[str, pd.DataFrame]:
        """Filter the DataFrame to contain only the most recent reporting date.

        Args:
            df: Regional DataFrame with 'FECHA REPORTE' column.

        Returns:
            Tuple[str, pd.DataFrame]: The latest date string and the filtered snapshot DataFrame.
        """
        if "FECHA REPORTE" not in df.columns:
            raise KeyError("Column 'FECHA REPORTE' not found in DataFrame.")

        date_counts = df["FECHA REPORTE"].value_counts()
        latest_date = str(date_counts.index[0])
        snapshot = df[df["FECHA REPORTE"] == latest_date].copy()
        logger.info(
            f"Filtered latest snapshot for {latest_date}: {len(snapshot):,} records "
            f"across {snapshot['IPRESS'].nunique() if 'IPRESS' in snapshot.columns else 0} facilities"
        )
        return latest_date, snapshot


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    scraper = SismedScraper()
    df = scraper.scrape_region(region="TUMBES", max_batches=4)
    latest_date, snapshot = SismedScraper.extract_latest_snapshot(df)
    print(f"Latest Date: {latest_date}, Rows: {len(snapshot)}")
