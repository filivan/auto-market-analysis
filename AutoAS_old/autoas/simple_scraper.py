import logging
import os

import aiohttp
import asyncio
import pandas as pd

from bs4 import BeautifulSoup
from logging.handlers import RotatingFileHandler

main_by_models_page = "https://auto.drom.ru/used/page<PN>/?grouping=1"
start_page = 1

DROM_PAGES_TEMPLATES = {
    "main_by_models": "https://auto.drom.ru/used/page<PN>/?grouping=1",
    "model": "https://auto.drom.ru/<BRAND>/<MODEL>/year-<YEAR>/used/page<PN>/",
    "model_full": "https://auto.drom.ru/<BRAND>/<MODEL>/year-<YEAR>/used/page<PN>/",
}

DB_COLUMNS = {
    "slim": [
        "id",
        "brand",
        "model",
        "displacement",
        "fuel",
        "transmission",
        "drive",
        "mileage",
        "price",
        "price_estimation",
        "city",
        "date",
        "photo",
        "url",
    ],
    "full": [
        "id",
        "brand",
        "model",
        "displacement",
        "fuel",
        "transmission",
        "drive",
        "mileage",
        "body_type",
        "color",
        "wheel_side",
        "generation",
        "configuration",
        "description",
        "price",
        "price_estimation",
        "city",
        "date",
        "all_photos",
        "url",
    ],
}


class SimpleDromScraper:
    """
    Drom.ru scrapping pipeline
    """

    def __init__(
        self,
        drom_pages: dict,
        version: str = "slim",
        headers: dict | None = None,
        to_csv: bool = True,
        out_path: str = "out/",
        filename: str = "drom_data.csv",
    ):
        self.drom_pages = drom_pages
        self.version = version
        self.to_csv = to_csv
        self.out_path = out_path
        self.filename = filename
        self.headers = (
            {
                "user-agent": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
            }
            if headers is not None
            else headers
        )
        if self.to_csv:
            os.makedirs(self.out_path, exist_ok=True)
            self.csv_file = os.path.join(self.out_path, self.filename)
            if not os.path.isfile(self.csv_file):
                empty_df = pd.DataFrame(
                    columns=DB_COLUMNS[
                        self.version if self.version in DB_COLUMNS.keys() else "slim"
                    ]
                )
                empty_df.to_csv(self.csv_file, index=False)


    def get_logger(self, show_logs: bool, log_handler=None):
        """
        Handles the creation and retrieval of loggers to avoid
        re-instantiation.
        """
        # initialize and setup logging system for the parser object
        logger = logging.getLogger(self.username)
        logger.setLevel(logging.DEBUG)
        # log name and format
        general_log = f"{self.logfolder}general.log"
        file_handler = logging.FileHandler(general_log)
        # log rotation, 5 logs with 10MB size each one
        file_handler = RotatingFileHandler(
            general_log, maxBytes=10 * 1024 * 1024, backupCount=5
        )
        file_handler.setLevel(logging.DEBUG)
        # extra = {"username": self.username}
        logger_formatter = logging.Formatter(
            "%(levelname)s [%(asctime)s] [%(username)s]  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(logger_formatter)
        logger.addHandler(file_handler)

        # add custom user handler if given
        if log_handler:
            logger.addHandler(log_handler)

        if show_logs is True:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG)
            console_handler.setFormatter(logger_formatter)
            logger.addHandler(console_handler)

        return logger

    async def fetch(self, session: aiohttp.ClientSession, url: str) -> str:
        try:
            async with session.get(url) as response:
                text = await response.text()
                return text
        except Exception as e:
            print(str(e))

    async def _get_pages_number(self, session: aiohttp.ClientSession, url: str, models_page: bool = False) -> int:
        page = await self.fetch(session, url)
        page_soup = BeautifulSoup(page, "html.parser")
        items_number_element = "a.css-192eo94.e1px31z30" if models_page else "div.css-1ksi09z.eckkbc90"
        items_number = page_soup.select_one(items_number_element)
        models_number = int("".join(filter(str.isdigit, items_number.text)))
        pages_number = models_number // 20 + (models_number % 20 > 0)
        return pages_number
    
    async def parse_models_page(
        self, session: aiohttp.ClientSession, url: str
    ) -> list[dict] | None:
        parsed_models_items = []
        models_page = await self.fetch(session, url)
        models_page_soup = BeautifulSoup(models_page, "html.parser")
        model_items = models_page_soup.select("a.css-nox51k.esy1m7g5")
        if model_items:
            for model_item in model_items:
                url = model_item.attrs["href"]
                years = model_item.select_one("span.css-1l9tp44.e162wx9x0").text.split("—")
                start_year, last_year = years if len(years) == 2 else years * 2
                ads_number = model_item.select_one("span.css-1hrfta1.e162wx9x0")
                ads_number = int("".join(filter(str.isdigit, ads_number.text)))
                parsed_models_items.append(
                    {
                        "url": url,
                        "start_year": int(start_year),
                        "last_year": int(last_year),
                        "ads_number": ads_number,
                    }
                )
            return parsed_models_items
        return None

    async def main(self):
        async with aiohttp.ClientSession(headers=self.headers) as session:
            url = self.drom_pages["main_by_models"].replace("<PN>", str(1))
            pages_number = await self._get_pages_number(session, url, models_page=True)
            # pages_number = 10
            urls = [self.drom_pages["main_by_models"].replace("<PN>", str(page)) for page in range(1, pages_number+1)]
            tasks = [self.parse_models_page(session, url) for url in urls]
            models_items = await asyncio.gather(*tasks)
            print(models_items)
