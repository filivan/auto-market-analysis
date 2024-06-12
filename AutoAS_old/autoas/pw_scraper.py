import asyncio
import datetime
import logging
import os
import pickle
import re

import pandas as pd
from logging import Logger, Handler
from logging.handlers import RotatingFileHandler
from playwright.async_api import async_playwright, BrowserContext

# block pages by resource type. e.g. image, stylesheet
BLOCK_RESOURCE_TYPES = [
    "beacon",
    "csp_report",
    "font",
    "image",
    "imageset",
    "media",
    "object",
    "texttrack",
    #  we can even block stylsheets and scripts though it's not recommended:
    # 'stylesheet',
    # 'script',
    # 'xhr',
]


# we can also block popular 3rd party resources like tracking:
BLOCK_RESOURCE_NAMES = [
    "adzerk",
    "analytics",
    "cdn.api.twitter",
    "doubleclick",
    "exelator",
    "facebook",
    "fontawesome",
    "google",
    "google-analytics",
    "googletagmanager",
]


DROM_PAGES_TEMPLATES = {
    "main_by_models": "https://auto.drom.ru/used/page<PN>/?grouping=1",
    "model_by_year": "https://auto.drom.ru/<BRAND>/<MODEL>/year-<YEAR>/used/page<PN>/",
    "model": "https://auto.drom.ru/<BRAND>/<MODEL>/used/page<PN>/",
}

DB_COLUMNS = {
    "slim": [
        "id",
        "brand",
        "model",
        "year",
        "capacity",
        "power",
        "fuel",
        "transmission",
        "drive",
        "mileage",
        "price",
        "price_estimation",
        "city",
        "city_ru",
        "date",
        "photo_url",
        "url",
    ],
    "full": [
        "id",
        "brand",
        "model",
        "year",
        "capacity",
        "power",
        "fuel",
        "transmission",
        "drive",
        "mileage",
        "body_type",
        "color",
        "wheel_side",
        "generation",
        "configuration",
        "price",
        "price_estimation",
        "city",
        "city_ru",
        "date",
        "all_photos_urls",
        "url",
    ],
}


class DromScraper:
    """
    Drom.ru scrapping pipeline
    """

    def __init__(
        self,
        drom_pages: dict = DROM_PAGES_TEMPLATES,
        version: str = "slim",
        to_csv: bool = True,
        out_path: str = "out/",
        filename: str = "drom_data.csv",
        save_checkpoints: bool = True,
    ):
        self.drom_pages = drom_pages
        self.version = version
        self.to_csv = to_csv
        self.out_path = out_path
        self.filename = filename
        self.chunks_size = 50
        self.logger = self._get_logger()
        self.save_checkpoints = save_checkpoints
        if self.save_checkpoints:
            os.makedirs("model_checkpoints", exist_ok=True)
            os.makedirs("global_checkpoint", exist_ok=True)
        if self.to_csv:
            os.makedirs(self.out_path, exist_ok=True)
            self.csv_file = os.path.join(self.out_path, self.filename)

    def _get_logger(
        self, show_logs: bool = True, log_handler: Handler | None = None
    ) -> Logger:
        """
        Handles the creation and retrieval of loggers to avoid
        re-instantiation.
        """
        # initialize and setup logging system for the parser object
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.DEBUG)
        # log name and format
        logfolder = "log"
        os.makedirs(logfolder, exist_ok=True)
        general_log = os.path.join(logfolder, f"{self.__class__.__name__}.log")
        file_handler = logging.FileHandler(general_log)
        # log rotation, 5 logs with 10MB size each one
        file_handler = RotatingFileHandler(
            general_log, maxBytes=10 * 1024 * 1024, backupCount=5
        )
        file_handler.setLevel(logging.DEBUG)
        logger_formatter = logging.Formatter(
            "%(asctime)s %(name)s %(levelname)s %(message)s",
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

    def _intercept_route(self, route):
        """intercept all requests and abort blocked ones"""
        if route.request.resource_type in BLOCK_RESOURCE_TYPES:
            return route.abort()
        if any(key in route.request.url for key in BLOCK_RESOURCE_NAMES):
            return route.abort()
        return route.continue_()

    async def _get_pages_number(self, context: BrowserContext, url: str) -> int:
        main_page = url.replace("<PN>", str(1))
        self.logger.debug(f"Get number of pages from {main_page}")
        page = await context.new_page()
        await page.route("**/*", self._intercept_route)
        await page.goto(main_page)
        items_number_element = (
            "a.css-192eo94.e1px31z30"
            if "grouping=1" in url
            else "div.css-1ksi09z.eckkbc90"
        )
        type_warning = await page.query_selector(
            "div[data-ftid='component_notification_type_warning']"
        )
        if type_warning:
            self.logger.debug(f"DONE: get 0 pages from {url}")
            await page.close()
            return 0
        items_number = await page.query_selector(items_number_element)
        if items_number is None:
            self.logger.debug(f"FAIL: items number not found at {url}")
            await page.close()
            return 0
        items_number = await items_number.inner_text()
        items_number = int("".join(filter(str.isdigit, items_number)))
        pages_number = items_number // 20 + (items_number % 20 > 0)
        await page.close()
        self.logger.debug(f"DONE: get {pages_number} pages from {main_page}")
        return pages_number

    def _parse_car_url(self, car_url: str) -> tuple[str, str, str, int]:
        (*_, city, brand, model, car_id) = car_url.split("/")
        city = city.split(".")[0]
        car_id = car_id.split(".")[0]
        return city, brand, model, car_id

    def _parse_car_item_desription(self, item_desription: str) -> dict:
        param_specification = {
            "capacity": "(\d+\.\d+)(?=\s+л)",
            "power": "(\d+)(?=\s+л.с.)",
            "fuel": "(бензин|дизель|гибрид|электро)",
            "transmission": "(автомат|АКПП|робот|вариатор|механика)",
            "drive": "(передний|задний|4WD)",
            "mileage": "([0-9\s]+)(?=\s+км)",
        }
        param_dict = {key: None for key in param_specification}
        params_re_group = r"|".join(
            f"(?P<{param}>{param_re})"
            for param, param_re in param_specification.items()
        )
        for mo in re.finditer(params_re_group, item_desription):
            param = mo.lastgroup
            value = mo.group()
            if param == "capacity":
                value = float(value)
            elif param == "power":
                value = int(value)
            elif param == "mileage":
                value = int("".join(list(filter(str.isdigit, value))))
            param_dict[param] = value
        return param_dict

    def _get_date_from_car_item_date(self, car_item_date: str) -> str:
        month_number = {
            "января": 1,
            "февраля": 2,
            "марта": 3,
            "апреля": 4,
            "мая": 5,
            "июня": 6,
            "июля": 7,
            "августа": 8,
            "сентября": 9,
            "октября": 10,
            "ноября": 11,
            "декабря": 12,
        }
        today_key_words = {"сегодня", "назад"}
        today = datetime.datetime.now().date()
        splited_car_item_date = car_item_date.split()
        if today_key_words.intersection(splited_car_item_date):
            date = today.isoformat()
        else:
            day = int(splited_car_item_date[0])
            month = month_number[splited_car_item_date[1]]
            year = today.year if today.month >= month else today.year - 1
            date = datetime.date(year, month, day).isoformat()
        return date

    async def parse_group_by_models_page(
        self, context: BrowserContext, url: str
    ) -> list[dict]:
        continue_on_failure = True
        parsed_models_items = []
        self.logger.debug(f"Parse group by models {url.split('/')[4]}")
        try:
            page = await context.new_page()
            await page.route("**/*", self._intercept_route)
            await page.goto(url, timeout=120000)
            await page.wait_for_selector("a.css-nox51k.esy1m7g5", timeout=120000)
            model_items = await page.query_selector_all("a.css-nox51k.esy1m7g5")
            for model_item in model_items:
                model_url = await model_item.get_attribute("href")
                years = await model_item.query_selector("span.css-1l9tp44.e162wx9x0")
                years = await years.inner_text()
                ads_number = await model_item.query_selector(
                    "span.css-1hrfta1.e162wx9x0"
                )
                ads_number = await ads_number.inner_text()
                years = years.split("—")
                start_year, last_year = years if len(years) == 2 else years * 2
                ads_number = int("".join(filter(str.isdigit, ads_number)))
                brand, model = model_url.split("/")[3:5]
                parsed_models_items.append(
                    {
                        "url": model_url,
                        "brand": brand,
                        "model": model,
                        "start_year": int(start_year),
                        "last_year": int(last_year),
                        "ads_number": ads_number,
                    }
                )
            await page.close()
            self.logger.debug(f"Group by models {url.split('/')[4]} DONE")
        except Exception as e:
            if continue_on_failure:
                self.logger.warning(
                    f"Error fetching or processing {url}, exception: {e}"
                )
            else:
                raise e
        return parsed_models_items

    async def get_model_items(self, context: BrowserContext) -> list[list | None]:
        models_items = []
        pages_number = 1  # await self._get_pages_number(context, self.drom_pages["main_by_models"])
        urls = [
            self.drom_pages["main_by_models"].replace("<PN>", str(page))
            for page in range(1, pages_number + 1)
        ]
        tasks = [self.parse_group_by_models_page(context, url) for url in urls]
        chunks = [
            tasks[i : i + self.chunks_size]
            for i in range(0, len(tasks), self.chunks_size)
        ]
        for chunk in chunks:
            chunk_models_items = await asyncio.gather(*chunk)
            models_items.extend(
                [
                    model_dict
                    for model_dicts in chunk_models_items
                    for model_dict in model_dicts
                ]
            )
        return models_items

    async def parse_car_page(self, context: BrowserContext, url: str) -> dict:
        ...

    async def parse_model_page_slim(
        self, context: BrowserContext, url: str
    ) -> list[dict]:
        continue_on_failure = True
        parsed_cars_items = []
        self.logger.debug(f"Parse car items at {url}")
        try:
            page = await context.new_page()
            await page.route("**/*", self._intercept_route)
            await page.goto(url, timeout=120000)
            await page.wait_for_selector(
                "a[data-ftid='bulls-list_bull']", timeout=120000
            )
            car_items = await page.query_selector_all("a[data-ftid='bulls-list_bull']")
            for car_item in car_items:
                car_url = await car_item.get_attribute("href")
                title = await car_item.query_selector("span[data-ftid='bull_title']")
                title = await title.inner_text()
                description = await car_item.query_selector(
                    "div[data-ftid='component_inline-bull-description']"
                )
                description = await description.inner_text()
                broken = await car_item.query_selector(
                    "div[data-ftid='bull_label_broken']"
                )
                broken = broken is not None
                nodocs = await car_item.query_selector(
                    "div[data-ftid='bull_label_nodocs']"
                )
                nodocs = nodocs is not None
                price = await car_item.query_selector("span[data-ftid='bull_price']")
                price = await price.inner_text()
                price_estimation = await car_item.query_selector(
                    "div.css-b9bhjf.ejipaoe0"
                )
                price_estimation = (
                    await price_estimation.inner_text()
                    if price_estimation is not None
                    else None
                )
                city_ru = await car_item.query_selector(
                    "span[data-ftid='bull_location']"
                )
                city_ru = await city_ru.inner_text()
                date_dirty = await car_item.query_selector("div[data-ftid='bull_date']")
                date_dirty = await date_dirty.inner_text()
                photo_url = await car_item.query_selector("img")
                photo_url = (
                    await photo_url.get_attribute("src")
                    if photo_url is not None
                    else None
                )
                city, brand, model, car_id = self._parse_car_url(car_url)
                param_dict = self._parse_car_item_desription(description)
                price = int("".join(list(filter(str.isdigit, price))))
                year = int(title.split()[-1])
                date = self._get_date_from_car_item_date(date_dirty)
                parsed_cars_items.append(
                    {
                        "id": car_id,
                        "brand": brand,
                        "model": model,
                        "year": year,
                        "capacity": param_dict["capacity"],
                        "power": param_dict["power"],
                        "fuel": param_dict["fuel"],
                        "transmission": param_dict["transmission"],
                        "drive": param_dict["drive"],
                        "mileage": param_dict["mileage"],
                        "broken": broken,
                        "nodocs": nodocs,
                        "price": price,
                        "price_estimation": price_estimation,
                        "city": city,
                        "city_ru": city_ru,
                        "date": date,
                        "photo_url": photo_url,
                        "url": car_url,
                    }
                )
            await page.close()
            self.logger.debug(f"DONE: Parse car items at {url}")
        except Exception as e:
            await page.close()
            if continue_on_failure:
                self.logger.warning(
                    f"Error fetching or processing {url}, exception: {e}"
                )
            else:
                raise e
        return parsed_cars_items

    async def parse_model_page(self, context: BrowserContext, url: str) -> list[dict]:
        model_car_items_slim = []
        pages_number = await self._get_pages_number(context, url)
        urls = [url.replace("<PN>", str(page)) for page in range(1, pages_number + 1)]
        tasks = [self.parse_model_page_slim(context, url) for url in urls]
        chunks = [
            tasks[i : i + self.chunks_size]
            for i in range(0, len(tasks), self.chunks_size)
        ]
        for chunk in chunks:
            chunk_model_car_items_slim = await asyncio.gather(*chunk)
            model_car_items_slim.extend(
                [
                    model_cars_dict
                    for model_cars_dicts in chunk_model_car_items_slim
                    for model_cars_dict in model_cars_dicts
                ]
            )
        return model_car_items_slim

    async def get_model_car_items(
        self, context: BrowserContext, model_item: dict
    ) -> list[dict]:
        model_car_items = []
        pages_number = model_item["ads_number"] // 20 + (
            model_item["ads_number"] % 20 > 0
        )
        if pages_number > 100:
            url = (
                self.drom_pages["model_by_year"]
                .replace("<BRAND>", model_item["brand"])
                .replace("<MODEL>", model_item["model"])
            )
            for year in range(model_item["start_year"], model_item["last_year"] + 1):
                car_items_by_year = await self.parse_model_page(
                    context, url.replace("<YEAR>", str(year))
                )
                model_car_items.extend(car_items_by_year)
        else:
            url = (
                self.drom_pages["model"]
                .replace("<BRAND>", model_item["brand"])
                .replace("<MODEL>", model_item["model"])
            )
            model_car_items = await self.parse_model_page(context, url)
        return model_car_items

    async def get_car_items(
        self, context: BrowserContext, models_items: list[dict]
    ) -> list[dict]:
        car_items = []
        for model_item in models_items:
            model_car_items = await self.get_model_car_items(context, model_item)
            car_items.extend(model_car_items)
            self.logger.debug(f"Parsed {len(car_items)} advertisements of {model_item['brand']} {model_item['model']}")
            if self.save_checkpoints:
                with open(
                    f"model_checkpoints/{model_item['brand']}_{model_item['model']}.pkl",
                    "wb",
                ) as mchp:
                    pickle.dump(model_car_items, mchp, pickle.HIGHEST_PROTOCOL)
                with open("global_checkpoint/car_items.pkl", "wb") as gchp:
                    pickle.dump(car_items, gchp, pickle.HIGHEST_PROTOCOL)
        return car_items

    async def scrape(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            models_items = await self.get_model_items(context)
            car_items = await self.get_car_items(context, models_items)
            if self.to_csv:
                pd.DataFrame(car_items).to_csv(self.csv_file, index=False)
            await browser.close()
            return car_items


if __name__ == "__main__":
    drom_scraper = DromScraper(drom_pages=DROM_PAGES_TEMPLATES)
    asyncio.run(drom_scraper.scrape())
