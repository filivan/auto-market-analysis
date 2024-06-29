import scrapy
from scrapy.http.response import Response
from scrapy_playwright.page import PageMethod
from scraper.models import Car
from scraper.utils import (
    get_year_intervals,
    get_date_from_car_item_date,
    parse_car_item_desription,
    parse_car_url,
    get_price_estimation,
)


class DromSpider(scrapy.Spider):
    name = "drom"
    start_urls = [
        "https://auto.drom.ru/toyota/camry/used/"  # 'https://auto.drom.ru/japanese/used/?grouping=1'
    ]

    def parse(self, response: Response):
        models = response.xpath(
            "//div[@data-ftid='bulls-list_model-range']"
        )  # .extract()
        for model in models:
            model_url = model.xpath(
                ".//a[@href]/@href"
            ).get()  # 'https://auto.drom.ru/toyota/camry/used/'
            # brand_model = model.xpath(
            #     ".//div[@data-ftid='bulls-list_model-range_title']/text()"
            # ).get()  # 'Toyota Camry'
            # brand, model_name = brand_model.split()
            model_years = model.xpath(
                ".//span[@data-ftid='bulls-list_model-range_year']/text()"
            ).getall()  # ['1982', '—', '2024']
            min_year, max_year = (
                (int(model_years[0]), int(model_years[-1]))
                if len(model_years) == 3
                else (int(model_years[0]), int(model_years[0]))
            )
            ads_number = model.xpath(
                ".//span[@data-ftid='bulls-list_model-range_bulls-count']/text()"
            ).get()
            ads_number = int("".join(filter(str.isdigit, ads_number)))
            # ads_number = int("".join(ads_number.split()[:-1]))
            if ads_number > 2000:
                year_intervals = get_year_intervals(min_year, max_year)
                for min_y, max_y in year_intervals:
                    url = model_url + f"?minyear={min_y}&maxyear={max_y}"
                    yield scrapy.Request(
                        url=url,
                        callback=self.parse_model,
                        meta=dict(
                            playwright=True,
                            playwright_page_coroutines=[
                                PageMethod(
                                    "wait_for_selector",
                                    "a[data-ftid='bulls-list_bull']",
                                ),
                            ],
                        ),
                        # cb_kwargs={
                        #     "brand": brand,
                        #     "model_name": model_name,
                        # },
                    )
            yield scrapy.Request(
                url=model_url,
                callback=self.parse_model,
                meta=dict(
                    playwright=True,
                    playwright_page_coroutines=[
                        PageMethod(
                            "wait_for_selector", "a[data-ftid='bulls-list_bull']"
                        ),
                    ],
                ),
                # cb_kwargs={
                #     "brand": brand,
                #     "model": model_name,
                # },
            )

        next_page = response.xpath(
            "//a[@data-ftid='component_pagination-item-next']/@href"
        ).get()
        # if next_page:
        #     yield response.follow(next_page, self.parse)

    def parse_model(self, response: Response):  # brand, model):
        cars = response.xpath("//a[@data-ftid='bulls-list_bull']")
        for car in cars:
            car_url: str = car.xpath(".//@href").get()
            title: str = car.xpath(".//div[@data-ftid='bull_title']/text()").get()
            description: str = "".join(
                car.xpath(
                    ".//div[@data-ftid='component_inline-bull-description']//span/text()"
                ).getall()
            )
            broken: bool = (
                car.xpath(".//div[@data-ftid='bull_label_broken']").get() is not None
            )
            nodocs: bool = (
                car.xpath(".//div[@data-ftid='bull_label_nodocs']").get() is not None
            )
            price: int = int(
                "".join(
                    filter(
                        str.isdigit,
                        car.xpath(".//span[@data-ftid='bull_price']/text()").get(),
                    )
                )
            )
            price_estimation: str | None = get_price_estimation(
                car.xpath(".//*[text()[contains(.,'цена')]]/text()").get()
            )
            city_ru: str = car.xpath(".//span[@data-ftid='bull_location']/text()").get()
            date_raw: str = car.xpath(".//div[@data-ftid='bull_date']/text()").get()
            photo_url: str | None = car.xpath(".//img/@src").get()

            city, brand, model, car_id = parse_car_url(car_url)
            desription_params = parse_car_item_desription(description)
            year = int(title.split()[-1])
            date = get_date_from_car_item_date(date_raw)
            yield {
                "id": car_id,
                "brand": brand,
                "model": model,
                "year": year,
                "capacity": desription_params["capacity"],
                "power": desription_params["power"],
                "fuel": desription_params["fuel"],
                "transmission": desription_params["transmission"],
                "drive": desription_params["drive"],
                "mileage": desription_params["mileage"],
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
        next_page = response.xpath(
            "//a[@data-ftid='component_pagination-item-next']/@href"
        ).get()
        # if next_page:
        #     yield response.follow(next_page, self.parse)

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                callback=self.parse_model,
                meta=dict(
                    playwright=True,
                    playwright_page_coroutines=[
                        PageMethod(
                            "wait_for_selector",
                            "div[data-ftid='bulls-list_model-range']",
                        ),
                    ],
                ),
            )
