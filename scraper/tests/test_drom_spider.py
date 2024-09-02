# tests/test_drom_spider.py
import pytest
from scrapy.crawler import CrawlerProcess
from scrapy.http.response import Response
from scraper.spiders.drom_spider import DromSpider


@pytest.fixture
def spider():
    return DromSpider()


def test_spider_name(spider):
    assert spider.name == "drom"


def test_start_urls(spider):
    assert spider.start_urls == ["https://auto.drom.ru/japanese/used/?grouping=1"]


def test_parse_model_url(spider):
    # Mock response
    response = Response(url="https://auto.drom.ru/toyota/camry/used/", body="")
    model = {"xpath": "//div[@data-ftid='bulls-list_model-range']"}
    model_url = spider.parse(response, model)
    assert model_url == "https://auto.drom.ru/toyota/camry/used/"


def test_parse_model_years(spider):
    # Mock response
    response = Response(url="https://auto.drom.ru/toyota/camry/used/", body="")
    model = {"xpath": "//div[@data-ftid='bulls-list_model-range']"}
    model_years = spider.parse(response, model)
    assert model_years == (int, int)  # min_year, max_year


def test_parse_model_ads_number(spider):
    # Mock response
    response = Response(url="https://auto.drom.ru/toyota/camry/used/", body="")
    model = {"xpath": "//div[@data-ftid='bulls-list_model-range']"}
    ads_number = spider.parse(response, model)
    assert ads_number == int  # ads_number


def test_parse_model_next_page(spider):
    # Mock response
    response = Response(url="https://auto.drom.ru/toyota/camry/used/", body="")
    model = {"xpath": "//div[@data-ftid='bulls-list_model-range']"}
    next_page = spider.parse(response, model)
    assert next_page == str  # next_page_url


def test_crawl(spider):
    # Run the spider
    process = CrawlerProcess(settings={"USER_AGENT": "pytest"})
    process.crawl(spider)
    process.start()

    # Check if the spider crawled successfully
    assert spider.stats.get_value("item_count") > 0
