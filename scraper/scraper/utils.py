import datetime
import re


def parse_car_url(car_url: str) -> tuple[str, str, str, int]:
    (*_, city, brand, model, car_id) = car_url.split("/")
    city = city.split(".")[0]
    car_id = car_id.split(".")[0]
    return city, brand, model, car_id


def parse_car_item_desription(item_desription: str) -> dict:
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
        f"(?P<{param}>{param_re})" for param, param_re in param_specification.items()
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


def get_date_from_car_item_date(car_item_date: str) -> str:
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


def get_year_intervals(min_year: int, max_year: int) -> list[tuple[int, int]]:
    year_intervals = []
    range_points = [2000, 2005, 2010]
    next_year = min_year
    for range_point in range_points:
        if min_year < range_point:
            year_intervals.append((next_year, range_point))
            next_year = range_point + 1
    year_intervals.extend([(year, year) for year in range(next_year, max_year + 1)])
    return year_intervals


def get_price_estimation(price_estimation: str | None) -> str | None:
    if price_estimation is not None:
        price_estimation = price_estimation.split()[0]
    return price_estimation
