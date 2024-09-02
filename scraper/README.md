# Auto Advertisements Scraper

Скрапер на базе `scrapy` в свзяке с `playwright` для сбора данных с площадки [Drom.ru](https://www.drom.ru/).  
Из каждого объявления собираются следующие данные о продаваемом авто:  
Марка/Модель/Год выпуска/Объём двигателя(л)/Мощность(лc)/Топливо/Коробка передач/Привод/Пробег/Битая или нет/Есть ли проблемы с документами/Цена/Оценка справедливости цены/Город/Дата/Фото/URL  
(brand/model/year/capacity/power/fuel/transmission/drive/mileage/broken/nodocs/price/price_estimation/city/city_ru/date/photo_url/url)  
Данные сохраняются в базу данных Postgres.