from sqlalchemy import Boolean, Integer, String, Float, Date
from sqlalchemy.orm import mapped_column
from scraper.database import Base


class Car(Base):
    __tablename__ = "cars"
    id = mapped_column(String, primary_key=True)
    brand = mapped_column(String, nullable=False)
    model = mapped_column(String, nullable=False)
    year = mapped_column(Integer, nullable=False)
    capacity = mapped_column(Float, nullable=False)
    power = mapped_column(Float, nullable=True)
    fuel = mapped_column(String, nullable=True)
    transmission = mapped_column(String, nullable=True)
    drive = mapped_column(String, nullable=True)
    mileage = mapped_column(Integer, nullable=True)
    broken = mapped_column(Boolean, nullable=True)
    nodocs = mapped_column(Boolean, nullable=True)
    price = mapped_column(Float, nullable=False)
    price_estimation = mapped_column(String, nullable=True)
    city = mapped_column(String, nullable=False)
    city_ru = mapped_column(String, nullable=False)
    date = mapped_column(Date, nullable=False)
    photo_url = mapped_column(String, nullable=True)
    url = mapped_column(String, nullable=False)
