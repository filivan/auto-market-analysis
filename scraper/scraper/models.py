from sqlalchemy import Boolean, Integer, String, Float, Date
from sqlalchemy.orm import Mapped, mapped_column
from scraper.database import Base


class Car(Base):
    __tablename__ = "cars"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    brand: Mapped[str] = mapped_column(String, nullable=False)
    model: Mapped[str] = mapped_column(String, nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    capacity: Mapped[float] = mapped_column(Float, nullable=True)
    power: Mapped[float] = mapped_column(Float, nullable=True)
    fuel: Mapped[str] = mapped_column(String, nullable=True)
    transmission: Mapped[str] = mapped_column(String, nullable=True)
    drive: Mapped[str] = mapped_column(String, nullable=True)
    mileage: Mapped[int] = mapped_column(Integer, nullable=True)
    broken: Mapped[bool] = mapped_column(Boolean, nullable=True)
    nodocs: Mapped[bool] = mapped_column(Boolean, nullable=True)
    price: Mapped[float] = mapped_column(Float, nullable=True)
    price_estimation: Mapped[str] = mapped_column(String, nullable=True)
    city: Mapped[str] = mapped_column(String, nullable=False)
    city_ru: Mapped[str] = mapped_column(String, nullable=False)
    region: Mapped[str] = mapped_column(String, nullable=True)
    date: Mapped[str] = mapped_column(Date, nullable=False)
    photo_url: Mapped[str] = mapped_column(String, nullable=True)
    url: Mapped[str] = mapped_column(String, nullable=False)

class City(Base):
    __tablename__ = "cities"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    city: Mapped[str] = mapped_column(String, nullable=False)
    region: Mapped[str] = mapped_column(String, nullable=False)