from sqlalchemy import Boolean, Integer, String, Float, Date
from sqlalchemy.orm import Mapped, mapped_column
from database import Base


class Car(Base):
    __tablename__ = "cars"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    brand: Mapped[str] = mapped_column(String, nullable=False)
    model: Mapped[str] = mapped_column(String, nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    capacity: Mapped[float]
    power: Mapped[float]
    fuel: Mapped[str]
    transmission: Mapped[str]
    drive: Mapped[str]
    mileage: Mapped[int]
    broken: Mapped[bool]
    nodocs: Mapped[bool]
    price: Mapped[float]
    price_estimation: Mapped[str]
    city: Mapped[str] = mapped_column(String, nullable=False)
    city_ru: Mapped[str] = mapped_column(String, nullable=False)
    date: Mapped[str] = mapped_column(Date, nullable=False)
    photo_url: Mapped[str]
    url: Mapped[str] = mapped_column(String, nullable=False)

class City(Base):
    __tablename__ = "cities"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    city: Mapped[str] = mapped_column(String, nullable=False)
    region: Mapped[str] = mapped_column(String, nullable=False)