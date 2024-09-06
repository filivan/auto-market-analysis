import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from scraper.models import Car, City
from scraper.config import settings


class SQLAlchemyPipeline:
    def open_spider(self, spider):
        self.engine = create_engine(settings.DATABASE_URL)
        self.Session = sessionmaker(bind=self.engine)
        logging.info("PostgreSQL connection established.")

    def close_spider(self, spider):
        self.engine.dispose()
        logging.info("PostgreSQL connection closed.")

    def process_item(self, item, spider):
        session = self.Session()
        city = session.query(City).filter_by(city=item["city_ru"]).first()
        item["region"] = city.region if city is not None else None
        logging.info(f"Item {item['id']} region is {item["region"]}")
        try:
            car = Car(**item)
            session.add(car)
            session.commit()
            logging.info(f"Item {item['id']} stored in PostgreSQL.")
        except Exception as e:
            session.rollback()
            logging.error(f"Error storing item {item['id']}: {e}")
        finally:
            session.close()
        return item
