from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from scraper.models import Car
from scraper.config import settings
import logging


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
        item["region"] = session.get_one()
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
