import fire
import time
import logging
import os
from scraper.worker import Worker
from scraper.database import Database
from utils.config import AppConfig
from utils.helper_functions import set_logger, get_latest_file
from scraper.base_url_collector import BaseUrlCollector


class ScraperApp:
    def __init__(self):
        self.config = AppConfig()
        self.postgres_database = Database(self.config)
    
        # Set the logger
        logger = logging.getLogger(__name__)
        self.logger = set_logger(self.config, logger)
        self.logger.debug("ScraperApp Starts")

    def get_base_urls(self):
        #TODO: perform all the checks here
        buc = BaseUrlCollector(self.config, self.logger) 
        self.config.suburb_base_urls = buc.get_suburb_url_reiwa()

    def setup(self):
        
        # If we are using previously scraped suburb urls 
        if self.config.use_suburb_cache:
            self.config.suburb_base_urls, latest_file_path = get_latest_file(self.config.suburb_dir)
            self.logger.debug(f"Using the latest suburb list from {latest_file_path}")

            if self.config.suburb_base_urls is None:
                self.logger.warning(f"No suburb base urls found in {latest_file_path}..Going to scrape again!")
                self.get_base_urls()
                self.logger.debug(f"Just scraped the suburb base urls after not finding from the file!")

        # Or scraping nowthe latest
        else:
            self.get_base_urls()
            self.logger.debug(f"Using the suburb list just scraped!")
        
        # Connect to the database
        self.database_conn = self.postgres_database.connect_db()
        
        # Assign the scrape_batch_id
        self.postgres_database.push_batch_id_to_database(self.database_conn, self.config)

        # # Write the config file to a binary file
        # self.config.write_config()

        self.worker = Worker(self.logger, self.config, self.database_conn)

    def run(self):
        start_time = time.time()
        self.setup()

        self.worker.run()
        num_suburbs = len(self.config.suburb_base_urls.keys())
        time_taken = time.time() - start_time

        self.logger.debug(f"Total time takes {time_taken} seconds.")
        self.logger.info(f"Average time takes {time_taken/num_suburbs} seconds for {num_suburbs} suburbs.")

        self.end()
        self.logger.info(f"ScraperApp ends, Congratulations ?!")

    def end(self):
        """Closing the works"""
        # Send the log file to the database
        self.postgres_database.push_log_to_db(config=self.config, database_conn=self.database_conn)


        # self.database_conn.close()
        # self.logger.info(f"Database connection closed")


if __name__ == "__main__":
    fire.Fire(ScraperApp)
    print('[[DONE]]')


