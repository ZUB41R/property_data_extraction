
import os
import time
import pickle
import random


class AppConfig:
    def __init__(self):
        # Name assigned to this instance of the application
        self.name = "WORKER-" + str(random.randint(10000, 99999))
        self.timestr = time.strftime("%Y%m%d-%H%M%S")
        self.scrape_batch_id = None

        self.use_suburb_cache = (os.environ["USE_SUBURB_CACHE"] == 'true')
        print(f"self.use_suburb_cache: {type(self.use_suburb_cache)}")
        
        # Assign the urls
        self.base_url_reiwa = os.environ["BASE_URL_REIWA"]
        self.base_url_realestate = os.environ["BASE_URL_REALESTATE"]
        
        self.suburb_list_url_tail = os.environ["SUBURB_LIST_URL_TAIL"]
        
        self.config_dir = os.environ["CONFIG_DIR"]
        self.config_path = os.path.join(self.config_dir, "config_" + self.timestr)
        self.create_dir(self.config_dir)

        self.suburb_dir = os.environ["SUBURB_DIR"]
        self.suburb_path = os.path.join(self.suburb_dir, "suburb_" + self.timestr)
        self.create_dir(self.suburb_dir)
        
        self.log_dir = os.environ["LOG_DIR"]
        self.log_path = os.path.join(self.log_dir, "log_" + self.timestr +".log")
        self.create_dir(self.log_dir)
        
        self.suburb_main_dict: dict = {}
        self.suburb_base_urls: dict = {}

        stage_list = os.environ["STAGE_LIST"]
        self.stage_list = [stage for stage in stage_list.split(',')]

        # Placeholder for the raw scrape pages
        self.raw_scrape_suburb_list = None

        # self.suburb_key = set()
        # self.suburb_stats_key = set()
        #Write the config file with a timestamp
        self.timestr = time.strftime("%Y%m%d-%H%M%S")

    def write_config(self):
        self.create_dir(self.config_dir)
        self.save_config_file = os.path.join(self.config_dir, f"Config_{self.timestr}.bin")
        with open(self.save_config_file, 'wb') as conf:
            pickle.dump(self, conf)

    def create_dir(self, input_dir):
        if not os.path.exists(input_dir):
            os.makedirs(input_dir)

