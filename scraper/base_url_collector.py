from bs4 import BeautifulSoup
import requests
import os
from utils.config import AppConfig
from utils.helper_functions import dumppickle, create_dir, set_logger
from utils.helper_functions import create_dir
import logging


class BaseUrlCollector:
    def __init__(self, config, logger):
        self.config = config
        logger = logging.getLogger(__name__)
        self.logger = set_logger(config=self.config, logger=logger)
        
        self.base_url_reiwa = self.config.base_url_reiwa
        self.base_url_realestate = self.config.base_url_realestate

        self.logger.debug(f"Starting {self.__class__.__name__}")

    def get_suburb_list(self):
        # Set the url
        self.url_tail=self.config.suburb_list_url_tail
        
        suburb_list_url = os.path.join(self.base_url_reiwa, self.url_tail)
        self.logger.debug(f"Read the suburb_list_url: {suburb_list_url}")

        source = requests.get(suburb_list_url).text
        soup = BeautifulSoup(source, 'lxml')
        self.config.raw_scrape_suburb_list = source

        table = soup.find("table", class_="data-table members-data-table")

        # Get the header of the table
        header_row = table.find('tr')
        ths = []
        for th in header_row.find_all('th'):
            ths.append(th.text.lower())
        
        suburb_main_dict = {ths[0]: [], ths[1]: [], ths[2]: [], ths[3]: []}
        for table_row in table.find_all('tr')[1:]:
            tds = table_row.find_all('td')
            suburb_main_dict[ths[0]].append(tds[0].text.replace("\n", ""))
            suburb_main_dict[ths[1]].append(int(tds[1].text.replace("\n", "").replace("$", "").replace(",", "")))
            suburb_main_dict[ths[2]].append(float(tds[2].text.replace("\n", "").replace("%", "")))
            suburb_main_dict[ths[3]].append(tds[3].text.replace("\n", ""))
            
        # print(f"{len(suburb_main_dict[ths[0]])} suburbs have been listed!")
        
        # Attach the suburb names in config file
        self.config.suburb_main_dict = suburb_main_dict[ths[0]]
        
        # # Write to a binary file that can be used later
        # dumppickle(self.config.suburb_path, suburb_main_dict)
        self.logger.info(f"Found {len(self.config.suburb_main_dict)} suburbs!")
        
        return suburb_main_dict

    def get_suburb_url_reiwa(self):
        """ Takes a list of suburbs and creates an individual url of those according to the type of the property listing condition, e.g for buy or rent etc."""
        suburb_main_dict = self.get_suburb_list()
        # Remove white spaces and concatenate with -
        suburb_list = [suburb.replace(" ", "-").lower() for suburb in suburb_main_dict["suburb"]]

        suburb_connector = "suburb"
        for_sale_connector = "for-sale"
        for_rent_connector = "rental-properties"  #### TODO: This can be dynamic, i.e. scraping the urls
        sold_connector = "sold"

        # Get the plcaeholders
        suburb_all_urls = {}
        suburb_all_url = {}
        for suburb_name in suburb_list:
            suburb_all_url = {f"{suburb_name}": {}}
            suburb_all_urls.update(suburb_all_url)
        
        for suburb_name in suburb_list:
            suburb_all_urls[f"{suburb_name}"]["suburb_main_url"] = os.path.join(self.base_url_reiwa, suburb_connector, suburb_name)
            
            suburb_all_urls[f"{suburb_name}"]["suburb_for_sale_url"] = os.path.join(self.base_url_reiwa, for_sale_connector, suburb_name)
            
            suburb_all_urls[f"{suburb_name}"]["suburb_for_rent_url"] = os.path.join(self.base_url_reiwa, for_rent_connector, suburb_name)
            
            suburb_all_urls[f"{suburb_name}"]["suburb_sold_url"] = os.path.join(self.base_url_reiwa, sold_connector, suburb_name)

        # Write to a binary file that can be used later
        dumppickle(self.config.suburb_path, suburb_all_urls)
        self.logger.debug(f"suburb_base_urls are saved in {self.config.suburb_path}")
        # self.config.write_config()

        return suburb_all_urls

        
    # def get_suburb_url_realestate(self, suburb_postcode):
    #     suburb_url_list_realestate = []
    #     suburb_for_sale_list_realestate = []
    #     suburb_for_rent_list_realestate = []
    #     suburb_sold_list_realestate = []
        
    #     for_sale_connector = "buy"
    #     for_rent_connector = "rent"  #### TODO: This can be dynamic, i.e. scraping the urls
    #     sold_connector = "sold"

    #     for (suburb, postcode) in suburb_postcode:
    #         suburb_ = f"in-{suburb.replace(' ', '+')},+wa+{postcode}"
    #         for_sale_url = os.path.join(self.base_url_realestate, for_sale_connector, suburb_)
    #         for_rent_url = os.path.join(self.base_url_realestate, for_rent_connector, suburb_)
    #         sold_url = os.path.join(self.base_url_realestate, sold_connector, suburb_)
            
    #         suburb_for_sale_list_realestate.append(for_sale_url)
    #         suburb_for_rent_list_realestate.append(for_rent_url)
    #         suburb_sold_list_realestate.append(sold_url)
        
    #     reiwa_suburb_urls = {"suburb_main": suburb_url_main_list, 
    #                             "suburb_for_sale": suburb_for_sale_list_reiwa, 
    #                             "suburb_for_rent": suburb_for_rent_list_reiwa, 
    #                             "suburb_sold": suburb_sold_list_reiwa}

    #     return reiwa_suburb_urls


# if __name__ == "__main__":
#     buc = BaseUrlCollector()
#     table_dict = buc.get_suburb_list()
#     suburb_base_urls_dict = buc.get_suburb_url_reiwa(suburb_list=table_dict['suburb'])
#     print(f"suburb_base_urls_dict: {suburb_base_urls_dict}")

