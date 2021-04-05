import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime

from scraper.stages.stage import Stage, Minion
from utils.suburb_task import SuburbTask
from utils.helper_functions import set_logger


class SuburbLoadMinion(Minion):
    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
    # Mostly, just do what the parent used to do.
        super().__init__(group=group, target=target, name=name, daemon=daemon, args=args, kwargs=kwargs)
        
        self.suburb_task = args[0]
        self.config = args[1]
        self.logger = args[2]
        

    def work(self):
        if self.tag_details():
            self.logger.debug("Success:" + self.suburb_task_string)
            return True

        else:
            # Recognised as failed.
            self.logger.debug("Failed:" + self.suburb_task_string)
            return False

    def get_suburb_details(self, suburb_main_url):
        """ Scrapes the suburb details from REIWA"""
       #TODO: make a global replace function
        source = requests.get(suburb_main_url).text
        time.sleep(3) # Sleep 3 seconds in between every request!
        soup = BeautifulSoup(source, 'lxml')

        tables = soup.find('div', class_= "table-responsive table-responsive-mobile-no-border")
        tables_for_stats = tables.find_all('table', class_="data-table data-table-alt table-hover")
        table_key_facts = None
        
        # print(f" ....{tables_for_stats[0]}")
        if len(tables_for_stats) > 0: # and tables_for_stats[0].h3.text.lower().replace(" ", "") == "keyfacts":
            table_key_facts = tables_for_stats[0]
        
        table_census_summary_2016 = tables.find('table', class_="data-table data-table-alt table-hover", title="2016 Census Summary")
        # print(f" ++++{table_census_summary_2016}")
        # if len(tables_for_stats) > 1 and tables_for_stats[1].title.text().lower().replace(" ", "")=="2016censussummary":
        #     table_census_summary_2016 = tables_for_stats[1]
        
        key_facts = {}
        if table_key_facts is not None:
            for tr in table_key_facts.find_all('tr'):
                tds = tr.find_all('td')
                key_facts.update({tds[0].text.lower().replace(" ", "_").replace("(", "").replace(")", ""): tds[1].text.replace("\n", "").replace(",", "")})

        census_summary_2016 = {}
        if table_census_summary_2016 is not None:
            for tr in table_census_summary_2016.find_all('tr')[1:]:
                tds = tr.find_all('td')
                census_summary_2016.update({tds[0].text.lower().replace(" ", "_").replace("(", "").replace(")", "").replace("\n", "").replace("*", "").replace("-", ""): tds[1].text.replace("\n", "").replace("$", "").replace(",", "")})

        for suburb_stat in soup.find_all('div', class_='suburb-profile-stats-mobile'):
            if suburb_stat.small.text.lower() == "annualgrowth":
                census_summary_2016.update({'annual_growth': float(suburb_stat.strong.text.replace("%", "").replace(",", ""))})
            elif suburb_stat.small.text.lower() == "annualmed.price":
                census_summary_2016.update({'annual_median_price': int(suburb_stat.strong.text.replace("$", "").replace(",", ""))})
            elif suburb_stat.small.text.lower() == "population":
                census_summary_2016.update({'population': int(suburb_stat.strong.text.replace("$", "").replace(",", ""))})
            else:
                print(f'Got more suburb_stats than the expected 3')

        return source, key_facts, census_summary_2016


    def tag_details(self):
        suburb_task = self.suburb_task
        self.suburb_task_string = f"Downloading Taget: {suburb_task.name}"
        self.logger.debug("Work:" + self.suburb_task_string)
        
        url = self.config.suburb_base_urls[suburb_task.name]["suburb_main_url"]
        
        source, key_facts, census_summary_2016 = self.get_suburb_details(suburb_main_url=url)
        # combined_stats = {**key_facts, **census_summary_2016}
        # print(f"combined_stats: {combined_stats}")

        try:
            #TODO: Debug error for set object is not json serializable in self.config.__dict__
            
            # Table: suburb
            # we are not running this if we are using suburb list from file
            if not self.config.use_suburb_cache:
                suburb_task.tag_details("suburb_load.suburb.suburb_name", suburb_task.name)
                if "distance_to_perth_km" in key_facts:
                    suburb_task.tag_details("suburb_load.suburb.distance_to_perth_km", float(key_facts['distance_to_perth_km']))
                if "postcode" in key_facts:
                    suburb_task.tag_details("suburb_load.suburb.postcode", int(key_facts['postcode']))
                if "local_government" in key_facts:
                    suburb_task.tag_details("suburb_load.suburb.local_government", key_facts['local_government'])
                
                suburb_task.tag_details("suburb_load.suburb.scrape_batch_id", self.config.scrape_batch_id)

            if self.config.use_suburb_cache:    
                # Taking the suburb name from the task if uses cache otherwise from the returning value of the "suburb" table query
                suburb_task.tag_details("suburb_load.suburb_stats.suburb_name", suburb_task.name)

            # Table: suburb_stats
            if "primary_schools" in key_facts:
                suburb_task.tag_details("suburb_load.suburb_stats.primary_schools", key_facts['primary_schools'])
            if "secondary_schools" in key_facts:
                suburb_task.tag_details("suburb_load.suburb_stats.secondary_schools", key_facts['secondary_schools'])
            if "shops" in key_facts:
                suburb_task.tag_details("suburb_load.suburb_stats.shops", key_facts['shops'])
            if "train_stations" in key_facts:
                suburb_task.tag_details("suburb_load.suburb_stats.train_stations", key_facts['train_stations'])
            if "bus_services" in key_facts:
                suburb_task.tag_details("suburb_load.suburb_stats.bus_services", key_facts['bus_services'])
            
            suburb_task.tag_details("suburb_load.suburb_stats.suburb_link", url)
            
            if 'median_age_of_residents_years' in census_summary_2016:
                suburb_task.tag_details("suburb_load.suburb_stats.median_age_of_residents_years", float(census_summary_2016['median_age_of_residents_years']))
            if 'area_sqkm' in census_summary_2016:
                suburb_task.tag_details("suburb_load.suburb_stats.area_sqkm", int(census_summary_2016['area_sqkm']))
            if 'number_of_occupied_dwellings_' in census_summary_2016:
                suburb_task.tag_details("suburb_load.suburb_stats.number_of_occupied_dwellings_", int(census_summary_2016['number_of_occupied_dwellings_']))
            if 'annual_growth' in census_summary_2016:
                suburb_task.tag_details("suburb_load.suburb_stats.annual_growth", float(census_summary_2016['annual_growth']))
            if 'annual_median_price' in census_summary_2016:
                suburb_task.tag_details("suburb_load.suburb_stats.annual_median_price", int(census_summary_2016['annual_median_price']))
            if 'population' in census_summary_2016:
                suburb_task.tag_details("suburb_load.suburb_stats.population", int(census_summary_2016['population']))
            if 'average_household_size_persons' in census_summary_2016:
                suburb_task.tag_details("suburb_load.suburb_stats.average_household_size_persons", float(census_summary_2016['average_household_size_persons']))
            if 'median_weekly_household_income' in census_summary_2016:
                suburb_task.tag_details("suburb_load.suburb_stats.median_weekly_household_income", int(census_summary_2016['median_weekly_household_income']))
            if 'median_monthly_mortgage_repayment' in census_summary_2016:
                suburb_task.tag_details("suburb_load.suburb_stats.median_monthly_mortgage_repayment", int(census_summary_2016['median_monthly_mortgage_repayment']))
            if 'population__usually_resident' in census_summary_2016:
                suburb_task.tag_details("suburb_load.suburb_stats.population__usually_resident", int(census_summary_2016['population__usually_resident']))
            if 'total_private_dwellings' in census_summary_2016:
                suburb_task.tag_details("suburb_load.suburb_stats.total_private_dwellings", int(census_summary_2016['total_private_dwellings']))
            if 'number_of_unoccupied_dwellings' in census_summary_2016:
                suburb_task.tag_details("suburb_load.suburb_stats.number_of_unoccupied_dwellings", int(census_summary_2016['number_of_unoccupied_dwellings']))

            suburb_task.tag_details("suburb_load.suburb_stats.suburb_raw_page", source)
            
            suburb_task.tag_details("suburb_load.suburb_stats.scrape_batch_id", self.config.scrape_batch_id)
                        
            
            # for key, value in census_summary_2016.items():
            #     suburb_task.tag_details(f"suburb_load.suburb_stats.{key}", value)
            #     self.config.suburb_stats_key.add(key)
            
            # print(f"suburb_key: {self.config.suburb_key}")
            # print(f"suburb_stats_key: {self.config.suburb_stats_key}")
            return True

        except Exception as e:
            print(f"suburb: {suburb_task.name} is having error!")
            print(e)
            return False
            
class SuburbLoadStage(Stage):
    def __init__(self, config):
        Stage.__init__(self, name="suburb_load")

        self.config = config
        logger = self.logger_minion
        self.logger = set_logger(config=self.config, logger=logger)

    def make_minion(self, suburb_task):
        return SuburbLoadMinion(args=(suburb_task, self.config, self.logger))


if __name__ == "__main__":
    sd = SuburbLoadMinion()
    sd.get_suburb_details("https://reiwa.com.au/suburb/applecross")

    

    