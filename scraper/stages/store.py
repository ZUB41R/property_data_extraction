from scraper.stages.stage import Stage, Minion
from scraper.database import Database
import psycopg2
from psycopg2 import extras
import pandas as pd
import datetime
import numpy as np
from utils.helper_functions import set_logger


class StoreMinion(Minion):
    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
        # Mostly, just do what the parent used to do.
        super().__init__(group=group, target=target, name=name, daemon=daemon, args=args, kwargs=kwargs)

        self.suburb_task = args[0]
        self.config = args[1]
        self.database_conn = args[2]
        self.logger = args[3]
        
        self.database = Database(self.config)

        self.suburb_task_string = "Storing_results:" + self.suburb_task.name

    def work(self):
        e, published = self.publish_results(task_result=self.suburb_task.details)

        if published:
            self.logger.debug("Success:" + self.suburb_task_string)
            return True

        else:
            self.logger.debug("Failed:" + self.suburb_task_string)
            print(e)
            return False
       
    def publish_results(self, task_result) :
        """Publishes the data that are related to the transaction id (top level)""" 

        json_result = task_result

        stage, table, column, value = [], [], [], []
        for key in json_result.keys() :

            split_key = key.split('.')

            if len(split_key) >= 3 :
                stage.append(split_key[0])
                table.append(split_key[1])
                column.append('.'.join(split_key[2:]))
                value.append(json_result[key])

        # print(f"valus_1: {value}")
        results = pd.DataFrame({'stage': stage, 'table': table, 'column': column, 'value': value})
        
        # make a unique list of the stages we have data to publish
        stage = list(set(stage))
        # make an output dictionary to help with formatting to posgres
        result_dict = {}
        
        # go through each stage getting the results in a useful format
        for s in stage :        
            result_dict.update({s: {}})     
            stage_results = results[results['stage'] == s]
            table_list = list(set(stage_results['table']))
            for t in table_list : 
                # subset to a particular table
                table_results = stage_results[stage_results['table'] == t]
                # make lists of the columns and corresponding values
                columns = list(table_results['column'])
                values = list(table_results['value'])
                # convert any json elements to appropriate format
                values = [ psycopg2.extras.Json(v) if isinstance(v, dict) else v for v in values ]
                # convert any datetime elements 
                values = [ str(v) if isinstance(v, datetime.datetime) else v for v in values ]
                # convert floats
                values = [ float(v) if isinstance(v, np.float32) else v for v in values ]
                # make sure all the column values are strings
                columns = [ str(c) for c in columns ]                    
                # update the output dictionary
                result_dict[s].update({t:{'columns': columns, 'values': values}})

        error, published = self.database.push_results_to_postgresdb(result_dict, database_conn=self.database_conn)

        return error, published

class StoreStage(Stage):
    def __init__(self, config, database_conn):
        Stage.__init__(self, "Store")

        self.config = config
        self.database_conn = database_conn
        logger = self.logger_minion
        self.logger = set_logger(config=self.config, logger=logger)

        self.batch_size = 10

    def make_minion(self, task):
        return StoreMinion(args=(task, self.config, self.database_conn, self.logger))