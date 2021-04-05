from keepitsecret.decrypt_file import decrypt_secrets
import psycopg2
import datetime
import logging
from utils.helper_functions import set_logger, get_latest_file

class Database:
    def __init__(self, config):
        self.config = config
        self.stage_list = self.config.stage_list
        self.final_stage = self.stage_list[-2]
        
        self.secret_dict = decrypt_secrets()

        logger = logging.getLogger(__name__)
        self.logger = set_logger(config=self.config, logger=logger)

    def connect_db(self):

        pg_host = self.secret_dict["PG_HOST"]     
        pg_port = self.secret_dict["PG_PORT"]
        pg_db = self.secret_dict["PG_DB"]
        pg_user = self.secret_dict["PG_USER"]
        pg_pass = self.secret_dict["PG_PASS"]
        
        pg_connection_string = "host=" + pg_host + " port=" + pg_port + \
                                " dbname=" + pg_db + " user=" + pg_user + \
                                " password=" + pg_pass
        
        pg_connection = psycopg2.connect(pg_connection_string)
        self.logger.debug(f"Successfully connected to {pg_db}")
        pg_connection.autocommit = True

        self.postgresdb = pg_connection
        return pg_connection

    def form_query_section(self, result_dict, stage, table, primary_key,
                            foreign_keys=[], foreign_stages=[],
                            returning=False, segment:str=None):
        # set up strings containing the foreign keys
        fks = ','.join(foreign_keys)

        # here we check if the foreign keys are already specified in the results -
        # if they are not we will need to select tdetailshem.        
        fts, fks = [], []
        for i, f in enumerate(foreign_stages) :
            if foreign_keys[i] not in result_dict[stage][table]['columns'] :
                fts.append("(SELECT " + foreign_keys[i] + " FROM " + f + " )" )
                fks.append(foreign_keys[i])
        fts = ','.join(fts)
        fks = ','.join(fks)
        if len(fks) > 0 :
            fts += ','
            fks += ','

        # set up the column names
        cols = [ "\"" + c + "\"" if '.' in c else c for c in result_dict[stage][table]['columns']]
        cols = fks + ','.join(cols)
        holders = ','.join(len(result_dict[stage][table]['columns']) * ['%s'] )
        
        # generate the query section 
        vals = result_dict[stage][table]['values']
        
        if stage == self.final_stage and segment =="last":
            insert = " INSERT INTO " + table + "(" + cols + ") values(" + fts + holders + ");"
        else :
            if returning :
                insert = stage + "_" + table + " AS (INSERT INTO " + table + "(" + cols + ") values(" + fts + holders + ") RETURNING " + primary_key + ")"
            else :
                insert = stage + "_" + table + " AS (INSERT INTO " + table + "(" + cols + ") values(" + fts + holders + "))" 
        
        return insert, vals


    def push_results_to_postgresdb(self, result_dict, database_conn) :
    
        query_parts = []
        values = []
        
        # Not expecting the "suburb" in "suburb_load" having any value in case using self.config.use_suburb_cache
        if "suburb_load" in result_dict and "suburb" in result_dict["suburb_load"] :
            sx, vx = self.form_query_section(result_dict, stage="suburb_load",
                                            table="suburb", primary_key="suburb_name",
                                            returning=True)
            
            query_parts += [sx]
            values += vx

        if "suburb_load" in result_dict and "suburb_stats" in result_dict["suburb_load"]:
            if self.config.use_suburb_cache:
                sx, vx = self.form_query_section(result_dict, stage="suburb_load", table="suburb_stats",
                                            primary_key="suburb_stats_id",returning=False, segment="last")
            else:
                sx, vx = self.form_query_section(result_dict, stage="suburb_load", table="suburb_stats",
                                            primary_key="suburb_stats_id", foreign_keys=["suburb_name"],
                                            foreign_stages=["suburb_load_suburb"], 
                                            returning=False, segment="last")
        
            query_parts += [sx]
            values += vx

        if self.config.use_suburb_cache:
            query = ",".join(query_parts[0:-1]) + query_parts[-1]
        else:
            query = "WITH " + ",".join(query_parts[0:-1]) + query_parts[-1]

        # print(f"Suburb: {values[2]}...... QUERY: {query}")
        values = tuple(values)

        # push to the database
        # self.connect_db()
        if database_conn is not None:
            # print(f"postgresdb: {self.postgresdb}")
            error_found = False
            error_message = None
            
            try :
                db = database_conn
                cursor = db.cursor()
                cursor.execute(query, values)
                print(f"XXXXXXXXXXX: Query executed successfully!")
                # print(f"valuesss: {values}")
                db.commit()

            except Exception as e:
                self.logger.error(f"Suburb {values[0]} is having this error:{e}")
                error_found = True
                error_message = e

            finally:
                print(f"Cursor closing for Suburb {values[0]}")
                # self.lock.release()
                cursor.close()

                if error_found:
                    return error_message, False

                else:
                    return "No error message", True
        else:
            raise ConnectionError("Postgres Database is not connected.")

    def form_batch_id_query(self, scrape_batch_id: int=None):
        if scrape_batch_id is None:
            batch_id_query = f"""WITH get_batch_id as (INSERT INTO scrape_batch_run(scrape_batch_run_date, config) VALUES(%s, %s) RETURNING scrape_batch_id) SELECT scrape_batch_id FROM get_batch_id;"""

        else:
            batch_id_query = f"""WITH get_batch_id as (INSERT INTO scrape_batch_run(scrape_batch_id, scrape_batch_run_date, config) VALUES({scrape_batch_id}, %s, %s) RETURNING scrape_batch_id) SELECT scrape_batch_id FROM get_batch_id;"""

        return batch_id_query

    def push_batch_id_to_database(self, database_conn, config):
        batch_id_query = self.form_batch_id_query()

        if database_conn is not None:
            error_found = False
            error_message = None
            try:
                # Assign the values
                config.scrape_batch_ts = str(datetime.datetime.utcnow())
                print(f"batch_ts: {config.scrape_batch_ts}")
                # Convert the config for psycopg2 json object for pushing to database
                psycopg2_json_config = psycopg2.extras.Json(self.config.__dict__)
                
                # Gather the values
                values = (config.scrape_batch_ts, psycopg2_json_config)
                
                #Connect to teh database
                db = database_conn
                cursor = db.cursor()
                cursor.execute(batch_id_query, values)
                scrape_batch_id = cursor.fetchone()[0]
                print(f"scrape_batch_id: {scrape_batch_id}")
                db.commit()
                
                # Attach the scrape_batch_id to the config
                config.scrape_batch_id = scrape_batch_id
                
            except Exception as e:
                print(f"ERROR: {e}")
                error_found = True
                error_message = e

            finally:
                cursor.close()
                print(f"cursor_closed")

        else:
            raise ConnectionError("Postgres database is not connected!")
        
        return scrape_batch_id

    def form_query_push_log_file(self, config, log_file):
        query = f"""UPDATE scrape_batch_run SET log_file={log_file} WHERE scrape_batch_id={config.scrape_batch_id};"""
        return query
    
    def push_log_to_db(self, config, database_conn):
        """Push the log file to the database"""
        log_file_read, log_file_path = get_latest_file(config.log_dir)
        log_file_read = psycopg2.Binary(log_file_read)

        self.logger.debug(f"Reading log file {log_file_path}")

        query = self.form_query_push_log_file(config, log_file_read)
        print(f'query: {query}')

        if database_conn is not None:
            try:
                db = database_conn
                cursor = db.cursor()
                cursor.execute(query)
                self.logger.debug(f"Pushed log file for scrape_batch_id {config.scrape_batch_id}!")
                db.commit()
                
            except Exception as e:
                print(f"ERROR: {e}")
                error_found = True
                error_message = e

            finally:
                cursor.close()
                print(f"cursor_closed")

        else:
            raise ConnectionError("Postgres database is not connected!")
       


        
