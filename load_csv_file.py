import pandas as pd
from sqlalchemy import create_engine
from parse_config import fn_parse_config
from configparser import ConfigParser
import logging
import logging.config
import datetime
import os

def fn_load_csv_file(f_name, load_table_name):
    
    #initialise logging function   
    logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
    logger=logging.getLogger(__name__)

    
    try:
        login_params = fn_parse_config("etl_process.ini", "postgresql")
        file_path = fn_parse_config("etl_process.ini", "sftp_folders")
        load_path = file_path.get('local_path')


        logger.info("IMPORT FILE:Connecting to database")
        url = 'postgresql+psycopg2://{user}:{pass}@{host}:5432/{db}'.format(**login_params)  # 5432 is the default port
        engine = create_engine(url, client_encoding='utf8')

        df = pd.read_csv(load_path + f_name)  # this returns a DataFrame
        logger.info("IMPORT FILE:Loaded file " + load_path + f_name + " to dataframe")
        from re import sub
        old_columns = list(df.columns)  # create list of columns
        new_columns = [sub('[^A-Za-z0-9_]+', '_', _) for _ in old_columns]  # list comprehension with regex sub
        df = df.rename(columns=dict(zip(old_columns, new_columns)))  # zip as {old_column: new_column}

        df.to_sql(load_table_name, con=engine, if_exists='append')
        logger.info("IMPORT FILE:Inserted dataframe into loading table " + load_table_name)
    
    except Exception as err:
        #write the error to the log
        logger.error("FILENAME"+f_name)
        logger.error(err)
