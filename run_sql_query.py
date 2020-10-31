import psycopg2
import smtplib
from parse_config import fn_parse_config
from configparser import ConfigParser
import datetime
import logging
import logging.config

def fn_run_sql_query(sql_str):  
    #get details from config files
    #initialise logging function   
    logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
    logger=logging.getLogger(__name__)
    
    del_params = fn_parse_config("etl_process.ini", "psycopg_login")

    conn = None
    try:
        print("Test")
        #connect to the PostgreSQL server
        conn = psycopg2.connect(**del_params)
        conn.autocommit = True
        logger.info("DATABASE:login complete")
        #create a cursor
        cur = conn.cursor()

        #execute a statement

        cur.execute(sql_str)
        #display the PostgreSQL database server version
        query_results = cur.fetchone()

        #close the communication with the PostgreSQL
        cur.close()
        
        return query_results


    except (Exception, psycopg2.DatabaseError) as error:
        print (error) 
        logger.error(error)
    finally:
        logger.info("Ran SQL query")
        if conn is not None:
            conn.close()
            print('Done')