import psycopg2
from parse_config import fn_parse_config
import logging
import logging.config
 
def fn_insert_load_to_staging(f_name, proc_name):
    
    logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
    logger=logging.getLogger(__name__)
    
    conn = None
    try:
        # read database configuration
        login_params = fn_parse_config("etl_process.ini", "psycopg_login")
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**login_params)
        conn.autocommit = True
        # create a cursor object for execution
        cur = conn.cursor()
        # another way to call a stored procedure
        #cur.execute("SELECT * FROM usp_test();")
        cur.callproc(proc_name, (f_name,))
        # process the result set
        row = cur.fetchall()
        #results=row 
        #while row is not None:
        #    print('x_test')
        #    row = cur.fetchall()
        # close the communication with the PostgreSQL database server
       
        cur.close()
        
        return row
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        logger.error(error)
    finally:
        logger.info("Completed loading files to staging")
        if conn is not None:
            conn.close()
