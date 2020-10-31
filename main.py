from parse_config import fn_parse_config
#from configparser import configparser
import logging
import logging.config
import datetime
from sftp_get_files import fn_sftp_get_files
from process_files import fn_process_files
from run_postgres_updates import fn_run_postgres_updates
from completed_email import fn_write_postgres_csv
from completed_email import fn_send_csv_email
from email_error_logs import fn_email_error_logs
from os import path

def main():
    
    #initialise logging
    
    log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logging.conf')
    print(log_file_path)
    logging.config.fileConfig(log_file_path)
    logger=logging.getLogger(__name__)
    
    logger.info("START MAIN:Begin Process")
    
    #get the files
    #fn_sftp_get_files()
    
    #load the files into the staging tables
    #fn_process_files()
    
    #push data from staging to fact table
    fn_run_postgres_updates()
    
    #send email
    query = "select * from load_file_log where load_time >= current_timestamp - interval '1' hour;"
    
    fn_write_postgres_csv(query, "output_load")
    query = "select * from segment_history_log where load_date = current_date;"
    fn_write_postgres_csv(query, "output_segment")
    
    fn_send_csv_email()
    fn_email_error_logs()
    
    logger.info("END MAIN:Completed Process")

#if __name__== "__main__":
main()
