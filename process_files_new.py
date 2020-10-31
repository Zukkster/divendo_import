from parse_config import fn_parse_config
from configparser import ConfigParser
import logging
import logging.config
import zipfile
import os
import datetime
import fnmatch
from run_sql_query import fn_run_sql_query 
from load_csv_file import fn_load_csv_file
from insert_load_to_staging import fn_insert_load_to_staging


def fn_process_files_new():

    #initialise logging function   
    logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
    logger=logging.getLogger(__name__)
    
    file_count=0
    
    logger.info("DATABASE:login")
    login_params = fn_parse_config("etl_process.ini", "sftp_folders")
    loaded_path = login_params.get('local_path')
    archive_path = login_params.get('archive_path')
    #loaded_path="C:\\Users\\stuar\\eclipse-workspace\\vivobarefoot\\data\\"
    #archive_path="C:\\Users\\stuar\\eclipse-workspace\\vivobarefoot\\archived\\"
    logger.info("DATABASE:login complete")
    #make sure there is nothing in file load tables before 
    fn_run_sql_query('select from usp_truncate_file_load_tables();')
    logger.info("Truncate loading tables")
    
	
    try:
        #check tere are file sin the folder
        if  os.listdir(loaded_path):
            for folder, subfolders, files in os.walk(loaded_path):
                for file in sorted(files):
                    file_count+=1
                    logger.info("PROCESS FILE:"+file)
                    #look for FULFILLED files - these need to be processed first
                    if fnmatch.fnmatch(file, 'vb-fulfilled*.csv'):
                        print(file)
                        logger.info("CHECK 1")
                        fn_load_csv_file(file, 'file_load_fulfilled')
                        logger.info("CHECK 2")
                        cancelled_file = file.replace("vb-fulfilled-", "vb-cancelled-")
                        
                        #archive file
                        archive_zip = zipfile.ZipFile(archive_path + os.path.splitext(os.path.basename(file))[0] + '.zip', 'w')
                        archive_zip.write(os.path.join(folder, file), os.path.relpath(os.path.join(folder,file), loaded_path), compress_type = zipfile.ZIP_DEFLATED)
                        os.remove(os.path.join(folder, file))        
                        archive_zip.close()
                        if os.path.exists(loaded_path + cancelled_file):
                            logger.info("CHECK 3")
                            fn_load_csv_file(cancelled_file, 'file_load_cancelled')
                            logger.info("CHECK 4")
                            #archive file
                            archive_zip = zipfile.ZipFile(archive_path + os.path.splitext(os.path.basename(cancelled_file))[0] + '.zip', 'w')
                            archive_zip.write(os.path.join(folder, cancelled_file), os.path.relpath(os.path.join(folder,cancelled_file), loaded_path), compress_type = zipfile.ZIP_DEFLATED)
                            os.remove(os.path.join(folder, cancelled_file))        
                            archive_zip.close()
                            
                        else:
                            logger.error("FILE NOT FOUND: " + file)
                        
                        logger.info("UPDATE SQL TABLES:Running postgres function")    
                        #insert records from load table to staging
                        #result = fn_insert_load_to_staging(file, 'testing.usp_run_file_update')
                        result = fn_insert_load_to_staging(file, 'usp_run_file_update')
                        if result==[(True,)]:
                            logger.info("SQL UPDATES COMPLETED:Tables updated successfully for file " + file)
                            print('File loaded')
                        else:
                            logger.error("FILE REJECTED:Already loaded file " + file + " file zipped but not loaded")
                            print('Nothing loaded')
                    else:
                        logger.info("CHECK5")

        else:
            logger.info("No files to process")
            
        return file_count
            
    except Exception as err:
        #write the error to the log
        logger.error(err)