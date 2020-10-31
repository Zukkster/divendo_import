import pysftp
from parse_config import fn_parse_config
from configparser import ConfigParser
import logging
import logging.config
import datetime
import socket

def fn_sftp_get_files():
    
    #initialise logging function
    logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
    logger=logging.getLogger(__name__)
    
    #get details from config files
    sftp_params = fn_parse_config("etl_process.ini", "remote_sftp")
    sftp_folders = fn_parse_config("etl_process.ini", "sftp_folders")
    
    #login to sftp
    try:
        logger.info("SFTP Connection:Send credentials")
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        sftp = pysftp.Connection(**sftp_params)
        logger.info("SFTP Connection:SUCCESS")

    except (AttributeError, socket.gaierror):
        # couldn't connect
        raise ConnectionException(host, port)
        logger.error("SFTP Connection: Connection Error")

    #retrieve files
    try:
        for file in sftp.listdir("/"):
        
            sftp.get(remotepath = sftp_folders.get('remote_path') + file, localpath=sftp_folders.get('local_path')  + file )
            logger.info("RETRIEVED SFTP FILE: " + file)
            sftp.remove(sftp_folders.get('remote_path') + file)
            logger.info("REMOVED SFTP FILE: " + file)
    except:
            logger.info("No files to retrieve")	

    logger.info("SFTP Connection:CLOSED")
    
