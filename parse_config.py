from configparser import ConfigParser
import logging
import logging.config
     
def fn_parse_config(filename, section):
    
    global logger
    
    #logging.config.fileConfig('logging.conf', disable_existing_loggers=False)
    #logger=logging.getLogger(__name__)
    
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)
 
    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
        logger.error('Section {0} not found in the {1} file'.format(section, filename))
 
    return db

