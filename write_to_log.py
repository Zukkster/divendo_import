import logging
import logging.handlers
import datetime
from parse_config import fn_parse_config

mydate_str = datetime.datetime.now().strftime("%Y%m%d%HH%mm") 

f = logging.Formatter(fmt='%(levelname)s:%(name)s: %(message)s '
                      '(%(asctime)s; %(filename)s:%(lineno)d)',
                      datefmt="%Y-%m-%d %H:%M:%S")
handlers = [
    logging.handlers.TimedRotatingFileHandler('./log/rotated_' + mydate_str + '.log', encoding='utf8', when='midnight', maxBytes=100000, backupCount=15, interval=1),
    logging.StreamHandler()
]
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
for h in handlers:
    h.setFormatter(f)
    h.setLevel(logging.DEBUG)
    root_logger.addHandler(h)