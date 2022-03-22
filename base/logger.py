import logging


# General logging parameters
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

logging.getLogger('urllib3.connectionpool').setLevel(level=logging.WARNING)
logging.getLogger('fiona').setLevel(level=logging.WARNING)
logging.getLogger('sqlalchemy.engine').setLevel(level=logging.WARNING)
logging.getLogger('sqlalchemy.engine.base.Engine').setLevel(level=logging.WARNING)
logging.getLogger('google.auth').setLevel(level=logging.WARNING)
logging.getLogger('urllib3').setLevel(level=logging.WARNING)
logging.getLogger('botocore').setLevel(level=logging.WARNING)
logging.getLogger('shapely').setLevel(level=logging.WARNING)

logger = logging.getLogger('FOSSIL_SHIPMENT_TRACKER')
logger.setLevel(logging.INFO)

# Adding an error logging in file
# logger_fh = logging.FileHandler('error.log')
# logger_fh.setLevel(logging.ERROR)
# logger.addHandler(logger_fh)