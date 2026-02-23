import logging
import sys


def logs(name=None):
    logger = logging.getLogger(name or __name__)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        logger.setLevel(logging.INFO)
        
        formatter = logging.Formatter(
             "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )

        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False
        
    return logger