import logging

def get_logger():
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    return logger

