import logging

logging.basicConfig()


def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.CRITICAL)
    return logger
