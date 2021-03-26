import logging

logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s - %(message)s')


def get_logger(logger_name) -> logging.Logger:
    """
    Returns logger with desired format.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    return logger
