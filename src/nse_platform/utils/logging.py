import logging

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"


def make_logger(name: str):
    """Return a configured logger for the given name.

    Idempotent: calling twice with the same name returns the same
    logger without attaching duplicate handlers.
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        formatter = logging.Formatter(LOG_FORMAT)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
        logger.setLevel(logging.INFO)
    return logger
