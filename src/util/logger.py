import logging as log

def setup_logging():
    log.basicConfig(format=log.BASIC_FORMAT, level=log.INFO)
    logger = log.getLogger()
    print(logger.handlers)
    assert len(logger.handlers) == 1
    handler = logger.handlers[0]
    handler.setLevel(log.DEBUG)
    formatter = log.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    return logger