import time
import logging
from contextlib import contextmanager


level = 0
file_handler = None


def set_config(path, log_level=logging.DEBUG):
    global file_handler
    file_handler = logging.FileHandler(path, 'a+')
    logging.basicConfig(level=log_level)


def get_logger(name):
    return Logger(name)


class Logger:
    def __init__(self, name):
        global file_handler
        self.logger = logging.getLogger(name)
        if file_handler:
            print('set file handler')
            self.logger.addHandler(file_handler)

    def debug(self, *args, **kw):
        return self.logger.debug(*args, **kw)

    def info(self, *args, **kw):
        return self.logger.info(*args, **kw)

    def warning(self, *args, **kw):
        return self.logger.warning(*args, **kw)

    def error(self, *args, **kw):
        return self.logger.error(*args, **kw)

    def critical(self, *args, **kw):
        return self.logger.critical(*args, **kw)

    def lap(self, tag):
        return Lap(tag, self.logger)


@contextmanager
def Lap(tag, logger=None):
    global level
    start = time.time()
    log = '%s>> %s' % ('  ' * level, tag)
    if logger:
        logger.debug(log)
    else:
        print(log)
    level += 1

    yield

    level -= 1
    interval = time.time() - start
    log = '%s<< %s | elapsed time = %d' % \
        ('  ' * level, tag, interval)

    if logger:
        logger.debug(log)
    else:
        print(log)
