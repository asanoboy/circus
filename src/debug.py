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
            self.logger.addHandler(file_handler)

    def timestr(self):
        return time.strftime('%D %T')

    def debug(self, *args, **kw):
        text = ' '.join([str(arg) for arg in args])
        text = '[%s] %s' % (self.timestr(), text)
        return self.logger.debug(text)

    def info(self, *args, **kw):
        text = ' '.join([str(arg) for arg in args])
        text = '[%s] %s' % (self.timestr(), text)
        return self.logger.debug(text)

    def warning(self, *args, **kw):
        text = ' '.join([str(arg) for arg in args])
        text = '[%s] %s' % (self.timestr(), text)
        return self.logger.info(text)

    def error(self, *args, **kw):
        text = ' '.join([str(arg) for arg in args])
        text = '[%s] %s' % (self.timestr(), text)
        return self.logger.error(text)

    def critical(self, *args, **kw):
        text = ' '.join([str(arg) for arg in args])
        text = '[%s] %s' % (self.timestr(), text)
        return self.logger.critical(text)

    def lap(self, tag):
        return Lap(tag, self)


class Timer:
    def __init__(self):
        self.start = time.time()

    def elapsed(self):
        return time.time() - self.start

    def reset(self):
        self.start = time.time()


@contextmanager
def Lap(tag, logger=None):
    global level
    timer = Timer()
    log = '%s>> [%s]' % ('  ' * level, tag)
    if logger:
        logger.debug(log)
    else:
        print(log)
    level += 1

    try:
        yield timer
    finally:
        level -= 1
        interval = timer.elapsed()
        log = '%s<< [%s] | elapsed time = %d' % \
            ('  ' * level, tag, interval)

        if logger:
            logger.debug(log)
        else:
            print(log)
