import time


class Lap:
    def __init__(self, tag):
        self.start = None
        self.tag = tag

    def __enter__(self):
        self.start = time.time()
        print('[%s]: start' % (self.tag,))

    def __exit__(self, exception_type, exception_value, traceback):
        interval = time.time() - self.start
        print('[%s]: elapsed time = %d' % (self.tag, interval))
