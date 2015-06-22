import time


class Lap:
    _level = 0

    def __init__(self, tag):
        self.start = None
        self.tag = tag

    def __enter__(self):
        self.start = time.time()
        print('%s>> %s' % ('  ' * Lap._level, self.tag,))
        Lap._level += 1

    def __exit__(self, exception_type, exception_value, traceback):
        Lap._level -= 1
        interval = time.time() - self.start
        print(
            '%s<< %s | elapsed time = %d' %
            ('  ' * Lap._level, self.tag, interval))
