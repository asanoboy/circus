from http_utils import get_proxy_list, get_html
from functools import reduce
from debug import get_logger
import time
import random


class ProxyManager:
    def __init__(self, interval=3600):
        self.proxy_list = []
        self.proxy_updated_at = 0
        self.update_interval = interval

    def update(self):
        if time.time() - self.proxy_updated_at > self.update_interval:
            if not self._update_proxy():
                raise 'Can\'t update proxy'

    def _update_proxy(self):
        logger = get_logger(__name__)
        for _ in range(5):
            candidates = get_proxy_list()
            if candidates is None:
                time.sleep(5)
                continue
            for proxy in self.proxy_list:
                logger.debug(proxy.dump())
            self.proxy_list = [ProxyWrapper(p) for p in candidates[:30]]
            self.proxy_updated_at = time.time()
            logger.debug('Succeed to update proxy', self.proxy_updated_at)
            return True
        return False

    def get_html(self, url, timeout=20):
        proxy = random.sample(
            [p for p in self.proxy_list if p.is_available()],
            1)[0]

        start_at = time.time()
        html = get_html(url, proxy=proxy.proxy, timeout=timeout)
        elapsed_sec = time.time() - start_at
        if html is None:
            proxy.log_fail(elapsed_sec)
            return None
        else:
            proxy.log_success(elapsed_sec)
        return html


class ProxyWrapper:
    def __init__(self, proxy):
        self.proxy = proxy
        self.successes = []
        self.fails = []

    def __str__(self):
        return self.proxy.__str__()

    def log_success(self, elapsed_sec):
        self.successes.append(elapsed_sec)

    def log_fail(self, elapsed_sec):
        self.fails.append(elapsed_sec)

    def is_available(self, meantime=10):
        for _ in range(1):
            if len(self.fails) <= 1:
                break

            if len(self.successes) >= len(self.fails):
                break

            return False

        if len(self.successes) >= 2 and \
            reduce(
                lambda x, y: x + y, self.successes) / \
                len(self.successes) >= meantime:
            return False

        return True

    def dump(self):
        success_num = len(self.successes)
        fail_num = len(self.fails)
        if success_num > 0:
            success_ave = reduce(
                lambda x, y: x + y, self.successes) / success_num
        else:
            success_ave = -1

        return '%s: success_rate = %d / %d, meantime = %d' % (
            self.proxy.__str__(),
            success_num,
            success_num + fail_num,
            success_ave)
