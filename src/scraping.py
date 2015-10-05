from debug import get_logger, Timer
from proxyutils import ProxyManager
import time
from selenium import webdriver
import random


class AutoEncoder:
    lookup = (
        'utf_8', 'euc_jp', 'euc_jis_2004', 'euc_jisx0213',
        'shift_jis', 'shift_jis_2004', 'shift_jisx0213',
        'iso2022jp', 'iso2022_jp_1', 'iso2022_jp_2', 'iso2022_jp_3',
        'iso2022_jp_ext', 'latin_1', 'ascii')

    def __init__(self):
        self.last_encoding = AutoEncoder.lookup[0]

    def __call__(self, binary):
        try:
            return binary.encode(self.last_encoding)
        except:
            pass

        for encoding in AutoEncoder.lookup:
            if self.last_encoding == encoding:
                continue
            try:
                string = binary.encode(encoding)
                self.last_encoding = encoding
                return string
            except:
                pass

        raise UnicodeEncodeError()


class ProxyDriverPool:
    def __init__(self, num, use_proxy=True):
        self._drivers = []
        self.encoder = AutoEncoder()
        self.proxy_manager = ProxyManager(num=num) if use_proxy else None

    def is_empty(self):
        return len(self._drivers) == 0

    def update(self):
        self.clear_all()

        if self.proxy_manager:
            self.proxy_manager.update(True)

            for proxy_wrapper in self.proxy_manager.proxy_list:
                d = webdriver.PhantomJS(
                    service_args=[
                        '--proxy=%s:%s' % (
                            proxy_wrapper.proxy.host,
                            proxy_wrapper.proxy.port)])

                self._drivers.append(d)
        else:
            d = webdriver.PhantomJS()
            self._drivers.append(d)

    def clear_all(self):
        for d in self._drivers:
            d.quit()
        self._drivers = []

    def clear_one(self, d):
        d.quit()
        self._drivers.remove(d)

    def get_html(self, url):
        d = random.sample(self._drivers, 1)[0]
        d.get(url)
        # html = self.encoder(d.page_source)
        return d.page_source, d


class ScrapeTarget:
    def next(self):
        '''
        Returns url string.
        '''
        pass

    def add_html(self, url, html):
        '''
        Callback on getting html.
        '''
        pass


class Scraper:
    def __init__(self, target, validate_html=None):
        self.target = target
        if validate_html:
            self.validate_html = validate_html
        else:
            self.validate_html = lambda x: '</title>' in x

    def run(self):
        logger = get_logger(__name__)
        pool = ProxyDriverPool(num=5, use_proxy=False)

        timer = Timer()
        sleep = 1
        for url in self.target.next():
            if pool.is_empty() or timer.elapsed() > 7200:
                pool.update()
                timer.reset()

            ignore = False
            html = None
            for i in range(1):
                try:
                    with logger.lap('get_html url=%s' % (url,)) as timer:
                        html, driver = pool.get_html(url)
                        if self.validate_html and not self.validate_html(html):
                            pool.clear_one(driver)
                            logger.debug('Load error: url = ', url)
                            continue
                    break
                except UnicodeDecodeError:
                    logger.debug('unicode error')
                    ignore = True
                    break

            if ignore or html is None:
                continue

            self.target.add_html(url, html)

            time.sleep(sleep)
