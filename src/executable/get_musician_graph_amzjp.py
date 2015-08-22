from envutils import init_logger
from bs4 import BeautifulSoup as Soup
from http_utils import get_html, get_proxy_list
from debug import get_logger
from urllib.parse import urlparse
import re
import os
import time
import random
import hashlib
import argparse
from functools import reduce


class AmazonHandler:
    def __init__(self, lang):
        if lang == 'ja':
            self.data_dir = '/mnt/hdd500/amazon/jp/musician'
            self.title_pattern = re.compile('Amazon.co.jp: ([^:]+):')
            self.worksnum_pattern = re.compile('\\(([0-9]+)')
        elif lang == 'us':
            self.data_dir = '/mnt/hdd500/amazon/us/musician'
            self.title_pattern = re.compile('Amazon.com: ([^:]+):')
            self.worksnum_pattern = re.compile('\\(See all ([0-9]+)')
        else:
            raise 'Invalid lang: %s' % (lang,)

        self.link_selector = '#entitySimsTable a'
        self.work_selector = 'div.customFaceoutImage'
        self.works_num_selector = '#twAlbumCountHeader a'

    def calc_data_path(self, page):
        page_id = page.get_id()
        dirname = hashlib.md5(page_id.encode()).hexdigest()[:2]
        data_path = os.path.join(self.data_dir, dirname, page_id)
        return data_path


class Page:
    @staticmethod
    def create_by_url(url, amz):
        page = Page(amz)
        page.set_url(url)
        return page

    @staticmethod
    def create_by_html(page_id, content, amz):
        page = Page(amz)
        page.set_content(content)
        page.id = page_id
        return page

    def __init__(self, amz):
        self.url = None
        self.url_obj = None
        self.content = None
        self.amz = amz

    def set_url(self, url):
        self.url = url
        self.url_obj = urlparse(url)
        path = self.url_obj.path
        self.id = '_'.join(path.split('/')[2:4])

    def get_id(self):
        return self.id

    def title(self):
        dom_title = self.soup.select_one('title').text
        mat = self.amz.title_pattern.match(dom_title)
        if not mat:
            return False
        return mat.group(1)

    def set_content(self, content):
        self.content = content
        self.soup = Soup(content, 'html.parser')

    def is_musician(self):
        return len(self.soup.select('div.MusicCartBar')) > 0

    def create_similar_pages(self):
        link_elems = self.soup.select(self.amz.link_selector)
        link_urls = [elem.get('href').strip() for elem in link_elems]
        return [Page.create_by_url(url, self.amz) for url in link_urls]

    def get_works_num(self):
        elem = self.soup.select_one(self.amz.works_num_selector)
        if elem is None:
            return self.get_works_num_in_first_page()

        mat = self.amz.worksnum_pattern.match(elem.text)
        if not mat:
            return False
        return mat.group(1)

    def get_works_num_in_first_page(self):
        return len(self.soup.select(self.amz.work_selector))


def load_content(page, amz, proxy, sleep):
    logger = get_logger(__name__)
    data_path = amz.calc_data_path(page)
    if os.path.exists(data_path):
        with open(data_path, 'r') as f:
            content = f.read()
        if content:
            page.set_content(content)
            return True

    start_at = time.time()
    with logger.lap('get_html proxy=%s' % (proxy,)):
        html = get_html(page.url, proxy=proxy.proxy, timeout=20)
    elapsed_sec = time.time() - start_at

    if html is None:
        proxy.log_fail(elapsed_sec)
        return None
    else:
        proxy.log_success(elapsed_sec)

    page.set_content(html)

    dir_path = os.path.dirname(data_path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    with open(data_path, 'w') as f:
        f.write(html)

    if sleep - elapsed_sec > 0:
        time.sleep(sleep - elapsed_sec)
    return True


proxy_list = []
proxy_updated_at = 0


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


def update_proxy():
    logger = get_logger(__name__)
    global proxy_list
    global proxy_updated_at
    for _ in range(5):
        candidates = get_proxy_list()
        if candidates is None:
            time.sleep(5)
            continue
        for proxy in proxy_list:
            logger.debug(proxy.dump())
        proxy_list = [ProxyWrapper(p) for p in candidates[:30]]
        proxy_updated_at = time.time()
        logger.debug('Succeed to update proxy', proxy_updated_at)
        return True
    return False


if __name__ == '__main__':
    init_logger()
    logger = get_logger(__name__)

    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--lang', required=True)  # ja,us
    args = parser.parse_args()
    args = vars(args)
    lang = args['lang']

    amz = AmazonHandler(lang)

    if lang == 'ja':
        init_url = 'http://www.amazon.co.jp/%E3%82%B1%E3%83%9F%E3%82%AB%E3%83%AB%E3%83%BB%E3%83%96%E3%83%A9%E3%82%B6%E3%83%BC%E3%82%BA/e/B000AQ22AU/'
    elif lang == 'us':
        init_url = 'http://www.amazon.com/The-Chemical-Brothers/e/B000AQ22AU/'
    else:
        raise 'Invalid lang = %s' % (lang,)
    page_stack = [Page.create_by_url(init_url, amz)]
    page_ids = [p.get_id() for p in page_stack]

    while len(page_stack) > 0:
        if time.time() - proxy_updated_at > 3600:
            if not update_proxy():
                raise 'Can\'t update proxy'
        page = page_stack.pop()
        logger.debug(
            'stack = ', len(page_stack),
            'page_id = ', page_get_id())

        for i in range(5):
            proxy = random.sample(
                [p for p in proxy_list if p.is_available()],
                1)[0]
            if load_content(page, amz, proxy, 10):
                break
            logger.debug(
                'Load error: url = ', page.url, 'proxy = ', proxy)

        if page.content is None:
            logger.debug(
                'Can\'t load: url = ', page.url)
            continue

        if not page.is_musician():
            continue
        for link_page in page.create_similar_pages():
            page_id = link_page.get_id()
            if page_id in page_ids:
                continue
            page_stack.append(link_page)
            page_ids.append(page_id)
