from envutils import get_amz_db, init_logger
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
            self.url_template = 'http://www.amazon.co.jp/%s/%s/%s/'
        elif lang == 'us':
            self.data_dir = '/mnt/hdd500/amazon/us/musician'
            self.title_pattern = re.compile('Amazon.com: ([^:]+):')
            self.worksnum_pattern = re.compile('\\(See all ([0-9]+)')
            self.url_template = 'http://www.amazon.com/%s/%s/%s/'
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
        self._has_attrs = False

    def set_url(self, url):
        self.url = url
        self.url_obj = urlparse(url)
        path = self.url_obj.path
        self.id = '_'.join(path.split('/')[2:4])

    def get_id(self):
        return self.id

    def title(self):
        return self._title

    def set_content_by_db(self, db):
        rs = db.select_one('''
            select page_type, name, worksnum, links
            from page where page_id = %s
            ''', args=(self.id,))
        if not rs:
            return False
        similar_pages = [
            Page.create_by_url(
                self.amz.url_template % (
                    hashlib.md5(page_id.encode()).hexdigest()[:8],
                    page_id[0],
                    page_id[2:]),
                self.amz)
            for page_id in rs['links'].split(',')]
        self.set_attrs(
            rs['name'],
            rs['worksnum'],
            similar_pages,
            rs['page_type'] == 1)
        return True

    def set_content(self, content):
        self.content = content
        soup = Soup(content, 'html.parser')
        is_musician = len(soup.select('div.MusicCartBar')) > 0
        link_elems = soup.select(self.amz.link_selector)
        link_urls = [elem.get('href').strip() for elem in link_elems]
        similar_pages = [
            Page.create_by_url(url, self.amz) for url in link_urls]

        elem = soup.select_one(self.amz.works_num_selector)
        if elem is None:
            works_num = len(soup.select(self.amz.work_selector))
        else:
            mat = self.amz.worksnum_pattern.match(elem.text)
            if not mat:
                return False
            works_num = mat.group(1)

        elem = soup.select_one('title')
        if not elem:
            return False
        mat = self.amz.title_pattern.match(elem.text)
        title = mat.group(1)
        self.set_attrs(title, works_num, similar_pages, is_musician)
        return True

    def set_attrs(self, title, works_num, similar_pages, is_musician):
        self._title = title
        self._works_num = works_num
        self._similar_pages = similar_pages
        self._is_musician = is_musician
        self._has_attrs = True

    def has_attrs(self):
        return self._has_attrs

    def is_musician(self):
        return self._is_musician

    def create_similar_pages(self):
        return self._similar_pages

    def get_works_num(self):
        return self._works_num


def load_content(page, db, amz, proxy, sleep):
    logger = get_logger(__name__)
    data_path = amz.calc_data_path(page)

    if page.set_content_by_db(db):
        return True

    if os.path.exists(data_path):
        with open(data_path, 'r') as f:
            content = f.read()
        if page.set_content(content):
            return True
        else:
            os.remove(data_path)
            logger.debug('Removed invalid file: ', data_path)

    start_at = time.time()
    with logger.lap('get_html proxy=%s' % (proxy,)):
        html = get_html(page.url, proxy=proxy.proxy, timeout=20)
    elapsed_sec = time.time() - start_at

    if html is None:
        proxy.log_fail(elapsed_sec)
        return None
    else:
        proxy.log_success(elapsed_sec)

    if not page.set_content(html):
        logger.log('Invalid page format')
        return False

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
    db = get_amz_db(lang)

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
            'page_id = ', page.get_id())

        ignore = False
        for i in range(10):
            proxy = random.sample(
                [p for p in proxy_list if p.is_available()],
                1)[0]

            try:
                success = False
                success = load_content(page, db, amz, proxy, 10)
            except UnicodeDecodeError:
                logger.debug('unicode error')
                ignore = True
                break

            if success:
                break
            logger.debug(
                'Load error: url = ', page.url, 'proxy = ', proxy)

        if ignore:
            continue

        if not page.has_attrs():
            raise 'Can\'t load: url = %s' % (page.url,)

        if not page.is_musician():
            continue
        for link_page in page.create_similar_pages():
            page_id = link_page.get_id()
            if page_id in page_ids:
                continue
            page_stack.append(link_page)
            page_ids.append(page_id)
