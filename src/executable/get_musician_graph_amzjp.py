from envutils import init_logger
from bs4 import BeautifulSoup as Soup
from http_utils import get_html, get_proxy_list
from debug import get_logger
from urllib.parse import urlparse
import os
import time
import random

data_dir = '/mnt/hdd500/amazon/jp/musician'
init_url = 'http://www.amazon.co.jp/%E3%82%B1%E3%83%9F%E3%82%AB%E3%83%AB%E3%83%BB%E3%83%96%E3%83%A9%E3%82%B6%E3%83%BC%E3%82%BA/e/B000AQ22AU/'
link_selector = '#entitySimsTable a'


class Page:
    def __init__(self, url):
        self.url = url
        self.url_obj = urlparse(url)

    def get_id(self):
        path = self.url_obj.path
        return '_'.join(path.split('/')[2:4])

    def set_content(self, content):
        self.content = content


def calc_data_path(page):
    page_id = page.get_id()
    data_path = os.path.join(data_dir, page_id[:4], page_id[4:])
    return data_path


def load_content(page, proxy, sleep):
    data_path = calc_data_path(page)
    if os.path.exists(data_path):
        with open(data_path, 'r') as f:
            content = f.read()
        if content:
            page.set_content(content)
            return True

    html = get_html(page.url, proxy)
    if html is None:
        return None
    page.set_content(html)

    dir_path = os.path.dirname(data_path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    with open(data_path, 'w') as f:
        f.write(html)
    return True


proxy_list = []
proxy_updated_at = 0


def update_proxy():
    logger = get_logger(__name__)
    global proxy_list
    global proxy_updated_at
    trial = 5
    while trial > 0:
        candidates = get_proxy_list()
        if candidates is None:
            time.sleep(5)
            trial -= 1
        proxy_list = candidates[:50]
        proxy_updated_at = time.time()
        logger.debug('Succeed to update proxy', proxy_updated_at)
        return True
    return False


if __name__ == '__main__':
    init_logger()
    logger = get_logger(__name__)
    page_stack = [Page(init_url)]
    page_ids = [p.get_id() for p in page_stack]

    while len(page_stack) > 0:
        if time.time() - proxy_updated_at > 1000:
            if not update_proxy():
                raise 'Can\'t update proxy'
        logger.debug('stack_length = ', len(page_stack))
        page = page_stack.pop()

        proxy = random.sample(proxy_list, 1)[0]
        if not load_content(page, proxy, 10):
            logger.debug('Load error: url = ', page.url)
            continue
        sp = Soup(page.content, 'html.parser')
        link_elems = sp.select(link_selector)
        link_urls = [elem.get('href').strip() for elem in link_elems]
        for link_page in [Page(url) for url in link_urls]:
            page_id = link_page.get_id()
            if page_id in page_ids:
                continue
            page_stack.append(link_page)
            page_ids.append(page_id)
