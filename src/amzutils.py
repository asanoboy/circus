import re
import hashlib
import os
from urllib.parse import urlparse
from bs4 import BeautifulSoup as Soup


MUSICIAN_TYPE = 1
MOVIE_TYPE = 2


class AmazonHandler:
    def __init__(self, lang, page_type=1):
        if lang == 'ja':
            self.data_dir = '/mnt/hdd500/amazon/jp/musician'
            self.title_pattern = re.compile('Amazon.co.jp: ([^:]+):')
            self.worksnum_pattern = re.compile('\\(([0-9]+)')
            self.url_template = 'http://www.amazon.co.jp/%s/%s/%s/'
            self.init_url = \
                'http://www.amazon.co.jp/The-Chemical-Brothers/e/B000AQ22AU/'

            self.review_pattern = re.compile('([0-9]+)')
        elif lang == 'us':
            self.data_dir = '/mnt/hdd500/amazon/us/musician'
            self.title_pattern = re.compile('Amazon.com: ([^:]+):')
            self.worksnum_pattern = re.compile('\\(See all ([0-9]+)')
            self.url_template = 'http://www.amazon.com/%s/%s/%s/'
            self.init_url = \
                'http://www.amazon.com/The-Chemical-Brothers/e/B000AQ22AU/'
        else:
            raise 'Invalid lang: %s' % (lang,)

        self.link_selector = '#entitySimsTable a'
        self.work_selector = 'div.customFaceoutImage'
        self.works_num_selector = '#twAlbumCountHeader a'
        self.top_content_link_selector = \
            '#wwFaceoutsContainer > table div.faceoutTitle a'

    def calc_data_path(self, page):
        code = page.get_code()
        return self.calc_data_path_by_page_id(code)

    def calc_data_path_by_page_id(self, code):
        dirname = hashlib.md5(code.encode()).hexdigest()[:2]
        data_path = os.path.join(self.data_dir, dirname, code)
        return data_path


class Page:
    @classmethod
    def create_by_code(cls, code, amz, html=None):
        page_id_split = code.split('_')
        dummy_url = amz.url_template % (
            hashlib.md5(code.encode()).hexdigest()[:8],
            page_id_split[0],
            page_id_split[1])

        page = cls(amz)
        page.set_url(dummy_url)
        if html:
            page.set_content(html)
        return page

    @classmethod
    def create_by_url(cls, url, amz, html=None):
        page = cls(amz)
        page.set_url(url)
        if html:
            page.set_content(html)
        return page

    @classmethod
    def create_by_html(cls, code, content, amz):
        page = cls(amz)
        if not page.set_content(content):
            return None
        page.code = code
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
        path = path.split('=')[0]
        self.code = '_'.join(path.split('/')[-3:-1])

    def get_code(self):
        return self.code

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

    def title(self):
        return self._title


class ContentPage(Page):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)

        self.review_num = None
        self.breadcrumb = None
        self.name = None
        self.parent_page_id = None

    def set_content_by_db(self, db):
        self._has_attrs = True

    def set_content(self, html):
        soup = Soup(html, 'html.parser')
        els = soup.select('#wayfinding-breadcrumbs_feature_div a')
        self.breadcrumb = ','.join([el.text.strip() for el in els])
        del els

        el = soup.select_one('#acrCustomerReviewText')
        if el:
            mat = self.amz.review_pattern.match(el.text)
            self.review_num = int(mat.group(1))
        else:
            self.review_num = 0
        del el

        el = soup.select_one('#productTitle')
        if el:
            self.name = el.text
        else:
            return False

        self._has_attrs = True
        return True

    def set_parent_page_id(self, id):
        self.parent_page_id = id


class ArtistPage(Page):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.top_contents = None

    def set_content_by_db(self, db):
        rs = db.select_one('''
            select page_type, name, worksnum, links, top_contents
            from page where code = %s
            ''', args=(self.code,))
        if not rs:
            return False
        similar_pages = [
            ArtistPage.create_by_code(code, self.amz)
            for code in rs['links'].split(',')] \
            if len(rs['links']) > 0 else []
        self.set_attrs(
            rs['name'],
            rs['worksnum'],
            similar_pages,
            rs['page_type'] == 1)

        if rs['top_contents']:
            self.top_contents = [
                ContentPage.create_by_code(code, self.amz)
                for code in rs['top_contents'].split(',')]
        else:
            self.top_contents = []
        return True

    def set_content(self, content):
        if not content:
            return False
        self.content = content
        soup = Soup(content, 'html.parser')
        is_musician = len(soup.select('div.MusicCartBar')) > 0
        link_elems = soup.select(self.amz.link_selector)
        link_urls = [elem.get('href').strip() for elem in link_elems]
        similar_pages = [
            ArtistPage.create_by_url(url, self.amz) for url in link_urls]

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

        link_elems = soup.select(self.amz.top_content_link_selector)
        link_urls = [
            elem.get('href').strip()
            for elem in link_elems
            if elem.get('href')]
        self.top_contents = [
            ContentPage.create_by_url(url, self.amz)
            for url in link_urls]
        return True

    def get_top_products(self):
        return self.top_contents

    def is_valid(self):
        return self.is_musician()
