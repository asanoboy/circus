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

    def calc_data_path(self, page):
        page_id = page.get_id()
        dirname = hashlib.md5(page_id.encode()).hexdigest()[:2]
        data_path = os.path.join(self.data_dir, dirname, page_id)
        return data_path


class Page:
    @classmethod
    def create_by_url(cls, url, amz):
        page = cls(amz)
        page.set_url(url)
        return page

    @classmethod
    def create_by_html(cls, page_id, content, amz):
        page = cls(amz)
        if not page.set_content(content):
            return None
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


class ArtistPage(Page):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)

    def set_content_by_db(self, db):
        rs = db.select_one('''
            select page_type, name, worksnum, links
            from page where page_id = %s
            ''', args=(self.id,))
        if not rs:
            return False
        similar_pages = [
            ArtistPage.create_by_url(
                self.amz.url_template % (
                    hashlib.md5(page_id.encode()).hexdigest()[:8],
                    page_id[0],
                    page_id[2:]),
                self.amz)
            for page_id in rs['links'].split(',')] \
            if len(rs['links']) > 0 else []
        self.set_attrs(
            rs['name'],
            rs['worksnum'],
            similar_pages,
            rs['page_type'] == 1)
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
        return True

    def is_valid(self):
        return self.is_musician()
