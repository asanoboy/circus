from envutils import get_amz_db, init_logger
from debug import get_logger
import os
import argparse
from amzutils import AmazonHandler, ArtistPage
from scraping import Scraper, ScrapeTarget


class ArtistScrapeTarget(ScrapeTarget):
    def __init__(self, lang):
        self.amz = AmazonHandler(lang)
        self.db = get_amz_db(lang)
        self.page_stack = [
            ArtistPage.create_by_url(self.amz.init_url, self.amz)]
        self.codes = {p.get_code() for p in self.page_stack}

    def next(self):
        logger = get_logger(__name__)
        while len(self.page_stack):
            page = self.page_stack.pop()
            logger.debug(
                'stack = ', len(self.page_stack),
                'code = ', page.get_code())
            if page.set_content_by_db(self.db):
                '''
                Assumes that the data already exists.
                '''
                self._add_page(page)
                continue

            data_path = self.amz.calc_data_path(page)

            if os.path.exists(data_path):
                with open(data_path, 'r') as f:
                    content = f.read()

                if page.set_content(content):
                    self._add_page(page)
                else:
                    os.remove(data_path)
                    logger.debug('Removed invalid file: ', data_path)
                continue

            yield page.url

    def add_html(self, url, html):
        page = ArtistPage.create_by_url(url, self.amz, html)

        data_path = self.amz.calc_data_path(page)
        dir_path = os.path.dirname(data_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        with open(data_path, 'w') as f:
            f.write(html)

        if not page.has_attrs():
            raise 'Can\'t load: url = %s' % (page.url,)

        self._add_page(page)

    def _add_page(self, page):
        if not page.is_valid():
            return

        for link_page in page.create_similar_pages():
            code = link_page.get_code()
            if code in self.codes:
                continue
            self.page_stack.append(link_page)
            self.codes.add(code)


if __name__ == '__main__':
    init_logger()

    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--lang', required=True)  # ja,us
    args = parser.parse_args()
    args = vars(args)
    lang = args['lang']

    scrape_target = ArtistScrapeTarget(lang)
    scraper = Scraper(scrape_target)
    scraper.run()
