from envutils import get_amz_db, init_logger
import os
import argparse
from amzutils import AmazonHandler, ContentPage, ArtistPage
from scraping import Scraper, ScrapeTarget


def get_album_pages(db, amz, min_linked_num=1, mod_cond=None):
    for r in db.select_all('''
            select p.id, p.code
            from pagelinks pl
            inner join page p on p.id = pl.id_to
            group by pl.id_to
            having count(*) >= %s
            order by p.id asc
            ''', args=(min_linked_num,)):

        if mod_cond and r['id'] % mod_cond[0] != mod_cond[1]:
            continue

        page = ArtistPage.create_by_code(r['code'], amz)
        data_path = amz.calc_data_path(page)

        if not os.path.exists(data_path):
            raise Exception(
                'Html does not exist. %s' % (data_path,))

        with open(data_path, 'r') as f:
            html = f.read()

        if not page.set_content(html):
            raise Exception(
                'Fail to load artist page: %s' % (r['code'],))

        for content_page in page.get_top_products():
            content_page.set_parent_page_id(r['id'])
            yield content_page


class AlbumScrapeTarget(ScrapeTarget):
    def __init__(self, lang):
        self.amz = AmazonHandler(lang)
        self.db = get_amz_db(lang)

    def next(self):
        for content_page in get_album_pages(self.db, self.amz):
            content_data_path = self.amz.calc_data_path(content_page)
            if not os.path.exists(content_data_path):
                print(content_page.url)
                yield content_page.url

    def add_html(self, url, html):
        page = ContentPage.create_by_url(url, self.amz, html)

        data_path = self.amz.calc_data_path(page)
        dir_path = os.path.dirname(data_path)
        print('save to %s', data_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        with open(data_path, 'w') as f:
            f.write(html)

        if not page.has_attrs():
            raise Exception('Can\'t load: url = %s' % (page.url,))


if __name__ == '__main__':
    init_logger()

    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--lang', required=True)  # ja,us
    args = parser.parse_args()
    args = vars(args)
    lang = args['lang']

    scrape_target = AlbumScrapeTarget(lang)
    scraper = Scraper(scrape_target)
    scraper.run()
