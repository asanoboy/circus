import os
import argparse
from envutils import get_amz_db, init_logger
from amzutils import AmazonHandler, ContentPage
from bs4 import BeautifulSoup as Soup


if __name__ == '__main__':
    init_logger()

    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--lang', required=True)  # ja,us
    args = parser.parse_args()
    args = vars(args)
    lang = args['lang']

    db = get_amz_db(lang)
    amz = AmazonHandler(lang)

    for r in db.select_all('''
            select id, code
            from page
            /*where top_contents is null*/
            '''):
        data_path = amz.calc_data_path_by_page_id(r['code'])
        if not os.path.exists(data_path):
            raise Exception('Data does not exist: %s' % (data_path,))

        with open(data_path, 'r') as f:
            html = f.read()

        soup = Soup(html, 'html.parser')
        link_elems = soup.select(amz.top_content_link_selector)
        try:
            link_urls = [elem.get('href').strip() for elem in link_elems]
        except:
            continue

        content_pages = [
            ContentPage.create_by_url(url, amz) for url in link_urls]
        content_codes = [p.get_code() for p in content_pages]

        db.update_query('''
            update page
            set top_contents = %s
            where id = %s
        ''', args=(
            ','.join(content_codes),
            r['id']))
        db.commit()
