import os
import argparse
from envutils import get_amz_db, init_logger
from debug import get_logger
# from numerical import levenshtein
from get_musician_graph_amzjp import AmazonHandler, Page
from circus_itertools import lazy_chunked as chunked


def generate_musician_pages(amz, exclude_page_ids=[]):
    for dirpath, _, filenames in os.walk(amz.data_dir):
        for filename in filenames:
            if filename in exclude_page_ids:
                continue
            filepath = os.path.join(dirpath, filename)
            with open(filepath) as f:
                content = f.read()
            page = Page.create_by_html(filename, content, amz)
            if not page.is_musician():
                continue
            yield filename, page


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

    exist_page_ids = [
        r['page_id'] for r in db.select_all('select page_id from page')]

    insert_record_iter = ([
        1,
        page_id, page.title(),
        page.get_works_num(),
        ','.join([p.get_id() for p in page.create_similar_pages()])
        ]
        for page_id, page in
        generate_musician_pages(amz, exist_page_ids))

    for records in chunked(insert_record_iter, 1000):
        db.multi_insert(
            'page',
            ['page_type', 'page_id', 'name', 'worksnum', 'links'],
            records)
    db.commit()
