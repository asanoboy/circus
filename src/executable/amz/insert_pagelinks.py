import argparse
from envutils import get_amz_db, init_logger
from debug import get_logger
from circus_itertools import lazy_chunked as chunked


def generate_pagelinks_record(db):
    for r in db.generate_records_from_sql(
            'select id, page_id, links from page'):
        link_ids = r['links'].split(',')
        for odr, page_id in enumerate(link_ids):
            record = db.select_one(
                'select id from page where page_id = %s',
                args=(page_id,))
            if not record:
                continue
            yield [r['id'], record['id'], odr]


if __name__ == '__main__':
    init_logger()
    logger = get_logger(__name__)

    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--lang', required=True)  # ja,us
    args = parser.parse_args()
    args = vars(args)
    lang = args['lang']

    db = get_amz_db(lang)
    for records in chunked(generate_pagelinks_record(db), 1000):
        db.multi_insert(
            'pagelinks',
            ['id_from', 'id_to', 'odr'],
            records)
    db.commit()
