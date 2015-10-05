from multiprocessing import Pool
from debug import get_logger
import argparse
import os
from get_albums import get_album_pages
from envutils import get_amz_db, init_logger
from amzutils import AmazonHandler
from circus_itertools import lazy_chunked as chunked


def get_pages_with_content(db, amz, proc_index, proc_num):
    logger = get_logger(__name__)

    for i, page in enumerate(
            get_album_pages(db, amz, mod_cond=(proc_num, proc_index))):

        data_path = amz.calc_data_path(page)
        if not os.path.exists(data_path):
            raise Exception('%s does not exist' % (data_path,))

        with open(data_path, 'r') as f:
            html = f.read()

        if page.set_content(html):
            yield page
        else:
            logger.debug('code = %s is invalid page' % (page.get_code(),))


def import_albums(args):
    lang, proc_index, proc_num = args
    init_logger()
    logger = get_logger(__name__)

    db = get_amz_db(lang)
    amz = AmazonHandler(lang)
    for pages in chunked(
            get_pages_with_content(db, amz, proc_index, proc_num), 1000):
        db.multi_insert(
            'album',
            ['code', 'name', 'page_id', 'breadcrumb', 'review_num'],
            [[
                p.get_code(),
                p.name,
                p.parent_page_id,
                p.breadcrumb,
                p.review_num,
            ] for p in pages])
        db.commit()

        logger.debug('Insert @proc %s' % (proc_index,))


if __name__ == '__main__':
    init_logger()
    logger = get_logger(__name__)

    if 1:
        parser = argparse.ArgumentParser()
        parser.add_argument('-l', '--lang', required=True)  # ja,us
        args = parser.parse_args()
        args = vars(args)
        lang = args['lang']
    else:
        lang = 'ja'

    proc_num = 4
    args = [(lang, i, proc_num) for i in range(proc_num)]
    with Pool(processes=proc_num) as pool:
        results = pool.map(import_albums, args)

    logger.debug('Finished')
