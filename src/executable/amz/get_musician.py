from envutils import get_amz_db, init_logger
from debug import get_logger
import os
import time
import argparse
from amzutils import AmazonHandler, ArtistPage
from proxyutils import ProxyManager


def load_content(page, db, amz, proxy_manager, sleep):
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
    with logger.lap('get_html url=%s' % (page.url,)):
        html = proxy_manager.get_html(page.url, timeout=20)
    elapsed_sec = time.time() - start_at

    if not page.set_content(html):
        logger.debug('Invalid page format')
        return False

    dir_path = os.path.dirname(data_path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    with open(data_path, 'w') as f:
        f.write(html)

    if sleep - elapsed_sec > 0:
        time.sleep(sleep - elapsed_sec)
    return True


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
    proxy_manager = ProxyManager()

    page_stack = [ArtistPage.create_by_url(amz.init_url, amz)]
    page_ids = {p.get_id() for p in page_stack}

    while len(page_stack) > 0:
        proxy_manager.update()
        page = page_stack.pop()
        logger.debug(
            'stack = ', len(page_stack),
            'page_id = ', page.get_id())

        ignore = False
        for i in range(10):
            try:
                success = False
                success = load_content(page, db, amz, proxy_manager, 10)
            except UnicodeDecodeError:
                logger.debug('unicode error')
                ignore = True
                break

            if success:
                break

            logger.debug(
                'Load error: url = ', page.url)

        if ignore:
            continue

        if not page.has_attrs():
            raise 'Can\'t load: url = %s' % (page.url,)

        if not page.is_valid():
            continue
        for link_page in page.create_similar_pages():
            page_id = link_page.get_id()
            if page_id in page_ids:
                continue
            page_stack.append(link_page)
            page_ids.add(page_id)
