import os
import argparse
from envutils import get_amz_db, init_logger
from debug import get_logger
# from numerical import levenshtein
from get_musician_graph_amzjp import AmazonHandler, Page


def generate_musician_pages(amz):
    for dirpath, _, filenames in os.walk(amz.data_dir):
        for filename in filenames:
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

    for page_id, page in generate_musician_pages(amz):
        print(
            page_id,
            page.get_works_num(),
            page.title()
            )
