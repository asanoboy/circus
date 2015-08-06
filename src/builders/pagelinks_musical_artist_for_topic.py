from .pagelinks_music_genre import MusicHelper
from .builder_utils import find_link_from_wiki
from circus_itertools import lazy_chunked as chunked
from debug import get_logger


class Builder:
    def __init__(self, session, wiki_db):
        self.session = session
        self.wiki_db = wiki_db
        self.helper = MusicHelper(session, wiki_db)

    def build(self):
        logger = get_logger(__name__)
        with logger.lap('get_musician_pages'):
            musicians = self.helper.get_musician_pages()

        insert_records = []  # list of [$id_from, $id_to, $odr]
        for p in musicians:
            page_obj = self.wiki_db.createPageByTitle(p.name, with_info=False)
            for link_record in self.wiki_db.selectAndFetchAll('''
                    select id_from, id_to from an_pagelinks
                    where id_from = %s
                    ''', (p.page_id,)):
                page_id = link_record['id_to']
                page = self.wiki_db.selectOne('''
                    select
                        page_title name,
                        page_is_redirect is_redirect,
                        page_namespace namespace
                    from page where page_id = %s
                    ''', (page_id,))
                if not page:
                    if page['is_redirect'] == 0:
                        logger.debug(
                            'pageid=%s, id_to=%s is not found' %
                            (p.page_id, page_id))
                    continue

                if page['namespace'] != 0:
                    continue

                page_name = page['name']
                if not page_obj:
                    logger.debug(
                        'name=%s is not found' %
                        (page_name,))
                    continue

                odr = 0
                start_pos = 0
                content_length = len(page_obj.text)
                while 1:
                    pos = find_link_from_wiki(
                        page_name, page_obj.text, start_pos)
                    if pos == -1:
                        break
                    odr += (pos / content_length - 1) ** 2
                    start_pos = pos + 1

                if odr > 0:
                    insert_records.append([p.page_id, page_id, odr])

        for records in chunked(insert_records, 1000):
            self.wiki_db.multiInsert(
                'an_pagelinks_musical_artist_for_topic',
                ['id_from', 'id_to', 'odr'],
                records)
        self.wiki_db.commit()
