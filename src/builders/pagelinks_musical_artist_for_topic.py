from .pagelinks_music_genre import MusicHelper
from .builder_utils import find_link_from_wiki
from circus_itertools import lazy_chunked as chunked


class Builder:
    def __init__(self, session, wiki_db):
        self.session = session
        self.wiki_db = wiki_db
        self.helper = MusicHelper(session, wiki_db)

    def build(self):
        musicians = self.helper.get_musician_pages()

        musician_page_id_joined = ','.join(
            [str(p.page_id) for p in musicians])
        link_records = self.wiki_db.selectAndFetchAll('''
            select id_from, id_to from an_pagelinks
            where id_from in (%s)
            ''' % (musician_page_id_joined,))

        insert_records = []  # list of [$id_from, $id_to, $odr]
        for p in musicians:
            for link_record in self.wiki_db.selectAndFetchAll('''
                    select id_from, id_to from an_pagelinks
                    where id_from = %s
                    ''', (p.page_id,)):
                page_id = link_record['id_to']
                page_name = self.wiki_db.selectOne('''
                    select name from an_page where page_id = %s
                    ''', (page_id,))['name']
                page_obj = self.wiki_db.createPageByTitle(page_name, with_info=false)

                odr = 0
                start_pos = 0
                content_length = len(page_obj.text)
                while 1:
                    pos = find_link_from_wiki(page_name, page_obj.text, start_pos)
                    if pos == -1:
                        break
                    odr += (content_length - pos) / content_length
                    start_pos = pos + 1

                if odr > 0:
                    insert_records.append([p.page_id, page_id, odr])

        for records in chunked(insert_records, 1000):
            self.wiki_db.multiInsert(
                'an_pagelinks_musical_artist_for_topic',
                ['id_from', 'id_to', 'odr'],
                records)
        self.wiki_db.commit()
