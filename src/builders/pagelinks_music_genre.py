from itertools import chain
from model.master import Tag


class Builder:
    def __init__(self, session, wiki_db):
        self.session = session
        self.wiki_db = wiki_db

    def _get_pages_from_tag_id(self, tag_id):
        tag = self.session.query(Tag).filter(Tag.id == tag_id).first()
        if not tag:
            raise Exception('Genre tag does not exist.')

        return [p for p in chain.from_iterable(
            [list(item.pages) for item in tag.items])
            if p.lang == self.wiki_db.lang]

    def get_genre_pages(self):
        return self._get_pages_from_tag_id(2)

    def get_musician_pages(self):
        return self._get_pages_from_tag_id(1)

    def build(self):
        genres = self.get_genre_pages()
        musicians = self.get_musician_pages()

        genre_page_id_joined = ','.join(
            [str(p.page_id) for p in genres])
        musician_page_id_joined = ','.join(
            [str(p.page_id) for p in musicians])
        link_records = self.wiki_db.selectAndFetchAll('''
            select id_from, id_to from an_pagelinks
            where id_from in (%s) and id_to in (%s)
            ''' % (genre_page_id_joined, musician_page_id_joined))
        self.wiki_db.multiInsert(
            'an_pagelinks_music_genre',
            ['id_from', 'id_to', 'odr'],
            [[r['id_from'], r['id_to'], 0] for r in link_records])

        self.wiki_db.commit()
