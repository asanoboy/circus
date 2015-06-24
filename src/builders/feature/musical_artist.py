import json
from ..builder_utils import wkdb_session, find_links_from_wiki, \
    page_name_to_dict
from sqlalchemy import Table, Column, Integer, ForeignKey, String
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()

page_feature_page_table = Table(
    'page_feature_page', Base.metadata,
    Column('left_id', Integer, ForeignKey('page.id')),
    Column('right_id', Integer, ForeignKey('feature_page.id'))
    )

page_feature_table = Table(
    'page_feature', Base.metadata,
    Column('left_id', Integer, ForeignKey('page.id')),
    Column('right_id', Integer, ForeignKey('feature.id'))
    )


class Page(Base):
    __tablename__ = 'page'

    id = Column(Integer, primary_key=True)  # an_page.page_id
    infobox = Column(String, nullable=True)
    feature_pages = relationship(
        'FeaturePage',
        secondary=page_feature_page_table,
        backref='pages')

    features = relationship(
        'Feature',
        secondary=page_feature_table,
        backref='pages')


class FeaturePage(Base):
    __tablename__ = 'feature_page'

    id = Column(Integer, primary_key=True)  # an_page.page_id


class Feature(Base):
    __tablename__ = 'feature'

    id = Column(Integer, primary_key=True)  # is not feature_id
    feature_page_id = Column(Integer, ForeignKey('feature_page.id'))
    feature_page = relationship(
        'FeaturePage',
        backref=backref('feature_page', uselist=False))
    year = Column(Integer, nullable=True)
    popularity = Column(Integer, nullable=True)


class Loader:
    def __init__(self, master_db, lang_db):
        self.master_db = master_db
        self.lang_db = lang_db

        lang = lang_db.lang
        if lang == 'en':
            self.infotype = 'Infobox_musical_artist'
            self.target_infotypes = ['Infobox_music_genre', None]
            self.key = 'genre'
        elif lang == 'ja':
            self.infotype = 'Infobox_Musician'
            self.target_infotypes = ['Infobox_Music_genre', None]
            self.key = 'ジャンル'
        else:
            raise Exception('lang = %s is not supported.' % (lang,))

        self._name_to_page = {}  # cache

    def load(self):
        with wkdb_session(Base) as session:
            id_to_feature_page = {}
            for page_record in self.generate_featured_pages():
                page = Page(
                    id=page_record['page_id'],
                    infobox=page_record['infocontent'])
                feature_pages = []
                for page_id in self.find_feature_from_page(page):
                    if page_id not in id_to_feature_page:
                        id_to_feature_page[page_id] = FeaturePage(id=page_id)
                    feature_pages.append(id_to_feature_page[page_id])
                page.feature_pages = feature_pages

            session.add(page)
            session.flush()

            for feature_page in session.query(FeaturePage).all():
                print('len', len(feature_page.pages))
                if len(feature_page.pages) > 100:
                    pass
                else:
                    pass
            pass

    def generate_featured_pages(self):
        return self.lang_db.generate_records(
            'an_page', cols=['page_id', 'infocontent'],
            cond='infotype = %s',
            args=(self.infotype,))

    def find_feature_from_page(self, page):
        wiki_object = json.loads(page.infobox)
        if self.key in wiki_object:
            wiki_text = wiki_object[self.key]
        elif self.key.capitalize() in wiki_object:
            wiki_text = wiki_object[self.key.capitalize()]
        else:
            return []

        names = find_links_from_wiki(wiki_text)
        pages = [self._page_name_to_dict(name) for name in names]
        pages = [p for p in pages if p is not None]
        pages = filter(
            lambda x: x['infotype'] in self.target_infotypes,
            pages)
        return list(set(r['page_id'] for r in pages))

    def _page_name_to_dict(self, name):
        if name in self._name_to_page:
            return self._name_to_page[name]

        page = page_name_to_dict(self.lang_db, name)
        if page:
            self._name_to_page[name] = page
        return page

    def result(self):
        pass


def load(master_db, lang_db):
    loader = Loader(master_db, lang_db)
    loader.load()
    return loader.result()
