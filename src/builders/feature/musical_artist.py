import json
import re
from model.master import Base, Page, Feature, Item
from ..builder_utils import find_links_from_wiki, PageNameResolver, IdMap
from sqlalchemy import Table, Column, Integer, ForeignKey
from sqlalchemy.orm import relationship


# musician_to_genre_year_table = Table(
#     'musician_to_genre_year', Base.metadata,
#     Column('left_id', Integer, ForeignKey('musician.id')),
#     Column('right_id', Integer, ForeignKey('genre_year.id'))
#     )

album_to_genre_table = Table(
    'wk_album_to_genre', Base.metadata,
    Column('left_id', Integer, ForeignKey('wk_album.id')),
    Column('right_id', Integer, ForeignKey('item.id'))
    )


# class Musician(Base):
#     __tablename__ = 'wk_musician'
# 
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     page_id = Column(Integer, ForeignKey('page.id'))
#     page = relationship('Page', uselist=False)
#     genre_years = relationship(
#         'GenreYear',
#         secondary=musician_to_genre_year_table)


# class Genre(Base):
#     __tablename__ = 'wk_genre'
# 
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     page_id = Column(Integer, ForeignKey('page.id'))
#     page = relationship('Page', uselist=False)


# class GenreYear(Base):
#     __tablename__ = 'wk_genre_year'
#     __table_args__ = (
#         (UniqueConstraint(
#             'year', 'genre_id', name='unique_idx_year_genre_id')),
#         )
# 
#     id = Column(Integer, primary_key=True, autoincrement=True)  # not use
#     year = Column(Integer)
#     genre_id = Column(Integer, ForeignKey('wk_genre.id'))
#     genre = relationship(
#         'Genre', uselist=False)


GenreYear = Feature
Musician = Item
Genre = Item


class Album(Base):
    __tablename__ = 'wk_album'

    id = Column(Integer, primary_key=True, autoincrement=True)

    page_id = Column(Integer, ForeignKey('page.id'))
    page = relationship('Page', uselist=False)

    year = Column(Integer)
    genres = relationship(
        'Item',
        secondary=album_to_genre_table)

    musician_id = Column(Integer, ForeignKey('item.id'))
    musician = relationship(
        'Item', uselist=False)


def find_wiki_text(content, key):
    wiki_dict = json.loads(content)
    if key in wiki_dict:
        wiki_text = wiki_dict[key]
    elif key.capitalize() in wiki_dict:
        wiki_text = wiki_dict[key.capitalize()]
    else:
        return None
    return wiki_text


def find_links_from_infocontent(content, key):
    wiki_text = find_wiki_text(content, key)
    if wiki_text is None:
        return []
    return find_links_from_wiki(wiki_text)


class Loader:
    def __init__(self, lang_db, get_or_create_item_by_page_id):
        self.lang_db = lang_db
        self.get_or_create_item_by_page_id = get_or_create_item_by_page_id
        self.page_name_resolver = PageNameResolver(lang_db)

        self.musician_map = IdMap(Musician)
        self.genre_year_map = IdMap(GenreYear)
        self.albums = []

        lang = lang_db.lang
        if lang == 'en':
            self.infotype = 'Infobox_musical_artist'
            self.target_infotypes = ['Infobox_music_genre', None]
            self.genre_infotype = 'Infobox_music_genre'
            self.album_infotype = 'Infobox_album'
            self.album_infobox_genre = 'genre'
            self.album_infobox_artist = 'artist'
            self.album_infobox_released = 'released'
            self.key = 'genre'
        elif lang == 'ja':
            self.infotype = 'Infobox_Musician'
            self.target_infotypes = ['Infobox_Music_genre', None]
            self.genre_infotype = 'Infobox_Music_genre'
            self.album_infotype = 'Infobox_Album'
            self.album_infobox_genre = 'genre'
            self.album_infobox_artist = 'artist'
            self.album_infobox_released = 'released'
            self.key = 'ジャンル'
        else:
            raise Exception('lang = %s is not supported.' % (lang,))

    def get_or_create_musician(self, page_id):
        if self.musician_map.has(page_id):
            return self.musician_map.get_or_create(page_id)

        item = self.get_or_create_item_by_page_id(page_id)
        self.musician_map.set(page_id, item)
        return item

    def get_or_create_genre(self, page_id):
        # if page_id == 47774:
        #     genre = self.get_or_create_item_by_page_id(page_id)
        #     print(genre, genre.pages[0].id, genre.pages[0].name, genre.pages[0].lang)
        #     raise('aaa')
        #     return genre
        return self.get_or_create_item_by_page_id(page_id)

    def get_or_create_genre_year(self, genre, year):
        key = (genre, year)
        return self.genre_year_map.get_or_create(
            key, ref_item=genre, year=year)

    def load(self):
        # self.load_musicians()
        self.load_album()
        self.load_genre_year()
        # self.delete_album()

    def load_album(self):
        year_prog = re.compile('[\d]{4}')

        for r in self.lang_db.generate_records(
                'an_page',
                cols=['page_id', 'infocontent', 'name'],
                cond='infotype = %s',
                args=(self.album_infotype,)):
            # album = Album(page_id=r['page_id'])
            album = Album(page=Page(id=r['page_id']))

            artists = self.lang_db.selectAndFetchAll('''
                select i.page_id_to from an_infobox i
                where i.page_id = %s and i.key_lower = %s
                ''', args=(r['page_id'], self.album_infobox_artist))

            if len(artists) != 1:
                print(
                    'Musician is invalid "%s, %s"' %
                    (r['name'], r['page_id']))
                continue

            musician_record = self.lang_db.selectOne('''
                select infotype from an_page
                where page_id=%s
                ''', args=(artists[0]['page_id_to'],))

            '''
                In case of
                    lang='en' and page_id=15613,
                    lang='ja' and page_id=2260174,
                these records have an irregular artist.
            '''
            if musician_record['infotype'] in [None, self.genre_infotype]:
                print(
                    'Invalid musician: ', musician_record)
                continue

            musician = self.get_or_create_musician(artists[0]['page_id_to'])

            genre_records = self.lang_db.selectAndFetchAll('''
                select i.page_id_to page_id from an_infobox i
                inner join an_page p on p.page_id = i.page_id_to
                where i.page_id = %s and i.key_lower = %s
                    and ( p.infotype IS NULL or p.infotype = %s)
                ''', args=(
                    r['page_id'],
                    self.album_infobox_genre,
                    self.genre_infotype))

            genres = []
            for page_id in [r['page_id'] for r in genre_records]:
                genres.append(self.get_or_create_genre(page_id))
            album.genres = genres

            released_text = find_wiki_text(
                r['infocontent'], self.album_infobox_released)
            if released_text is None:
                print('Invalid released "%s"' % (r['name'],))
                continue
            match = year_prog.search(released_text)
            if not match:
                print(
                    'Invalid released "%s" : %s' %
                    (r['name'], released_text))
                continue
            year = int(match.group(0))
            if year < 1900 or 2999 < year:
                print(
                    'Invalid year "%s" : %s' %
                    (r['name'], released_text))
                continue
            album.year = year

            album.musician = musician
            self.albums.append(album)

    def load_genre_year(self):
        for album in self.albums:
            features = []
            for genre in album.genres:
                genre_year = self.get_or_create_genre_year(
                    genre, album.year)
                features.append(genre_year)
            exists_keys = [(f.ref_item.id, f.year) for f in album.musician.features]
            features = list(set(features))
            album.musician.features.extend(
                [f for f in features if (f.ref_item.id, f.year) not in exists_keys])

    def load_musicians(self):
        for r in self.lang_db.generate_records(
                'an_infobox',
                cols=['page_id', 'page_id_to'],
                cond='infotype=%s and key_lower=%s',
                args=(self.infotype, self.key)):

            self.get_or_create_musician(r['page_id'])

    def delete_album(self):
        for album in self.albums:
            album.genres = []
            album.musician = None
            album.page = None
            del album

    def result(self):
        return self.musician_map.values()
        # items = []
        # feature_map = IdMap(Feature)
        # for musician in self.musician_map.values():
        #     item = Item(page=musician.page)
        #     for genre_year in musician.genre_years:
        #         feature = feature_map.get_or_create(
        #             genre_year.id,
        #             ref_item=Item(page=genre_year.genre.page))
        #         item.features.append(feature)
        #     items.append(item)
        # return items


def load(lang_db, get_or_create_item_by_page_id):
    loader = Loader(lang_db, get_or_create_item_by_page_id)
    loader.load()
    return loader.result()
