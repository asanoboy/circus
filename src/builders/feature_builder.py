import json
from .builder_utils import ItemPageRelationManager, Syncer


def find_links_from_wiki(wiki_text):
    links = []
    needle = 0
    while 1:
        pos_open = wiki_text.find('[[', needle)
        pos_close = wiki_text.find(']]', needle)
        if pos_open == -1 and pos_close == -1:
            break
        elif pos_open == -1 or pos_close == -1:
            print('Parse error1', wiki_text)
            break
        elif pos_open > pos_close:
            print('Parse error2', wiki_text)
            break

        content = wiki_text[pos_open+2: pos_close]
        link = content.split('|').pop().strip()
        links.append(link)
        needle = pos_close + 2

    return links


def page_name_to_dict(wiki_db, name):
    name = '_'.join(name.split(' '))
    rs = wiki_db.selectAndFetchAll('''
        select page_id, page_title name from page
        where page_title = %s and page_namespace = 0
    ''', args=(name,), decode=True)
    if len(rs) == 1:
        return rs[0]
    elif name[0].islower():
        capitalized = name[0].upper() + name[1:]
        return page_name_to_dict(wiki_db, capitalized)
    else:
        print(wiki_db.lang, 'Not found page_id by name', name)
        return None


class FeatureItemRelationManager:
    def __init__(
            self, lang_db, featured_page_generator,
            search_feature_page_from_page):
        self.lang_db = lang_db
        self.page_generator = featured_page_generator
        self.search = search_feature_page_from_page
        self.page_id_to_features = {}

    def load(self):
        for page in self.page_generator():
            self.page_id_to_features[page['page_id']] = self.search(page)

    def generate_feature_pages(self):
        pages = []
        for p, each_pages in self.page_id_to_features.items():
            pages.extend(each_pages)

        page_map = {p['page_id']: p for p in pages}
        pages = page_map.values()  # unique
        pages = sorted(pages, key=lambda p: p['page_id'])
        return (p for p in pages)


class _MusicGenreBuilder:
    def __init__(self, master_db, lang_db, other_lang_dbs):
        self.master_db = master_db
        self.lang_db = lang_db
        self.lang = lang_db.lang
        self.ipr_manager = ItemPageRelationManager(
            master_db, lang_db,
            other_lang_dbs)
        self.fir_manager = FeatureItemRelationManager(
            lang_db,
            self.generate_featured_pages,
            self.find_feature_from_page)

        if self.lang == 'en':
            self.infotype = 'Infobox_musical_artist'
            self.key = 'genre'
        elif self.lang == 'ja':
            self.infotype = 'Infobox_Musician'
            self.key = 'ジャンル'
        else:
            raise Exception('lang = %s is not supported.' % (self.lang,))

        self.feature_type_id = 1
        self.feature_type_name = 'Music Genre'
        self._name_to_page = {}  # cache

    def _page_name_to_dict(self, name):
        if name in self._name_to_page:
            return self._name_to_page[name]

        page = page_name_to_dict(self.lang_db, name)
        if page:
            self._name_to_page[name] = page
        return page

    def generate_featured_pages(self):
        return self.lang_db.generate_records(
            'an_page', cols=['page_id', 'infocontent'],
            cond='infotype = %s',
            arg=(self.infotype,), dict_format=True)

    def find_feature_from_page(self, page):
        wiki_object = json.loads(page['infocontent'])
        if self.key in wiki_object:
            wiki_text = wiki_object[self.key]
            names = find_links_from_wiki(wiki_text)
            pages = [self._page_name_to_dict(name) for name in names]
            pages = [p for p in pages if p is not None]
            return pages
        return []

    def build_feature_type_if_not_exists(self):
        rs = self.master_db.selectAndFetchAll('''
        select * from feature_type where feature_type_id = %s
        ''', args=(self.feature_type_id,))
        if len(rs) == 0:
            self.master_db.updateQuery('''
            insert into feature_type (feature_type_id, name) values(%s, %s)
            ''', args=(self.feature_type_id, self.feature_type_name))
            self.master_db.commit()

    def generate_insert_page(self):
        self.fir_manager.load()
        source_page_iter = self.fir_manager.generate_feature_pages()
        dest_page_iter = self.master_db.generate_records(
            'item_page', cols=['page_id'], cond='lang=%s',
            order='page_id asc', dict_format=True, arg=(self.lang,))
        syncer = Syncer(source_page_iter, dest_page_iter, ['page_id'], True)
        return syncer.generate_for_insert()

    def build(self):
        self.build_feature_type_if_not_exists()

        insert_page_iter = self.generate_insert_page()
        self.ipr_manager.merge_page_to_item(insert_page_iter)

        self.master_db.commit()


class FeatureBuilder:
    def __init__(self, master_db, lang_db, other_lang_dbs):
        self.builders = [
            _MusicGenreBuilder(master_db, lang_db, other_lang_dbs),
        ]

    def build(self):
        for builder in self.builders:
            builder.build()
