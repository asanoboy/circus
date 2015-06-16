import json
from circus_itertools import lazy_chunked as chunked
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
    page = wiki_db.selectOne('''
        select
        page_id, page_title name,
        page_is_redirect is_redirect ,
        page_namespace namespace
        from page
        where page_title = %s and page_namespace = 0
    ''', args=(name,), decode=True)

    if page is None:
        if name[0].islower():
            capitalized = name[0].upper() + name[1:]
            page = page_name_to_dict(wiki_db, capitalized)
        else:
            print(wiki_db.lang, 'Not found page_id by name', name)
            return None

    if page and page['is_redirect'] != 0:
        redirect = wiki_db.selectOne('''
            select rd_title name from redirect
            where rd_from = %s and rd_namespace = %s
        ''', args=(page['page_id'], page['namespace']), decode=True)
        if redirect is None:
            print(
                wiki_db.lang,
                'Not found redirect from page_id=', page['page_id'])
            return page  # TODO
        print('Found redirect from', name, 'to', redirect['name'])
        page = page_name_to_dict(wiki_db, redirect['name'])
    return page


class FeatureItemRelationManager:
    def __init__(
            self,
            master_db,
            lang_db,
            other_lang_dbs,
            feature_type_id,
            feature_type_name,
            featured_page_generator,
            search_feature_page_from_page):
        self.master_db = master_db
        self.lang_db = lang_db
        self.lang = lang_db.lang
        self.page_generator = featured_page_generator
        self.search = search_feature_page_from_page
        self.page_id_to_features = {}
        self.feature_type_id = feature_type_id
        self.feature_type_name = feature_type_name

        self.ipr_manager = ItemPageRelationManager(
            master_db, lang_db,
            other_lang_dbs)

    def _load(self):
        for page in self.page_generator():
            self.page_id_to_features[page['page_id']] = self.search(page)

    def _generate_feature_pages(self):
        pages = []
        for p, each_pages in self.page_id_to_features.items():
            pages.extend(each_pages)

        page_map = {p['page_id']: p for p in pages}
        pages = page_map.values()  # unique
        pages = sorted(pages, key=lambda p: p['page_id'])
        return (p for p in pages)

    def _build_item(self):
        syncer = Syncer(
            self._generate_feature_pages(),
            self.master_db.generate_records(
                'item_page', cols=['page_id'], cond='lang=%s',
                order='page_id asc', dict_format=True, arg=(self.lang,)),
            ['page_id'], True)
        insert_page_iter = syncer.generate_for_insert()
        self.ipr_manager.merge_page_to_item(insert_page_iter)

    def _find_feature_id(self, item_id):
        rs = self.master_db.selectAndFetchAll('''
            select feature_id from feature
            where feature_type_id = %s and item_id = %s
        ''', args=(self.feature_type_id, item_id))
        if len(rs) == 1:
            return rs[0]['feature_id']
        raise 'Not found feature_id'

    def _find_item_id(self, page_id):
        rs = self.master_db.selectAndFetchAll('''
            select item_id from item_page
            where page_id = %s and lang = %s
        ''', args=(page_id, self.lang))
        if len(rs) == 1:
            return rs[0]['item_id']
        raise Exception('Not found item_id from page_id=%s' % (page_id,))

    def _generate_feature_items(self):
        rs = []
        for page_id, features in self.page_id_to_features.items():
            item_id = self._find_item_id(page_id)
            for feature in features:
                feature_item_id = self._find_item_id(feature['page_id'])
                feature_id = self._find_feature_id(feature_item_id)
                rs.append({'feature_id': feature_id, 'item_id': item_id})

        rs = sorted(rs, key=lambda x: x['item_id'])
        rs = sorted(rs, key=lambda x: x['feature_id'])
        return (x for x in rs)

    def _generate_feature(self):
        item_ids = []
        for page_id, features in self.page_id_to_features.items():
            for feature in features:
                feature_item_id = self._find_item_id(feature['page_id'])
                item_ids.append(feature_item_id)
        item_ids = list(set(item_ids))
        item_ids = sorted(item_ids)
        return ({'item_id': i} for i in item_ids)

    def _build_feature(self):
        syncer = Syncer(
            self._generate_feature(),
            self.master_db.generate_records(
                'feature', cols=['item_id'],
                cond='feature_type_id=%s',
                order='item_id asc',
                arg=(self.feature_type_id,),
                dict_format=True),
            ['item_id'], True)
        insert_iter = syncer.generate_for_insert()
        for records in chunked(insert_iter, 100):
            self.master_db.multiInsert(
                'feature',
                ['feature_type_id', 'item_id'],
                [[
                    self.feature_type_id,
                    r['item_id']
                ] for r in records]
            )

    def _build_feature_item(self):
        syncer = Syncer(
            self._generate_feature_items(),
            self.master_db.generate_records(
                'feature_item_lang', cols=['feature_id', 'item_id'],
                cond='lang=%s',
                order='feature_id asc, item_id asc',
                arg=(self.lang,),
                dict_format=True),
            ['item_id'], True)
        insert_iter = syncer.generate_for_insert()

        for records in chunked(insert_iter, 100):
            self.master_db.multiInsert(
                'feature_item_lang',
                ['feature_id', 'item_id', 'strength', 'lang'],
                [[
                    r['feature_id'],
                    r['item_id'],
                    0,
                    self.lang
                ] for r in records])

    def build_feature_type_if_not_exists(self):
        rs = self.master_db.selectAndFetchAll('''
        select * from feature_type where feature_type_id = %s
        ''', args=(self.feature_type_id,))
        if len(rs) == 0:
            self.master_db.updateQuery('''
            insert into feature_type (feature_type_id, name) values(%s, %s)
            ''', args=(self.feature_type_id, self.feature_type_name))
            self.master_db.commit()

    def build(self):
        self.build_feature_type_if_not_exists()
        self._load()

        self._build_item()
        self.master_db.commit()

        self._build_feature()
        self.master_db.commit()

        self._build_feature_item()
        self.master_db.commit()


class _MusicGenreBuilder:
    def __init__(self, master_db, lang_db, other_lang_dbs):
        self.master_db = master_db
        self.lang_db = lang_db
        self.feature_type_id = 1
        self.feature_type_name = 'Music Genre'

        self.fir_manager = FeatureItemRelationManager(
            master_db,
            lang_db,
            other_lang_dbs,
            self.feature_type_id,
            self.feature_type_name,
            self.generate_featured_pages,
            self.find_feature_from_page)

        lang = lang_db.lang
        if lang == 'en':
            self.infotype = 'Infobox_musical_artist'
            self.key = 'genre'
        elif lang == 'ja':
            self.infotype = 'Infobox_Musician'
            self.key = 'ジャンル'
        else:
            raise Exception('lang = %s is not supported.' % (lang,))

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

    def build(self):
        self.fir_manager.build()


class FeatureBuilder:
    def __init__(self, master_db, lang_db, other_lang_dbs):
        self.builders = [
            _MusicGenreBuilder(master_db, lang_db, other_lang_dbs),
        ]

    def build(self):
        for builder in self.builders:
            builder.build()
