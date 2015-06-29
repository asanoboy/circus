from .builder_utils import IdMap, is_equal_as_pagename
from model.master import Item, Page
from .feature.musical_artist import load as musical_artist_load


class FeatureItemRelationManager:
    def __init__(
            self,
            master,
            lang_db,
            other_lang_dbs,
            feature_type_id,
            feature_type_name,
            load_features):
        self.master = master
        self.lang_db = lang_db
        self.lang = lang_db.lang
        self.load_features = load_features
        self.feature_type_id = feature_type_id
        self.feature_type_name = feature_type_name
        self.lang_to_other_lang_db = {db.lang: db for db in other_lang_dbs}

        self.item_map = IdMap(Item)

    def get_or_create_by_page_id(self, page_id):
        if self.item_map.has(page_id):
            return self.item_map.get_or_create(page_id)
        else:
            page_record = self.lang_db.selectOne('''
                select name from an_page
                where page_id = %s
                ''', args=(page_id,))
            if page_record is None:
                raise Exception('Page_id: %s does not exist' % (page_id,))

            item = self._find_item(page_id, page_record['name'])
            if item:
                self.item_map.set(page_id, item)
                item.pages.append(Page(id=page_id, lang=self.lang))
                return item

            item = self.item_map.get_or_create(page_id)
            item.pages.append(Page(id=page_id, lang=self.lang))
            return item

    def _find_item(self, page_id, page_name):
        rs = self.lang_db.selectAndFetchAll('''
            select ll_lang lang, ll_title name from langlinks where ll_from=%s
            ''', args=(page_id,), decode=True)

        for lang, name in [(r['lang'], r['name']) for r in rs]:
            if lang not in self.lang_to_other_lang_db:
                continue

            db = self.lang_to_other_lang_db[lang]
            foreign_page = db.createPageByTitle(name, with_info=False)
            if not foreign_page:
                print("Invalid langlinks", page_id, lang, name)
                continue

            foreign_rs = db.selectAndFetchAll('''
                select ll_lang lang, ll_title name from langlinks
                where ll_from=%s and ll_lang = %s
                ''', args=(foreign_page.id, self.lang), decode=True)

            '''
            Confirms that found title is linked by found lang.
            '''
            if len(foreign_rs) == 1 and \
                    is_equal_as_pagename(foreign_rs[0]['name'], page_name):
                page = self.master.query(Page) \
                    .filter(
                        Page.id == foreign_page.id,
                        Page.lang == lang).first()
                if page:
                    return page.item
            else:
                print('''
                Does not match langlinks names( %s: %s, %s: %s)
                ''' % (lang, foreign_rs, self.lang, page_name))

        return None

    def _load(self):
        items = self.load_features(self.lang_db, self.get_or_create_by_page_id)

        pages = []
        features = []
        for item in items:
            pages.extend([p for p in item.pages if p.lang == self.lang])
            item.visible = 1
            features.extend(item.features)
            for f in item.features:
                pages.extend([
                    p for p in f.ref_item.pages if p.lang == self.lang])
                f.ref_item.visible = 0

        pages = list(set(pages))
        for page in pages:
            if page.name is None:
                rt = self.lang_db.selectOne('''
                    select name from an_page
                    where page_id = %s
                    ''', args=(page.id,))
                page.name = rt['name']
            if page.viewcount is None:
                rt = self.lang_db.selectOne('''
                    select count from an_pagecount
                    where page_id = %s and year = 2014
                    ''', args=(page.id,))
                page.viewcount = rt['count'] if rt else 0

            if page.lang is None:
                raise Exception('Invalid page_id: %s' % (page.id,))

        # print('============')
        # for item in items:
        #     for p in item.pages:
        #         if p.lang == self.lang:
        #             item_page = p
        #     print(item, len(item.pages))
        #     for f in item.features:
        #         for p in f.ref_item.pages:
        #             print(
        #                 'item_page_id: %s, feature_page_id: %s, year: %s' %
        #                 (item_page.id, p.id, f.year))

        self.master.add_all(pages)
        self.master.flush()

        self.master.add_all(items)
        self.master.flush()

    def _find_feature_id(self, item_id):
        rs = self.master_db.selectAndFetchAll('''
            select feature_id from feature
            where feature_type_id = %s and opt_item_id = %s
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

    def build(self):
        self._load()


class FeatureBuilder:
    def __init__(self, master, lang_db, other_lang_dbs):
        feature_type_id = 1
        feature_type_name = 'Music Genre'

        self.fir_manager = FeatureItemRelationManager(
            master,
            lang_db,
            other_lang_dbs,
            feature_type_id,
            feature_type_name,
            musical_artist_load)

    def build(self):
        self.fir_manager.build()
