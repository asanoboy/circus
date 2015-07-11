from .builder_utils import IdMap, is_equal_as_pagename
from model.master import Item, Page, Tag
from .feature.musical_artist import load as musical_artist_load


class FeatureItemRelationManager:
    def __init__(
            self,
            master,
            lang_db,
            other_lang_dbs,
            tag_id,
            tag_name,
            load_features):
        self.master = master
        self.lang_db = lang_db
        self.lang = lang_db.lang
        self.load_features = load_features
        self.tag_id = tag_id
        self.tag_name = tag_name
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
                item.pages.append(Page(page_id=page_id, lang=self.lang))
                return item

            item = self.item_map.get_or_create(page_id)
            item.pages.append(Page(page_id=page_id, lang=self.lang))
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
                        Page.page_id == foreign_page.id,
                        Page.lang == lang).first()
                if page:
                    return page.item
            else:
                print('''
                Does not match langlinks names( %s: %s, %s: %s)
                ''' % (lang, foreign_rs, self.lang, page_name))

        return None

    def _load(self):
        with self.load_features(
                self.master,
                self.lang_db,
                self.get_or_create_by_page_id) as items:
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
            for p in pages:
                p.load_from_wikidb(self.lang_db)
                if p.lang is None:
                    raise Exception('Invalid page_id: %s' % (p.page_id,))

                # print('fuga', p.name, p.page_id, p.viewcount, p)

            # print('============')
            # for item in items:
            #     for p in item.pages:
            #         if p.lang == self.lang:
            #             item_page = p
            #     print(item, len(item.pages))
            #     for f in item.features:
            #         for p in f.ref_item.pages:
            #             print(
            #                 '''
            #                 item_page_id: %s, feature_page_id: %s, year: %s
            #                 ''' % (item_page.id, p.id, f.year))

            tag = self.master.query(Tag).filter(Tag.id == self.tag_id).first()
            if not tag:
                tag = Tag(id=self.tag_id, name=self.tag_name)

            for item in items:
                if self.tag_id not in [t.id for t in item.tags]:
                    item.tags.append(tag)

            self.master.add_all(pages)
            self.master.flush()

            self.master.add_all(items)
            self.master.flush()

    def build(self):
        self._load()


class FeatureBuilder:
    def __init__(self, master, lang_db, other_lang_dbs):
        tag_id = 1
        tag_name = 'Music Genre'

        self.fir_manager = FeatureItemRelationManager(
            master,
            lang_db,
            other_lang_dbs,
            tag_id,
            tag_name,
            musical_artist_load)

    def build(self):
        self.fir_manager.build()
