from circus_itertools import lazy_chunked as chunked
from .builder_utils import ItemPageRelationManager, Syncer
from .feature.musical_artist import load as musical_artist_load


class FeatureItemRelationManager:
    def __init__(
            self,
            master_db,
            lang_db,
            other_lang_dbs,
            feature_type_id,
            feature_type_name,
            load_features):
            # featured_page_generator,
            # search_feature_page_from_page):
        self.master_db = master_db
        self.lang_db = lang_db
        self.lang = lang_db.lang
        # self.page_generator = featured_page_generator
        # self.search = search_feature_page_from_page
        self.load_features = load_features
        self.page_id_to_features = {}
        self.feature_type_id = feature_type_id
        self.feature_type_name = feature_type_name

        self.ipr_manager = ItemPageRelationManager(
            master_db, lang_db,
            other_lang_dbs,
            0)

    def _load(self):
        self.page_id_to_features = self.load_features(
            self.master_db, self.lang_db)
        # for page in self.page_generator():
        #     self.page_id_to_features[page['page_id']] = self.search(page)

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
                order='page_id asc', args=(self.lang,)),
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
                args=(self.feature_type_id,)),
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
                args=(self.lang,)),
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
        feature_type_id = 1
        feature_type_name = 'Music Genre'

        self.fir_manager = FeatureItemRelationManager(
            master_db,
            lang_db,
            other_lang_dbs,
            feature_type_id,
            feature_type_name,
            musical_artist_load)

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
