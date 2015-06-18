import json
from circus_itertools import lazy_chunked as chunked
from .builder_utils import find_links_from_wiki, PageFactory


class PageToFeatureResolver:
    def __init__(self, master_db):
        self.db = master_db
        self.cache = {}

    def find_feature(self, lang, page_id):
        key = (lang, page_id)
        if key in self.cache:
            return self.cache[key]

        record = self.db.selectOne('''
        select f.feature_type_id, f.feature_id, f.item_id from feature f
        inner join item_page ip on f.item_id = ip.item_id
        where ip.lang=%s and ip.page_id=%s
        ''', args=(lang, page_id))

        self.cache[key] = record
        return record


class IdRelation:
    def __init__(self, id_from, id_to):
        self.id_from = id_from
        self.id_to = id_to

    def __hash__(self):
        return (self.id_from, self.id_to).__hash__()

    def __eq__(self, other):
        return self.id_from == other.id_from and \
            self.id_to == other.id_to


class FeatureRelationBuilder:
    def __init__(self, master_db, lang_dbs):
        self.master_db = master_db
        self.lang_dbs = lang_dbs
        self.lang_to_infokeys = {
            'ja': [
            ],
            'en': [
                'stylistic_origins',
                'derivatives',
                'subgenres',
            ]
        }

    def _create_relation_map(self):
        feature_type_id = 1
        relations_by_langs = {}

        for lang_db in self.lang_dbs:
            lang = lang_db.lang
            records_iter = self.master_db.generate_records(
                'feature f',
                cols=['ip.page_id'],
                joins=[
                    'inner join item_page ip on ip.item_id = f.item_id',
                ],
                cond='f.feature_type_id=%s and ip.lang=%s',
                args=(feature_type_id, lang))

            page_ids_iter = map(lambda x: x['page_id'], records_iter)
            page_dicts_iter = map(lambda x: lang_db.selectOne('''
                select page_id, infocontent from an_page where page_id = %s
                ''', args=(x,)), page_ids_iter)

            factory = PageFactory(lang_db)
            relations = []
            for page in page_dicts_iter:
                if not page['infocontent']:
                    continue
                info_object = json.loads(page['infocontent'])
                added_page_ids = []
                for key in self.lang_to_infokeys[lang]:
                    if key in info_object:
                        text = info_object[key]
                        names = find_links_from_wiki(text)
                        pages = [
                            factory.page_name_to_dict(name)
                            for name in names]
                        pages = [
                            p for p in pages
                            if p is not None and
                            p['page_id'] not in added_page_ids]
                        rels = [
                            IdRelation(page['page_id'], p['page_id'])
                            for p in pages]
                        added_page_ids.extend([p['page_id'] for p in pages])
                        relations.extend(rels)

            relations_by_langs[lang] = relations
            return relations_by_langs

    def build(self):
        relations_by_lang = self._create_relation_map()
        resolver = PageToFeatureResolver(self.master_db)
        feature_relations = []
        for lang, relations in relations_by_lang.items():
            for rel in relations:
                feature_from = resolver.find_feature(lang, rel.id_from)
                feature_to = resolver.find_feature(lang, rel.id_to)
                if feature_from and feature_to:
                    feature_relations.append(IdRelation(
                            feature_from['feature_id'],
                            feature_to['feature_id']))

        feature_relations = list(set(feature_relations))
        for relations in chunked(feature_relations, 100):
            self.master_db.multiInsert(
                'feature_relation',
                ['id_from', 'id_to', 'strength'],
                [[
                    rel.id_from,
                    rel.id_to,
                    1
                ] for rel in relations],
                on_duplicate='strength = strength + values(strength)')
            self.master_db.multiInsert(
                'feature_relation',
                ['id_from', 'id_to', 'strength'],
                [[
                    rel.id_to,
                    rel.id_from,
                    1
                ] for rel in relations],
                on_duplicate='strength = strength + values(strength)')

        self.master_db.commit()
