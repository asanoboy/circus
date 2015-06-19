import math
from itertools import groupby


class FeatureItemStrengthCalc:
    def __init__(self, master_db):
        self.db = master_db

    def build(self):
        features = self.db.selectAndFetchAll('''
        select feature_id from feature
        ''')
        feature_strength = 100  # TODO
        for feature_id in map(lambda x: x['feature_id'], features):
            feature_items = self.db.selectAndFetchAll('''
            select * from feature_item
            where feature_id = %s
            ''', args=(feature_id,))
            count = len(feature_items)
            if count == 0:
                continue
            strength = math.pow(feature_strength, 1/count)
            self.db.updateQuery('''
            update feature_item
            set strength = %s
            where feature_id = %s
            ''', args=(strength, feature_id))

        self.db.commit()


class FeatureRelationStrengthCalc:
    def __init__(self, master_db):
        self.db = master_db

    def build(self):
        self.db.updateQuery('''
        update feature_relation
        set strength =0
        ''')

        feature_items = self.db.selectAndFetchAll('''
        select feature_id, item_id from feature_item
        order by feature_id asc
        ''')
        # Assmes that feature_items are ordered by 'feature_id'.
        feature_id_to_items = {
            fid: [
                item['item_id'] for item in items]
            for fid, items in groupby(
                feature_items, key=lambda x: x['feature_id'])}

        # Assumes that id_from and id_to are symmetric.
        relations = self.db.selectAndFetchAll('''
        select id_from, id_to from feature_relation
        where id_from < id_to
        ''')

        for relation in relations:
            id_from = relation['id_from']
            id_to = relation['id_to']
            if id_from not in feature_id_to_items or \
                    id_to not in feature_id_to_items:
                continue
            joined = []
            joined.extend(feature_id_to_items[id_from])
            joined.extend(feature_id_to_items[id_to])
            joined_num = len(joined)
            strength = (joined_num - len(set(joined))) / joined_num
            self.db.updateQuery('''
            update feature_relation
            set strength = %s
            where (id_to = %s and id_from = %s) or
                (id_to = %s and id_from = %s)
            ''', args=(strength, id_to, id_from, id_from, id_to))

        self.db.commit()


class StrengthCalc:
    def __init__(self, master_db):
        self.feature_item_calc = FeatureItemStrengthCalc(master_db)
        self.feature_relation_calc = FeatureRelationStrengthCalc(master_db)

    def build(self):
        self.feature_item_calc.build()
        self.feature_relation_calc.build()
