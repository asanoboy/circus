from itertools import groupby
from statistics import mean
from .builder_utils import IdMap
from model.master import Feature, FeatureRelationAssoc


def calc_range_list(thre, array, key=lambda x: x):
    item_cnt = 0
    pos_range_list = []
    pos_from = 0
    array = list(array)
    for i, f in enumerate(array):
        item_cnt += key(f)
        while item_cnt > thre:
            pos_range_list.append((pos_from, i+1))
            item_cnt -= key(array[pos_from])
            pos_from += 1

    return [
        list(array[pos_from: pos_to])
        for pos_from, pos_to in pos_range_list]


def find_closest_pos(val, array):
    if len(array) == 0:
        raise Exception('List is empty.')
    distance = -1
    pos = -1
    for i, el in enumerate(array):
        tmp = abs(el - val)
        if distance == -1 or distance > tmp:
            distance = tmp
            pos = i
    return pos


class FeatureRelationBuilder:
    def __init__(self, session):
        self.session = session
        self.assoc_map = IdMap(FeatureRelationAssoc)

    def build(self):
        THRE_NUM = 100

        for ref_item_id, features in groupby(
                self.session.query(Feature).
                order_by(Feature.ref_item_id.asc(), Feature.year.asc()),
                key=lambda f: f.ref_item_id):
            features = list(features)

            features_list = calc_range_list(
                THRE_NUM, features, key=lambda x: len(x.items))
            if len(features_list) == 0:
                features_list = [features]
            aves = [mean([f.year for f in fs]) for fs in features_list]

            for f in features:
                pos = find_closest_pos(f.year, aves)
                features_to = features_list[pos]
                for f_to in features_to:
                    self.assoc_map.get_or_create(
                        (f, f_to), id_from=f.id, id_to=f_to.id, strength=1)

        self.session.add_all(self.assoc_map.values())
        self.session.flush()


if __name__ == '__main__':
    if 0:
        arr = [i*i for i in range(10)]
        pos = find_closest_pos(30, arr)
        print(pos, '=>', arr[pos])

    if 1:
        arr = [1, 1, 1, 2, 1, 1, 1, 1, 2]
        print(calc_range_list(100, arr))

