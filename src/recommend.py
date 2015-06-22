import random
import time
from dbutils import MasterWikiDB
from numerical import RelationMatrix
from debug import Lap


def rand(maxim=1000):
    return int(random.random() * maxim)


class ItemList:
    def __init__(self, id_to_val=None):
        if id_to_val is None:
            self.id_to_val = {}
        else:
            self.id_to_val = id_to_val

    def push(self, val, item_id):
        if item_id in self.id_to_val:
            raise('item_id(%s) was already exists' % (item_id,))

        self.id_to_val[item_id] = val

    def ids_sorted_by_strength(self, db):
        ids = [r[0] for r in sorted(
            self.id_to_val.items(), key=lambda x: -x[1])]
        return ids

    def records_for_debug(self, db):
        ids = [r[0] for r in sorted(
            self.id_to_val.items(), key=lambda x: -x[1])]
        rs = [(db.selectOne(
            '''
            select
                ip.item_id, ip.name, ip.popularity
            from item_page ip
            where ip.item_id=%s
            ''', args=(item_id,)), self.id_to_val[item_id]) for item_id in ids]
        return rs

    def __add__(self, other):
        rt = ItemList()
        rt.id_to_val = self.id_to_val.copy()

        for other_item_id, other_val in other.id_to_val.items():
            if other_item_id in rt.id_to_val:
                rt.id_to_val[other_item_id] += other_val
            else:
                rt.id_to_val[other_item_id] = other_val
        return rt

    def __sub__(self, other):
        rt = ItemList()
        rt.id_to_val = self.id_to_val.copy()

        for other_item_id, other_val in other.id_to_val.items():
            if other_item_id in self.id_to_val:
                rt.id_to_val[other_item_id] -= other_val
        return rt

    def __mul__(self, other):
        rt = ItemList()
        rt.id_to_val = self.id_to_val.copy()
        for item_id in rt.id_to_val.keys():
            rt.id_to_val[item_id] *= other
        return rt


def find_features(db, item_id):
    return db.selectAndFetchAll('''
        select fi.feature_id, ip.*
        from feature_item fi
        inner join feature f on f.feature_id = fi.feature_id
        inner join item_page ip on ip.item_id = f.item_id
        where fi.item_id = %s
        ''', args=(item_id,))


def get_popular_items(db, lang):
    rs = db.selectAndFetchAll('''
        select
            i.item_id
        from item_page ip
        inner join item i on i.item_id = ip.item_id and i.visible = 1
        where ip.lang = %s
        order by ip.popularity desc
        limit %s
    ''', args=(lang, 100))
    return ItemList({r['item_id']: 0 for r in rs})


def find_item(lang, db, action_history, recommender):
    all_ids = [a.item_id for a in action_history]
    likes = [a.item_id for a in action_history if a.input_type <= 1]
    knows = [a.item_id for a in action_history if a.input_type <= 2]
    unknowns = [a.item_id for a in action_history if a.input_type == 3]

    with Lap('calc pos'):
        if len(likes):
            positive_items = recommender.find_items(likes)
        elif len(knows):
            positive_items = recommender.find_items(knows)
        else:
            positive_items = get_popular_items(db, lang)

    with Lap('calc nega'):
        if len(unknowns):
            negative_items = recommender.find_items(unknowns)
        else:
            negative_items = ItemList()

    with Lap('calc post-process'):
        items = positive_items - negative_items
        ids = items.ids_sorted_by_strength(db)
        ids = [i for i in ids if i not in all_ids]

    item_id = ids[rand(len(ids))]
    return db.selectOne('''
        select * from item_page
        where item_id = %s and lang = %s
        ''', args=(item_id, lang))


use_ff = False


class Recommender:
    def __init__(self, db):
        self.db = db

    def load(self):
        item_feature_mtx = RelationMatrix()
        with Lap('set data'):
            for r in self.db.selectAndFetchAll('''
                    select item_id, feature_id, strength
                    from feature_item'''):
                item_feature_mtx.append(
                    r['item_id'],
                    r['feature_id'],
                    r['strength'])

            if use_ff:
                feature_feature_mtx = RelationMatrix(
                    src=item_feature_mtx.get_dst(),
                    dst=item_feature_mtx.get_dst())

                for r in self.db.selectAndFetchAll('''
                        select id_from, id_to, strength
                        from feature_relation'''):
                    feature_feature_mtx.append(
                        r['id_from'],
                        r['id_to'],
                        r['strength'])

        with Lap('build item_feature'):
            item_feature_mtx.build()

        with Lap('build feature_item'):
            feature_item_mtx = item_feature_mtx.create_inverse()

        if use_ff:
            with Lap('build feature_feature'):
                feature_feature_mtx.build()

        self.item_feature_mtx = item_feature_mtx
        if use_ff:
            self.feature_feature_mtx = feature_feature_mtx
        self.feature_item_mtx = feature_item_mtx

    def find_items(self, item_ids):
        item_dict = {a: 1 for a in item_ids}
        with Lap('item_feature'):
            feature_dict = self.item_feature_mtx * item_dict
        # print('---------')
        # for record in [
        #         self.db.selectOne('''
        #         select f.feature_id, ip.* from feature f
        #         inner join item_page ip on ip.item_id = f.item_id
        #         where f.feature_id=%s
        #         ''', args=(feature_id,))
        #         for feature_id, strength in feature_dict.items()]:
        #     print(record)
        if use_ff:
            feature_dict = self.feature_feature_mtx * feature_dict
        # print('---------')
        # for record in [
        #         self.db.selectOne('''
        #         select f.feature_id, ip.* from feature f
        #         inner join item_page ip on ip.item_id = f.item_id
        #         where f.feature_id=%s
        #         ''', args=(feature_id,))
        #         for feature_id, strength in feature_dict.items()]:
        #     print(record)
        with Lap('feature_item'):
            item_dict = self.feature_item_mtx * feature_dict
        # print('---------')
        # for record in [
        #         self.db.selectOne('''
        #         select * from item_page
        #         where item_id=%s
        #         ''', args=(item_id,))
        #         for item_id, strength in item_dict.items()]:
        #     print(record)
        # print('---------')
        return ItemList({key: val for key, val in item_dict.items()})


class Action:
    def __init__(self, input_type, record):
        self.item_id = record['item_id']
        self.input_type = int(input_type)
        self.record = record


class Client:
    def __init__(self, lang, seed=None):
        self.lang = lang
        self.seed = seed
        self.db = MasterWikiDB('wikimaster')
        self.action_history = []

    def init(self):
        random.seed(time.time())
        if self.seed is None:
            self.seed = rand()
        random.seed(self.seed)

        self.recommender = Recommender(self.db)
        self.recommender.load()

        # self.db.updateQuery('''
        #     insert into session (seed, lang)
        #     values(%s, %s)
        # ''', args=(self.seed, self.lang))
        self.session_id = self.db.last_id()
        self.next_item = find_item(
            self.lang, self.db, self.action_history, self.recommender)

    def run(self):
        while 1:
            num = input('''>>> %s (popularity: %s, item_id: %s)
1: Like it
2: Just know it
3: Don't know it
> ''' % (
                self.next_item['name'],
                self.next_item['popularity'],
                self.next_item['item_id']))

            if num == 'q':
                break
            elif num == 'f':
                features = find_features(self.db, self.next_item['item_id'])
                for f in features:
                    print(f)
                continue
            elif num == 'i':
                pages = self.db.selectAndFetchAll('''
                    select * from item_page where item_id = %s
                    ''', args=(self.next_item['item_id'],))
                for p in pages:
                    print(p)
                continue
            elif num == '1' or num == '2' or num == '3':
                self.action_history.append(Action(num, self.next_item))
                self.next_item = find_item(
                    self.lang, self.db, self.action_history, self.recommender)
            else:
                continue


if __name__ == '__main__':
    c = Client('en')
    with Lap('init'):
        c.init()

    c.run()

    # db = MasterWikiDB('wikimaster')
    # rec = Recommender(db)
    # rec.load()
    # while 1:
    #     num = input('''>>>''')
    #     num = int(num)
    #     print(rec.find_items([num]))
    #     continue
