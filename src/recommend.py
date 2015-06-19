import random
import time
from dbutils import MasterWikiDB


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

    def records_sorted_by_strength(self, db):
        ids = [r[0] for r in sorted(
            self.id_to_val.items(), key=lambda x: -x[1])]
        rs = [db.selectOne(
            '''
            select item_id, name, popularity
            from item_page where item_id=%s
            ''', args=(item_id,)) for item_id in ids]
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


def get_items(db, ids):
    rs = db.selectAndFetchAll('''
        select
            fi2.item_id,
            fi.strength * ff.strength * fi2.strength strength
        from feature_item fi
        inner join feature_relation ff on ff.id_from = fi.feature_id
        inner join feature_item fi2 on fi2.feature_id = ff.id_to
        where fi.item_id in (%s)
        order by strength desc
        ''' % (','.join([str(i) for i in ids])))
    if len(rs) == 0:
        return ItemList()

    threshold = rs[0]['strength'] * 0.5
    return ItemList({
        r['item_id']: r['strength']
        for r in rs if r['strength'] > threshold})


def get_popular_items(db, lang):
    rs = db.selectAndFetchAll('''
        select
            item_id
        from item_page
        where lang = %s
        order by popularity desc
        limit %s
    ''', args=(lang, 100))
    return ItemList({r['item_id']: 0 for r in rs})


def popularity_filter(records, knows, unknowns):
    return records


def find_item(lang, db, action_history):
    all_ids = [a.item_id for a in action_history]
    likes = [a.item_id for a in action_history if a.input_type <= 1]
    knows = [a.item_id for a in action_history if a.input_type <= 2]
    unknowns = [a.item_id for a in action_history if a.input_type == 3]

    if len(likes):
        positive_items = get_items(db, likes)
    elif len(knows):
        positive_items = get_items(db, knows)
    else:
        positive_items = get_popular_items(db, lang)

    if len(unknowns):
        negative_items = get_items(db, unknowns)
    else:
        negative_items = ItemList()

    items = positive_items - negative_items
    records = items.records_sorted_by_strength(db)
    records = [r for r in records if r['item_id'] not in all_ids]
    records = popularity_filter(records, knows, unknowns)
    return records[rand(len(records))]


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

        # self.db.updateQuery('''
        #     insert into session (seed, lang)
        #     values(%s, %s)
        # ''', args=(self.seed, self.lang))
        self.session_id = self.db.last_id()
        self.next_item = find_item(self.lang, self.db, self.action_history)

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

            self.action_history.append(Action(num, self.next_item))
            self.next_item = find_item(self.lang, self.db, self.action_history)


if __name__ == '__main__':
    c = Client('en')
    c.init()
    c.run()
