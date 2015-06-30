import random
import time
import json
from model.master import Base, Page, Item, FeatureItemAssoc
from http_utils import post
from dbutils import master_session
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

    def ids_sorted_by_strength(self):
        ids = [r[0] for r in sorted(
            self.id_to_val.items(), key=lambda x: -x[1])]
        return ids

    def items(self):
        return self.id_to_val.items()

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


def get_popular_items(session, lang):
    pages = session.query(Page). \
        join(Page.item). \
        filter(Page.lang == lang). \
        filter(Item.visible == 1). \
        order_by(Page.popularity.desc()). \
        limit(100)
    return ItemList({p.item_id: 0 for p in pages})


def find_item(lang, session, action_history, recommender):
    all_ids = [a.item_id for a in action_history]
    likes = [a.item_id for a in action_history if a.input_type <= 1]
    like_items = ItemList({item_id: 1 for item_id in likes})
    knows = [a.item_id for a in action_history if a.input_type <= 2]
    know_items = ItemList({item_id: 1 for item_id in knows})
    unknowns = [a.item_id for a in action_history if a.input_type == 3]
    unknown_items = ItemList({item_id: 1 for item_id in unknowns})

    items = ItemList()

    if len(likes) == 0 and len(knows) == 0:
        items += get_popular_items(session, lang)
        if len(unknowns):
            items -= recommender.find_items(unknown_items)
    else:
        if len(likes):
            items += like_items
        elif len(knows):
            items += know_items

        if len(unknowns):
            items -= unknown_items

        items = recommender.find_items(items)

    with Lap('calc post-process'):
        ids = items.ids_sorted_by_strength()
        ids = [i for i in ids if i not in all_ids]

    item_id = ids[rand(len(ids))]
    return session.query(Page).filter(
        # Page.item_id == item_id, Page.lang == lang).fisrt()
        Page.item_id == item_id).first()


use_ff = False


class Recommender:
    urlbase = 'http://localhost:8000'

    def post(self, path, idmap):
        rt = post(
            self.urlbase + path, {'idmap': json.dumps(idmap)})
        if rt.status == 200:
            data = json.loads(rt.read().decode('utf-8'))
            return {int(key): float(val) for key, val in data.items()}

    def feature_to_item(self, idmap):
        return self.post('/feature_to_item', idmap)

    def feature_to_feature(self, idmap):
        return self.post('/feature_to_feature', idmap)

    def item_to_feature(self, idmap):
        return self.post('/item_to_feature', idmap)

    def find_items(self, items):
        item_dict = {item_id: strength for item_id, strength in items.items()}
        with Lap('item_feature'):
            feature_dict = self.item_feature_mtx * item_dict

        if use_ff:
            feature_dict = self.feature_feature_mtx * feature_dict

        with Lap('feature_item'):
            item_dict = self.feature_item_mtx * feature_dict

        return ItemList({key: val for key, val in item_dict.items()})


class Action:
    def __init__(self, input_type, page):
        self.item_id = page.item_id
        self.input_type = int(input_type)
        self.page = page


class Client:
    def __init__(self, session, lang, seed=None):
        self.session = session
        self.lang = lang
        self.seed = seed
        self.action_history = []

    def init(self):
        random.seed(time.time())
        if self.seed is None:
            self.seed = rand()
        random.seed(self.seed)

        self.recommender = Recommender()

        # self.db.updateQuery('''
        #     insert into session (seed, lang)
        #     values(%s, %s)
        # ''', args=(self.seed, self.lang))
        self.next_page = find_item(
            self.lang, self.session, self.action_history, self.recommender)

    def run(self):
        while 1:
            num = input('''>>> %s (popularity: %s, item_id: %s)
1: Like it
2: Just know it
3: Don't know it
> ''' % (
                self.next_page.name,
                self.next_page.popularity,
                self.next_page.item_id))

            if num == 'q':
                break
            elif num == 'f':
                assocs = self.session.query(FeatureItemAssoc).filter(
                    FeatureItemAssoc.item_id == self.next_page.item_id)
                for assoc in assocs:
                    f = assocs.feature
                    print(f.ref_item.page.name, f.year)
                continue
            elif num == 'i':
                # pages = self.db.selectAndFetchAll('''
                #     select * from item_page where item_id = %s
                #     ''', args=(self.next_item['item_id'],))
                # for p in pages:
                #     print(p)
                continue
            elif num == '1' or num == '2' or num == '3':
                self.action_history.append(Action(num, self.next_page))
                self.next_page = find_item(
                    self.lang, self.session,
                    self.action_history, self.recommender)
            else:
                continue


if __name__ == '__main__':
    with master_session('master', Base) as session:
        c = Client(session, 'en')
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
