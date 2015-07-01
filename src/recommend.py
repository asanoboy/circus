import argparse
import random
import time
import json
from model.master import Base, Page, Item, FeatureItemAssoc
from http_utils import post
from dbutils import master_session
from debug import get_logger, set_config


logger = None


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


class Recommender:
    urlbase = 'http://localhost:8000'

    def __init__(self, lang):
        self.lang = lang

    def post(self, path, idmap):
        rt = post(
            self.urlbase + path,
            {'idmap': json.dumps(idmap), 'lang': self.lang})
        if rt.status == 200:
            data = json.loads(rt.read().decode('utf-8'))
            return {int(key): float(val) for key, val in data.items()}

    def feature_to_item(self, idmap):
        with logger.lap('feature_to_item'):
            rt = self.post('/feature_to_item', idmap)
            logger.debug('item_feature_num: %s' % (len(rt),))
            return rt

    def feature_to_feature(self, idmap):
        with logger.lap('feature_to_feature'):
            rt = self.post('/feature_to_feature', idmap)
            logger.debug('feature_to_feature: %s' % (len(rt),))
            return rt

    def item_to_feature(self, idmap):
        with logger.lap('item_to_feature'):
            rt = self.post('/item_to_feature', idmap)
            logger.debug('item_to_feature: %s' % (len(rt),))
            return rt

    def find_items(self, items):
        item_dict = {item_id: strength for item_id, strength in items.items()}
        logger.debug('================')
        logger.debug('orig_num: %s' % (len(item_dict),))

        feature_dict = self.item_to_feature(item_dict)
        feature_dict = self.feature_to_feature(feature_dict)
        item_dict = self.feature_to_item(feature_dict)

        return ItemList({key: val for key, val in item_dict.items()})


class Action:
    def __init__(self, input_type, page):
        self.item_id = page.item_id
        self.input_type = int(input_type)
        self.page = page


class Result:
    def __init__(self, likes, knows, unknowns):
        self.likes = likes
        self.knows = knows
        self.unknowns = unknowns


class Client:
    def __init__(self, session, lang, seed=None):
        self.session = session
        self.lang = lang
        self.seed = seed
        self.action_history = []
        self.result_history = []
        self.curr = 0

    def init(self):
        random.seed(time.time())
        if self.seed is None:
            self.seed = rand()
        random.seed(self.seed)

        self.recommender = Recommender(self.lang)
        self.result_history.append(self.find_next())

    def run(self):
        while 1:
            next_page = self.result_history[self.curr].page
            num = input('''(%s) >>> %s (popularity: %s, item_id: %s)
1: Like it
2: Just know it
3: Don't know it
> ''' % (
                self.curr,
                next_page.name,
                next_page.popularity,
                next_page.item_id))

            if num == 'q':
                break
            if num == 'u':  # undo
                if self.curr == 0:
                    print('[Error] Can\'t undo!')
                else:
                    self.curr -= 1
                continue
            if num == 'r':  # redo
                if self.curr == len(self.result_history)-1:
                    print('[Error] Can\'t redo!')
                else:
                    self.curr += 1
                continue
            elif num == 'f':  # show feature
                assocs = self.session.query(FeatureItemAssoc).filter(
                    FeatureItemAssoc.item_id == next_page.item_id)
                for assoc in assocs:
                    f = assoc.feature
                    for p in [
                            p for p in f.ref_item.pages
                            if p.lang == self.lang]:
                        print(p.name, f.year, assoc.strength)
                continue
            elif num == 'p':  # show popular pages
                for item_id, dummy in self.get_popular_items().items():
                    item = self.session.query(Item).filter(
                        Item.id == item_id).first()
                    for p in item.pages:
                        if p.lang == self.lang:
                            print(p.name, p.popularity)
                continue
            elif num == 'i':
                # pages = self.db.selectAndFetchAll('''
                #     select * from item_page where item_id = %s
                #     ''', args=(self.next_page['item_id'],))
                # for p in pages:
                #     print(p)
                continue
            elif num == '1' or num == '2' or num == '3':  # select
                self.curr += 1
                self.action_history = self.action_history[:self.curr-1]
                self.result_history = self.result_history[:self.curr]

                self.action_history.append(Action(num, next_page))
                self.result_history.append(self.find_next())
            else:
                continue

    def get_popular_items(self):
        if not hasattr(self, '_popular_items'):
            pages = self.session.query(Page). \
                join(Page.item). \
                filter(Page.lang == self.lang). \
                filter(Item.visible == 1). \
                order_by(Page.popularity.desc()). \
                limit(100)
            return ItemList({p.item_id: 0 for p in pages})
        return self._popular_items

    def find_next(self):
        all_ids = [
            a.item_id for a in self.action_history]
        likes = [
            a.item_id for a in self.action_history if a.input_type <= 1]
        knows = [
            a.item_id for a in self.action_history if a.input_type <= 2]
        unknowns = [
            a.item_id for a in self.action_history if a.input_type == 3]

        like_items = ItemList({item_id: 1 for item_id in likes})
        know_items = ItemList({item_id: 1 for item_id in knows})
        unknown_items = ItemList({item_id: 1 for item_id in unknowns})

        result = Result(likes, knows, unknowns)

        items = ItemList()
        if len(likes) == 0 and len(knows) == 0:
            items += self.get_popular_items()
            if len(unknowns):
                items -= self.recommender.find_items(unknown_items)
        else:
            if len(likes):
                items += like_items
            elif len(knows):
                items += know_items

            if len(unknowns):
                items -= unknown_items

            items = self.recommender.find_items(items)

        with logger.lap('calc post-process'):
            ids = items.ids_sorted_by_strength()
            ids = [i for i in ids if i not in all_ids]

        item_id = ids[rand(len(ids))]
        page = self.session.query(Page).filter(
            Page.item_id == item_id, Page.lang == lang).first()

        result.page = page
        result.items = items
        return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--lang')  # ja,en
    args = parser.parse_args()
    args = vars(args)
    lang = args['lang']

    set_config('/home/anauser/log/circus/recommend.log')
    logger = get_logger(__name__)
    with master_session('master', Base) as session:
        c = Client(session, lang)
        with logger.lap('init'):
            c.init()

        c.run()
