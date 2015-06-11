from circus_itertools import lazy_chunked as chunked

class SyncRecord:
    def __init__(self, record, compares, validate=False):
        self.raw = record
        self.comps = compares
        self.validate = validate

    def __gt__(self, other):
        for comp in self.comps:
            if self.validate and type(self.raw[comp]) != type(other.raw[comp]):
                raise Exception('Types of "%s" are not matched' % (comp,))

            if self.raw[comp] > other.raw[comp]:
                return True
            elif self.raw[comp] < other.raw[comp]:
                return False
            else:
                continue
        return False
    
    def __lt__(self, other):
        return not self.__eq__(other) and not self.__gt__(other)

    def __eq__(self, other):
        return self.raw == other.raw

class Syncer:
    """
    Assumes that source_iter and dest_iter are sorted by compares asc.
    """
    def __init__(self, source_iter, dest_iter, compares, validate=False):
        self.source = source_iter
        self.dest = dest_iter
        self.comps = compares
        self.validate = validate

    def _next_source(self):
        try:
            return SyncRecord(self.source.__next__(), self.comps, self.validate)
        except StopIteration:
            return None

    def _next_dest(self):
        try:
            return SyncRecord(self.dest.__next__(), self.comps, self.validate)
        except StopIteration:
            return None

    def _get_compares(self, record):
        return [ record[key] for key in self.comps ]

    def generate_for_insert(self):
        source_iter = map(lambda x: x[0], self.generate_diff())
        return filter(lambda x: x is not None, source_iter)

    def generate_diff(self):
        source = self._next_source()
        dest = self._next_dest()
        while 1:
            #print("===", source.raw if source else None, dest.raw if dest else None)
            if source and dest:
                if source > dest:
                    yield (None, dest.raw)
                    dest = self._next_dest()
                elif source < dest:
                    yield (source.raw, None)
                    source = self._next_source()
                else:
                    source = self._next_source()
                    dest = self._next_dest()
            elif source and dest is None:
                yield (source.raw, None)
                source = self._next_source()
            elif source is None and dest:
                yield (None, dest.raw)
                dest = self._next_dest()
            else:
                break

class ItemPageRelationManager:
    def __init__(self, master_db, lang_db, other_lang_dbs):
        self.master_db = master_db
        self.lang_db = lang_db
        self.lang = lang_db.lang
        self.lang_to_other_lang_db = { db.lang: db for db in other_lang_dbs }
        self._last_item_id = None
        self.created_item_ids = []

    def _get_last_item_id(self):
        if self._last_item_id is None:
            rs = self.master_db.selectAndFetchAll('select item_id from item order by item_id desc limit 1')
            print(rs)
            if len(rs) == 0:
                self._last_item_id = 0
            else:
                self._last_item_id = rs[0]['item_id']

        return self._last_item_id

    def _find_or_create_item_id(self, page_id):
        rs = self.lang_db.selectAndFetchAll('select ll_lang lang, ll_title name from langlinks where ll_from=%s', \
            args=(page_id,), decode=True)

        for lang, name in [(r['lang'], r['name']) for r in rs]:
            if lang not in self.lang_to_other_lang_db:
                continue

            db = self.lang_to_other_lang_db[lang]
            foreign_page = db.createPageByTitle(name, with_info=False)
            if not foreign_page:
                print("Invalid langlinks", page_id, lang, name)
                continue

            item_rs = self.master_db.selectAndFetchAll('select item_id from item_page where lang = %s and page_id = %s', \
                args=(lang, foreign_page.id))
            if len(item_rs) == 1:
                return item_rs[0]['item_id']

        self._last_item_id += 1
        self.created_item_ids.append(self._last_item_id)
        return self._last_item_id

    def merge_page_to_item(self, page_iter):
        last_item_id = self._get_last_item_id()
        for pages in chunked(page_iter, 1000):
            page_id_to_item_id = { p['page_id']: self._find_or_create_item_id(p['page_id']) for p in pages }

            self.master_db.multiInsert('item', ['item_id', 'visible'], \
                [ [ \
                    page_id_to_item_id[p['page_id']], \
                    1, \
                ] for p in pages if page_id_to_item_id[p['page_id']] > last_item_id ])

            self.master_db.multiInsert('item_page', ['page_id', 'name', 'lang', 'item_id', 'view_count'], \
                [ [ \
                    p['page_id'], \
                    p['name'], \
                    self.lang, \
                    page_id_to_item_id[p['page_id']], \
                    0, \
                ] for p in pages])

