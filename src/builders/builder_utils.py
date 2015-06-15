from circus_itertools import lazy_chunked as chunked


def is_equal_as_pagename(a, b):
    return '_'.join(a.split(' ')) == \
           '_'.join(b.split(' '))


class SyncRecord:
    def __init__(self, record, compares, validate=False):
        self.raw = record
        self.comps = compares
        self.validate = validate

    def __gt__(self, other):
        for comp in self.comps:
            if self.validate and \
                    not isinstance(self.raw[comp], type(other.raw[comp])):
                raise Exception('Types of "%s" are not matched' % (comp,))

            if self.raw[comp] > other.raw[comp]:
                return True
            elif self.raw[comp] < other.raw[comp]:
                return False
            else:
                continue
        return False

    def __lt__(self, other):
        if self.__eq__(other):
            return False
        return not self.__gt__(other)

    def __eq__(self, other):
        for comp in self.comps:
            if self.raw[comp] != other.raw[comp]:
                return False

        return True


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
            return SyncRecord(
                self.source.__next__(), self.comps, self.validate)
        except StopIteration:
            return None

    def _next_dest(self):
        try:
            return SyncRecord(self.dest.__next__(), self.comps, self.validate)
        except StopIteration:
            return None

    def _get_compares(self, record):
        return [record[key] for key in self.comps]

    def generate_for_insert(self):
        source_iter = map(lambda x: x[0], self.generate_diff())
        return filter(lambda x: x is not None, source_iter)

    def generate_diff(self):
        source = self._next_source()
        dest = self._next_dest()
        while 1:
            '''
            print("===",
                source.raw if source else None, dest.raw if dest else None)
            '''
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


class IncrementalMerger:
    def __init__(self, db, table, pk, cols, inc_callback):
        self.db = db
        self.table = table
        self.pk = pk
        self.max_id = self._get_last_incremental_id()
        self.inc_callback = inc_callback
        self.insert_records = []

    def _get_last_incremental_id(self):
        rs = self.db.selectAndFetchAll('''
        select %s from %s order %s desc limit 1
        ''' % (self.pk, self.table, self.pk))

        if len(rs) == 0:
            return 0
        else:
            return rs[0][self.pk]

    def find_or_create(self, record):
        inc_id = self.inc_callback(record)
        if inc_id is not None:
            return inc_id
        else:
            self.max_id += 1
            insert_record = {col: record['col'] for col in self.cols}
            insert_record.update({self.pk: self.max_id})
            self.insert_records.append(insert_record)
            return self.max_id

    def insert_created(self):
        cols = self.cols.copy()
        cols.append(self.pk)
        for records in chunked(self.insert_records, 100):
            self.db.multiInsert(self.table, cols, records)


class Merger:
    def __init__(
            self,
            insert_iter,
            dest_db,
            dest_table,
            cols,
            cond,
            inc_merger=None):
        self.insert_iter
        self.dest_db = dest_db
        self.table = dest_table
        self.cols = cols
        self.inc_merger = inc_merger

    def execute(self):
        cols = self.cols.copy()
        if self.inc_merger:
            cols = cols.append(self.inc_merger.pk)

        for records in chunked(self.insert_iter, 100):
            if self.inc_merger:
                records = [r.update({
                    self.inc_merger.ok: self.inc_merger.find_or_create(r)
                    })
                    for r in records]

            self.dest_db.multiInsert(
                self.table,
                cols,
                [[r[col] for col in cols] for r in records])

        if self.inc_merger:
            self.inc_merger.insert_created()


class ItemPageRelationManager:
    def __init__(self, master_db, lang_db, other_lang_dbs):
        self.master_db = master_db
        self.lang_db = lang_db
        self.lang = lang_db.lang
        self.lang_to_other_lang_db = {db.lang: db for db in other_lang_dbs}
        self._last_item_id = None
        self.created_item_ids = []
        self.page_id_to_item_id = {}

    def _get_last_item_id(self):
        if self._last_item_id is None:
            rs = self.master_db.selectAndFetchAll('''
            select item_id from item order by item_id desc limit 1
            ''')
            print(rs)
            if len(rs) == 0:
                self._last_item_id = 0
            else:
                self._last_item_id = rs[0]['item_id']

        return self._last_item_id

    def _find_or_create_item_id(self, page_id, page_name):
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
                item_rs = self.master_db.selectAndFetchAll('''
                    select item_id from item_page
                    where lang = %s and page_id = %s
                    ''', args=(lang, foreign_page.id))
                if len(item_rs) == 1:
                    return item_rs[0]['item_id']
            else:
                print('''
                Does not match langlinks names( %s: %s, %s: %s)
                ''' % (lang, foreign_rs, self.lang, page_name))

        self._last_item_id += 1
        self.created_item_ids.append(self._last_item_id)
        return self._last_item_id

    def merge_page_to_item(self, page_iter):
        last_item_id = self._get_last_item_id()

        for pages in chunked(page_iter, 100):
            page_id_to_item_id = {
                p['page_id']:
                self._find_or_create_item_id(p['page_id'], p['name'])
                for p in pages}

            new_pages = [
                p for p in pages
                if page_id_to_item_id[p['page_id']] > last_item_id]
            self.master_db.multiInsert(
                'item', ['item_id', 'visible'],
                [[
                    page_id_to_item_id[p['page_id']],
                    1,
                ] for p in new_pages])

            self.master_db.multiInsert(
                'item_page',
                ['page_id', 'name', 'lang', 'item_id', 'view_count'],
                [[
                    p['page_id'],
                    p['name'],
                    self.lang,
                    page_id_to_item_id[p['page_id']],
                    0,
                ] for p in pages])

            self.page_id_to_item_id.update(page_id_to_item_id)