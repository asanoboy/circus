from circus_itertools import lazy_chunked as chunked

class SyncRecord:
    def __init__(self, record, compares):
        self.raw = record
        self.comps = compares

    def __gt__(self, other):
        for comp in self.comps:
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
    def __init__(self, source_iter, dest_iter, compares):
        self.source = source_iter
        self.dest = dest_iter
        self.comps = compares

    def _next_source(self):
        try:
            return SyncRecord(self.source.__next__(), self.comps)
        except StopIteration:
            return None

    def _next_dest(self):
        try:
            return SyncRecord(self.dest.__next__(), self.comps)
        except StopIteration:
            return None

    def _get_compares(self, record):
        return [ record[key] for key in self.comps ]

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

class _MusicArtistBuilder:
    def __init__(self, master_db, lang_db):
        self.master_db = master_db
        self.lang_db = lang_db
        self.lang = lang_db.lang
        if self.lang == 'en':
            self.infotype = 'Infobox_musical_artist'
        elif self.lang == 'ja':
            self.infotype = 'Infobox_Musician'


        self.tag_id = 1
        self.tag_name = 'Musician'
        self.pagecount_year = 2014

    def build_tag_if_not_exists(self):
        rs = self.master_db.selectAndFetchAll('select * from tag where tag_id = %s', args=(self.tag_id,))
        if len(rs) == 0:
            self.master_db.updateQuery('insert into tag (tag_id, name) values(%s, %s)', args=(self.tag_id, self.tag_name))
            self.master_db.commit()

    def _generate_insert_page(self):
        source_list_iter = self.lang_db.generate_records('an_page p', \
            cols=['p.page_id page_id', 'p.name name, pc.count count'], \
            joins=['inner join an_pagecount pc on pc.page_id = p.page_id and pc.year=%s'], \
            cond='infotype=%s', \
            order='page_id asc', \
            arg=(self.pagecount_year, self.infotype) )
        source_dict_iter = ( {'page_id': r[0], 'name': r[1], 'count': r[2]} for r in source_list_iter )

        dest_list_iter = self.master_db.generate_records('item_page', \
            cols=['page_id', 'name'], \
            cond='lang=%s', \
            order='page_id asc', \
            arg=(self.lang,) )
        dest_dict_iter = ( {'page_id': r[0], 'name': r[1]} for r in dest_list_iter )

        sync = Syncer(source_dict_iter, dest_dict_iter, ['page_id'])
        insert_source_iter = filter(lambda x: x[0] is not None, sync.generate_diff())
        insert_page_iter = map(lambda x: x[0], insert_source_iter)
        return insert_page_iter

    def _get_current_max_item_id(self):
        rs = self.master_db.selectAndFetchAll('select item_id from item order by item_id desc limit 1')
        if len(rs) == 0:
            return 0
        else:
            int(rs[0]['item_id'])
        

    def build(self):
        self.build_tag_if_not_exists()

        last_item_id = self._get_current_max_item_id()

        for pages in chunked(self._generate_insert_page(), 1000):
            page_id_to_item_id = { r['page_id']: last_item_id + i + 1 for i, r in enumerate(pages) }
            last_item_id += len(pages)

            self.master_db.multiInsert('item', ['item_id', 'visible'], \
                [ [ \
                    page_id_to_item_id[p['page_id']], \
                    1, \
                ] for p in pages])

            self.master_db.multiInsert('tag_item', ['item_id', 'tag_id'], \
                [ [ \
                    page_id_to_item_id[p['page_id']], \
                    self.tag_id, \
                ] for p in pages])

            self.master_db.multiInsert('item_page', ['page_id', 'name', 'lang', 'item_id', 'view_count'], \
                [ [ \
                    p['page_id'], \
                    p['name'], \
                    self.lang, \
                    page_id_to_item_id[p['page_id']], \
                    p['count'], \
                ] for p in pages])
            
        self.master_db.commit()


class ItemTagBuilder:
    def __init__(self, master_db, lang_db):
        self.builders = [ \
            _MusicArtistBuilder(master_db, lang_db), \
        ]

    def build(self):
        for builder in self.builders:
            builder.build()


if __name__ == '__main__':
    def gen_baisu(val_to, seed, key):
        for i in range(val_to):
            if i % seed == 0:
                yield {key: i}

    syn = Syncer( gen_baisu(10, 2, 'a'), gen_baisu(10, 3, 'a'), ['a'])
    list(syn.generate_diff())
