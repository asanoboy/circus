from circus_itertools import lazy_chunked as chunked
from .syncer import Syncer

class _MusicArtistBuilder:
    def __init__(self, master_db, lang_db, other_lang_dbs):
        self.master_db = master_db
        self.lang_db = lang_db
        self.lang_to_other_lang_db = { db.lang: db for db in other_lang_dbs }

        self.lang = lang_db.lang
        if self.lang == 'en':
            self.infotype = 'Infobox_musical_artist'
        elif self.lang == 'ja':
            self.infotype = 'Infobox_Musician'


        self.tag_id = 1
        self.tag_name = 'Musician'
        self.pagecount_year = 2014
        self.last_item_id = None


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
   
    def _find_item_id(self, page_id):
        rs = self.lang_db.selectAndFetchAll('select ll_lang lang, ll_title name from langlinks where ll_from=%s', \
            args=(page_id), decode=True)

        for lang, name in [(r['lang'], r['name']) for r in rs]:
            if lang not in self.lang_to_other_lang_db:
                continue

            db = self.lang_to_other_lang_db[lang]
            foreign_page = db.createPageByTitle(name, with_info=False)
            if foreign_page is None:
                print("Invalid langlinks", page_id, lang, name)
                continue

            item_rs = self.master_db.selectAndFetchAll('select item_id from item_page where lang = %s and page_id = %s', \
                args=(lang, foreign_page.id))
            if len(item_rs) == 1:
                return item_rs['item_id']

        new_item_id = self.last_item_id + 1
        self.last_item_id += 1
        return new_item_id
        

    def build(self):
        self.build_tag_if_not_exists()
        self.last_item_id = self._get_current_max_item_id()

        for pages in chunked(self._generate_insert_page(), 1000):
            page_id_to_item_id = { p['page_id']: self._find_item_id for p in pages }
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
    def __init__(self, master_db, lang_db, other_lang_dbs):
        self.builders = [ \
            _MusicArtistBuilder(master_db, lang_db, other_lang_dbs), \
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
