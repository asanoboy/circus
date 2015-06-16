from circus_itertools import lazy_chunked as chunked
from .builder_utils import ItemPageRelationManager, Syncer


class ItemTagBuilderBase:
    def __init__(self, master_db, lang_db, other_lang_dbs, tag_id, tag_name):
        self.master_db = master_db
        self.lang = lang_db.lang
        self.ipr_manager = ItemPageRelationManager(
            master_db, lang_db, other_lang_dbs)
        self.tag_id = tag_id
        self.tag_name = tag_name

    def build_tag_if_not_exists(self):
        rs = self.master_db.selectAndFetchAll('''
        select * from tag where tag_id = %s
        ''', args=(self.tag_id,))
        if len(rs) == 0:
            self.master_db.updateQuery('''
            insert into tag (tag_id, name) values(%s, %s)
            ''', args=(self.tag_id, self.tag_name))
            self.master_db.commit()

    def build(self):
        self.build_tag_if_not_exists()

        insert_page_iter = self.generate_insert_page()
        self.ipr_manager.merge_page_to_item(insert_page_iter)

        item_ids = sorted(self.ipr_manager.created_item_ids)
        item_id_iter = ({'item_id': i} for i in item_ids)
        exists_id_iter = self.master_db.generate_records(
            'tag_item',
            cols=['item_id'],
            cond='tag_id=%s',
            order='item_id asc',
            args=(self.lang,))

        syncer = Syncer(item_id_iter, exists_id_iter, ['item_id'], True)
        for records in chunked(syncer.generate_for_insert(), 1000):
            self.master_db.multiInsert(
                'tag_item', ['item_id', 'tag_id'],
                [[
                    r['item_id'],
                    self.tag_id,
                ] for r in records])

        self.master_db.commit()


class _MusicArtistBuilder(ItemTagBuilderBase):
    def __init__(self, master_db, lang_db, other_lang_dbs):
        super().__init__(
            master_db, lang_db, other_lang_dbs,
            1,
            'Musician')
        self.lang_db = lang_db

    def generate_insert_page(self):
        infotype = None

        if self.lang == 'en':
            infotype = 'Infobox_musical_artist'
        elif self.lang == 'ja':
            infotype = 'Infobox_Musician'

        source_dict_iter = self.lang_db.generate_records(
            'an_page p',
            cols=['p.page_id page_id', 'p.name name'],
            cond='infotype=%s',
            order='page_id asc',
            args=(infotype,))

        dest_dict_iter = self.master_db.generate_records(
            'item_page',
            cols=['page_id', 'name'],
            cond='lang=%s',
            order='page_id asc',
            args=(self.lang,))

        syncer = Syncer(source_dict_iter, dest_dict_iter, ['page_id'], True)
        return syncer.generate_for_insert()


class ItemTagBuilder:
    def __init__(self, master_db, lang_db, other_lang_dbs):
        self.builders = [
            _MusicArtistBuilder(master_db, lang_db, other_lang_dbs),
        ]

    def build(self):
        for builder in self.builders:
            builder.build()


if __name__ == '__main__':
    def gen_baisu(val_to, seed, key):
        for i in range(val_to):
            if i % seed == 0:
                yield {key: i}

    syn = Syncer(gen_baisu(10, 2, 'a'), gen_baisu(10, 3, 'a'), ['a'])
    list(syn.generate_diff())
