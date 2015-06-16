

class PopularityCalc:
    def __init__(self, master_db, lang_dbs):
        self.db = master_db
        self.lang_to_db = {d.lang: d for d in lang_dbs}

    def build(self):
        self.db.updateQuery('''
            update item_page
            set popularity = 0
        ''')

        record_to_count = {}
        for record in self.db.generate_records(
                'item_page', ['lang', 'page_id'],
                order='lang asc, page_id asc'):
            record_to_count[(record['lang'], record['page_id'])] = \
                self._calc_viewcount(record['lang'], record['page_id'])

        for record, count in record_to_count.items():
            lang = record[0]
            page_id = record[1]
            self.db.updateQuery('''
                update item_page
                set popularity = %s
                where lang = %s and page_id = %s
            ''', args=(count, lang, page_id))
        self.db.commit()

    def _calc_viewcount(self, lang, page_id):
        db = self.lang_to_db[lang]
        rs = db.selectAndFetchAll('''
            select count from an_pagecount
            where page_id = %s
        ''', args=(page_id,))
        if len(rs) == 0:
            return 0

        return max([r['count'] for r in rs])
