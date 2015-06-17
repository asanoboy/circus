
def generate_popularity():
    popularity = 1000
    ratio = 2
    num = 10
    while 1:
        for i in range(num):
            yield popularity

        num *= ratio
        popularity -= 1


class PopularityCalc:
    def __init__(self, master_db, lang_dbs):
        self.db = master_db
        self.lang_to_db = {d.lang: d for d in lang_dbs}

    def build(self):
        self.db.updateQuery('''
            update item_page
            set popularity = 0, viewcount = 0
        ''')

        record_to_count = {}
        for record in self.db.generate_records(
                'item_page', ['lang', 'page_id'],
                order='lang asc, page_id asc'):
            record_to_count[(record['lang'], record['page_id'])] = \
                self._calc_viewcount(record['lang'], record['page_id'])

        records = record_to_count.keys()
        records = sorted(records, key=lambda x: -record_to_count[x])
        record_to_popularity = {}
        current_popularity = None
        last_count = None
        for record, popularity in zip(records, generate_popularity()):
            count = record_to_count[record]
            if last_count != count:
                current_popularity = popularity

            record_to_popularity[record] = current_popularity
            last_count = count

        min_pop = current_popularity
        max_pop = generate_popularity().__next__()
        for record in record_to_popularity.keys():
            pop = record_to_popularity[record]
            record_to_popularity[record] = \
                ((pop - min_pop) / (max_pop - min_pop)) * 100

        for record, count in record_to_count.items():
            lang = record[0]
            page_id = record[1]
            popularity = record_to_popularity[record]
            self.db.updateQuery('''
                update item_page
                set popularity = %s, viewcount = %s
                where lang = %s and page_id = %s
            ''', args=(popularity, count, lang, page_id))
        self.db.commit()

    def _calc_viewcount(self, lang, page_id):
        db = self.lang_to_db[lang]
        rs = db.selectAndFetchAll('''
            select count from an_pagecount
            where page_id = %s
        ''', args=(page_id,))
        if len(rs) == 0:
            return 0
        elif len(rs) == 1:
            return rs[0]['count']
        else:
            rs = sorted(rs, key=lambda x: -x['count'])
            return rs[1]['count']
