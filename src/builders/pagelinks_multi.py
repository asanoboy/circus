from dbutils import TableIndexHolder


class Builder:
    def __init__(self, wiki_db):
        self.db = wiki_db

    def build(self):
        max_page_id = self.db.selectOne('''
            select id_from from an_pagelinks
            order by id_from desc limit 1
            ''')['id_from']
        current = 0
        # interval = 1000
        print('== max :', max_page_id)

        with TableIndexHolder(self.db.openConn, 'an_pagelinks_multi'):
            buf = []
            while current <= max_page_id:
                if current % 1000 == 0:
                    print('current=', current)
                from_records = self.db.selectAndFetchAll('''
                    select id_from, id_to from an_pagelinks
                    where id_from = %s
                ''', args=(current,))
                to_records = self.db.selectAndFetchAll('''
                    select id_from, id_to from an_pagelinks
                    where id_to = %s
                ''', args=(current,))
                to_record_set_inv = [
                    (r['id_to'], r['id_from']) for r in to_records]

                for from_record in from_records:
                    id_set = (from_record['id_from'], from_record['id_to'])
                    if id_set in to_record_set_inv:
                        buf.append(
                            [from_record['id_from'], from_record['id_to']])

                if len(buf) > 1000:
                    self.db.multiInsert(
                        'an_pagelinks_multi',
                        ['id_from', 'id_to'],
                        buf)
                    self.db.commit()
                    buf = []
                current += 1

            if len(buf) > 0:
                self.db.multiInsert(
                    'an_pagelinks_multi',
                    ['id_from', 'id_to'],
                    buf)
                self.db.commit()

            # while True or current < max_page_id:
            #     print('= current :', current)
            #     self.db.updateQuery("""
            #         insert into an_pagelinks_multi
            #         select pl.id_from, pl.id_to
            #         from an_pagelinks pl
            #         inner join an_pagelinks pl2 on
            #             pl.id_from = pl2.id_to and
            #             pl.id_to = pl2.id_from
            #         where pl.id_from >= %s and pl.id_to < %s
            #         """, args=(current, current + interval))
            #     self.db.commit()
            #     current += interval
            # print('= finished inserting:', current)
        print('= finished indexing')
