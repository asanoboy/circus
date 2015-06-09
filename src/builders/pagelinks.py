from dbutils import *

class PagelinksBuilder:
    def __init__(self, wiki_db):
        self.db = wiki_db


    def build(self):
        with TableIndexHolder(self.db.openConn, 'an_pagelinks'):
            self.db.updateQuery("""
            insert into an_pagelinks
            select pl.pl_from id_from, p.page_id id_to from pagelinks pl
            inner join page p on p.page_title = pl.pl_title and p.page_namespace = pl.pl_namespace
            """)
            self.db.commit()
