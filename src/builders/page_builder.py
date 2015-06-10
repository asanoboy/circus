from dbutils import selectGenerator
from circus_itertools import lazy_chunked as chunked
import json

class PageBuilder:
    def __init__(self, wiki_db):
        self.db = wiki_db
        pass

    def _build_page(self):
        allowedInfoNames = [ r['name'] for r in self.db.selectAndFetchAll("select name from an_info") ]
        def createPageInternal(title):
            return self.db.createPageByTitle(title, allowedInfoNames)

        pages = map(createPageInternal, self.db.allPageTitlesGenerator())
        #pages = filter(lambda p: p and p.info, pages)

        for pageList in chunked(pages, 100):
            self.db.multiInsert('an_page', ['page_id', 'page_type', 'name', 'infotype', 'infocontent'], \
                [[ \
                    p.id, \
                    0, \
                    p.title, \
                    p.info.name if p.info else None, \
                    json.dumps(p.info.keyValue) if p.info else '' \
                ] for p in pageList])

        
    def _build_cat(self):
        cat_iter = selectGenerator(self.db.openConn, 'page', \
            cols=['page_id', 'page_title'], \
            cond='page_namespace=14')

        for cat_list in chunked(cat_iter, 1000):
            self.db.multiInsert('an_page', ['page_id', 'name', 'page_type', 'infotype', 'infocontent'], \
                [ [r[0], r[1], 1, None, ''] for r in cat_list] )
        

    def build(self):
        self._build_cat()
        self._build_page()

        self.db.commit()
