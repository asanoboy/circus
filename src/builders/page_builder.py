from dbutils import selectGenerator
from circus_itertools import lazy_chunked as chunked
from .syncer import Syncer
import json

class PageBuilder:
    def __init__(self, wiki_db):
        self.db = wiki_db
        pass

    def _build_page(self):
        allowedInfoNames = [ r['name'] for r in self.db.selectAndFetchAll("select name from an_info") ]
        def createPageInternal(title):
            return self.db.createPageByTitle(title, allowedInfoNames)

        source_all_page_iter = self.db.generate_records('page', cols=['page_id', 'page_title', 'page_namespace', 'page_is_redirect'], \
            order='page_id asc', \
            dict_format=True)
        source_page_iter = filter(lambda x: int(x['page_namespace'])==0 and int(x['page_is_redirect'])==0, source_all_page_iter)
        dest_page_iter = self.db.generate_records('an_page', cols=['page_id'], \
            order='page_id asc', \
            dict_format=True)

        syncer = Syncer(source_page_iter, dest_page_iter, ['page_id'])
        #pages = map(createPageInternal, self.db.allPageTitlesGenerator())
        pages = map(createPageInternal, ( r['page_title'] for r in syncer.generate_for_insert() ))
        #pages = filter(lambda p: p and p.info, pages)

        for pageList in chunked(pages, 100):
            rt = self.db.multiInsert('an_page', ['page_id', 'page_type', 'name', 'infotype', 'infocontent'], \
                [[ \
                    p.id, \
                    0, \
                    p.title, \
                    p.info.name if p.info else None, \
                    json.dumps(p.info.keyValue) if p.info else '' \
                ] for p in pageList], \
                safe=True)

            if not rt:
                print([p.id for p in pageList])
        
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
