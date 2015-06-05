import json
from circus_itertools import lazy_chunked as chunked
from models import pos_in_content

class PagelinksFilteredBuilder:
    def __init__(self, wiki_db):
        self.db = wiki_db

    def _generate_filtered_pagelinks(self, from_page_dicts):
        for page_dict in from_page_dicts:
            page_id = page_dict['page_id']
            name = page_dict['name']
            page = self.db.createPageByTitle(name, with_info=False)
            if page is None:
                print('Invalid (page_id=%s, name=%s) in an_page ' % (page_id, name))
                continue

            content = page.text
            info_key_value = json.loads(page_dict['infocontent'])
            info_values_joined = '  '.join(info_key_value.values())

            res = self.db.selectAndFetchAll("""
            select p.page_id, p.name, p.infocontent from an_pagelinks pl
            inner join an_page p on p.page_id = pl.id_to
            where pl.id_from = %s
            """, (page_id,))
            #res = self.db.selectAndFetchAll("""
            #select p.page_id, p.page_title name, ap.infocontent from pagelinks pl
            #inner join page p on p.page_title = pl.pl_title and p.page_namespace = pl.pl_namespace
            #inner join an_page ap on ap.page_id = p.page_id
            #where pl.pl_from = %s
            #""", (page_id,), decode=True)
            for record in res:
                id_to = record['page_id']
                name_to = record['name']
                pos_content = pos_in_content(name_to, content)
                if pos_content != -1:
                    pos_info = pos_in_content(name_to, info_values_joined)
                    in_infobox = 0 if pos_info == -1 else 1
                    if pos_info == -1:
                        pos_info = None
                    yield [page_id, id_to, in_infobox, pos_info, pos_content]
                    


    def build(self):
        page_dicts = self.db.allFeaturedPageGenerator(dictFormat=True, featured=False)
        links = self._generate_filtered_pagelinks(page_dicts)
        interval = 10000
        cnt = 0
        for link_records in chunked(links, interval):
            self.db.multiInsert('an_pagelinks_filtered', \
                ['id_from', 'id_to', 'in_infobox', 'pos_info', 'pos_content'], \
                link_records)
            cnt += interval
            print('Inserted', cnt)

        self.db.commit()
