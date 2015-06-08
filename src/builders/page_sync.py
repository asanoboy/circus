from circus_itertools import lazy_chunked as chunked

class PageSyncer:
    def __init__(self, master_db, wiki_db, imported_langs):
        self.imported_langs = imported_langs
        self.wiki_db = wiki_db
        self.master_db = master_db

    def sync(self):
        wiki_db = self.wiki_db
        master_db = self.master_db
        imported_langs = self.imported_langs
        lang = wik_db.lang

        other_langs = [l for l in imported_langs if l != lang]
        lang_to_wiki_db = { l: WikiDB(l) for l in other_langs}

        page_iter = wiki_db.allFeaturedPageGenerator(dictFormat=True, featured=False)
        missing_page_iter = master_db.missing_page_generator(lang, page_iter)
        #linked_other_lang_page_infos_iter = wiki_db.other_lang_page_infos_generator(missing_page_id_iter)

        master_id_and_lang_id_list = []
        new_lang_page_list = []
        for missing_page in missing_page_iter:
            other_lang_page_infos = wiki_db.selectAndFetchAll("""
                select ll_from orig_id, ll_title title, ll_lang lang from langlinks
                where ll_from = %s
            """, (missing_page['page_id'], ), decode=True)
        #for other_lang_page_infos in linked_other_lang_page_infos_iter:
            imported_infos = [ r for r in other_lang_page_infos if r['lang'] in imported_langs]
            found_master_page_id = None
            for link_info in imported_infos:
                linked_lang = link_info['lang']
                db = lang_to_wiki_db[linked_lang]
                res = db.selectAndFetchAll("""
                    select p.page_id from page p
                    inner join an_page ap on p.page_id = ap.page_id
                    where p.page_title = %s and p.page_namespace = 0
                """, (link_info['title'],))
                if len(res) == 1:
                    lang_page_id = res[0]['page_id']
                    pid_rs = master_db.selectAndFetchAll("""
                        select page_id master_page_id from page_lang_relation
                        where lang = %s and lang_page_id = %s
                    """, (linked_lang, lang_page_id))
                    if len(pid_rs) == 1:
                        found_master_page_id = pid_rs[0]['master_page_id']
                    elif len(pid_rs) > 1:
                        raise Exception('Invalid')

                elif len(res) > 1:
                    raise Exception('Invalid')

            if found_master_page_id:
                #master_id_and_lang_id_list.append([found_master_page_id, missing_page.id, missing_page.title])
                master_id_and_lang_id_list.append({'master_id': found_master_page_id, 'page':missing_page})
            else:
                new_lang_page_list.append(missing_page)


        res = master_db.selectAndFetchAll("""
            select max(page_id) max from page
        """)
        max_page_id = res[0]['max'] if res[0]['max'] is not None else 0
        for lang_pages in chunked(new_lang_page_list, 100):
            start_page_id = max_page_id + 1
            #data = [ [i+start_page_id, page.id, page.title] for i, page in enumerate(lang_pages)]
            data = [ {'master_id': i+start_page_id, 'page': page} for i, page in enumerate(lang_pages)]
            master_db.multiInsert('page', \
                    ['page_id'], \
                    [[d['master_id'],] for d in data])
            master_id_and_lang_id_list += data
            max_page_id += len(data)

        for values in chunked(master_id_and_lang_id_list, 100):
            master_db.multiInsert('page_lang_relation', \
                    ['page_id', 'lang_page_id', 'name', 'lang'], \
                    [[r['master_id'], r['page']['page_id'], r['page']['name'], lang] for r in values] )

        master_db.commit()
