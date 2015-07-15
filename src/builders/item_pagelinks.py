from model.master import Tag


class Builder:
    def __init__(self, session, wiki_db):
        self.session = session
        self.wiki_db = wiki_db

    def build(self):
        tags = self.session.query(Tag).all()
        item_pages = []
        for tag in tags:
            for item in tag.items:
                for page in item.pages:
                    if page.lang != self.wiki_db.lang:
                        continue
                    item_pages.append(page)

        joined_ids = ','.join([str(p.page_id) for p in item_pages])
        self.wiki_db.updateQuery('''
            insert into an_pagelinks_picked
            select id_from, id_to from an_pagelinks_multi
            where id_from in (%s) and id_to in (%s)
            ''' % (joined_ids, joined_ids))
        self.wiki_db.commit()
