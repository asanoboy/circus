from model.master import Tag
from debug import get_logger


class Builder:
    def __init__(self, session, wiki_db):
        self.session = session
        self.wiki_db = wiki_db

    def build(self):
        logger = get_logger(__name__)
        tags = self.session.query(Tag).filter(Tag.id == 1).all()
        item_pages = []
        NOT_FOUND = 9999
        for tag in tags:
            for item in tag.items:
                for page in item.pages:
                    if page.lang != self.wiki_db.lang:
                        continue
                    item_pages.append(page)

        joined_ids = ','.join([str(p.page_id) for p in item_pages])
        self.wiki_db.updateQuery('''
            insert into an_pagelinks_picked
            select id_from, id_to, %s as odr from an_pagelinks_multi
            where id_from in (%s) and id_to in (%s)
            ''' % (NOT_FOUND, joined_ids, joined_ids))
        self.wiki_db.commit()

        for p in item_pages:
            p_obj = self.wiki_db.createPageByTitle(p.name, with_info=False)
            pos_to_dest_id = {}
            content = p_obj.text
            logger.debug('page_id = %s', p.page_id)
            for rec in self.wiki_db.selectAndFetchAll('''
                    select id_to from an_pagelinks_picked
                    where id_from = %s
                    ''', (p.page_id,)):
                id_to = rec['id_to']
                rec_to = self.wiki_db.selectOne('''
                    select name from an_page where page_id = %s
                    ''', (id_to,))
                name_to = rec_to['name']
                pos = content.find(' '.join(name_to.split('_')))
                if pos == -1:
                    logger.debug(
                        'Not found link "%s" in page "%s"' % (name_to, p.name))
                else:
                    pos_to_dest_id[pos] = id_to

            for odr, pos in enumerate(sorted(pos_to_dest_id.keys())):
                self.wiki_db.updateQuery('''
                    update an_pagelinks_picked
                    set odr = %s
                    where id_from = %s and id_to = %s
                    ''', (odr, p.page_id, pos_to_dest_id[pos]))

        self.wiki_db.commit()

        self.wiki_db.updateQuery('''
            delete from an_pagelinks_picked
            where odr = %s
            ''', (NOT_FOUND,))
        self.wiki_db.commit()
