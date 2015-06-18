import re
from circus_itertools import lazy_chunked as chunked


class InfoBuilder:
    def __init__(self, wiki_db):
        self.wiki_db = wiki_db

    def build(self):
        buildInfoEx(self.wiki_db)


def selectTextByTitle(wiki_db, title, namespace):
    res = wiki_db.selectAndFetchAll("""
        select t.old_text wiki, p.page_id id from page p
        inner join revision r on r.rev_page = p.page_id
        inner join text t on t.old_id = r.rev_text_id
        where p.page_title = %s and p.page_namespace = %s
        """), (title, namespace)
    if len(res) > 0:
        return res[0]['wiki'].decode('utf-8')
    return False


def buildInfoEx(wiki_db):
    for datas in chunked(wiki_db.allInfoDataGenerator(), 100):
        wiki_db.multiInsert('an_info', ['text_id', 'name'], datas)

    wiki_db.commit()
    updateInfoRedirect(wiki_db)


def updateInfoRedirect(wiki_db):
    infoRecordIter = wiki_db.allInfoRecordGenerator()
    p = re.compile('#redirect:?\s*\[\[(template:)?\s*(.+)\]\]', re.IGNORECASE)
    for infoRecord in infoRecordIter:
        infoName = infoRecord['name']
        text = selectTextByTitle(wiki_db, infoName, 10)
        m = p.search(text)
        if m:
            redirectName = m.group(2).replace(' ', '_')
            result = wiki_db.selectAndFetchAll("""
                select text_id from an_info where name = %s
            """, (redirectName,))

            if len(result) == 0:
                result = wiki_db.selectAndFetchAll("""
                    select text_id from an_info where lower(name) = lower(%s)
                """, (redirectName,))

            if len(result) == 1:
                redirectTo = result[0]['text_id']
                wiki_db.updateQuery("""
                    update an_info set redirect_to = %s
                    where text_id = %s
                """, (redirectTo, infoRecord['text_id']))
            else:
                print(
                    'Invalid redirect from %s to %s' %
                    (infoName, redirectName))

        else:
            if text.lower().startswith('#redirect'):
                msg = 'Missing to parse redirect info "%s".' % (text,)
                print(msg)
                raise Exception(msg)
    wiki_db.commit()
