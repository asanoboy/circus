import MySQLdb
import json
from more_itertools import chunked
from itertools import chain
from Page import Page

def openConn():
    return MySQLdb.connect(host="127.0.0.1", user="root", passwd="", db="wikidb", charset='utf8')

conn = openConn()
cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)


def createPageByTitle(title):
    cur.execute("""
select t.old_text wiki, p.page_id id from page p 
inner join revision r on r.rev_page = p.page_id
inner join text t on t.old_id = r.rev_text_id
where p.page_title = %s and p.page_namespace = 0
""", (title,))
    res = cur.fetchall()
    if len(res) > 0:
        return Page(res[0]['id'], title, res[0]['wiki'].decode('utf-8'))
    else:
        return False 



def selectPages(catTitle, recursive=0, excludeTitles=set()):
    cur.execute("""
select p.page_title title from categorylinks cl 
inner join page p on cl.cl_from = p.page_id
where cl.cl_to=%s and cl_type = "page"
""", (catTitle,))
    res = cur.fetchall()
    pageTitles = set([record['title'].decode('utf-8') for record in res])

    if recursive:
        cur.execute("""
select p.page_title title from categorylinks cl 
inner join page p on cl.cl_from = p.page_id
where cl.cl_to=%s and cl_type = "subcat"
""", (catTitle,))
        titles = set([record['title'].decode('utf-8') for record in cur.fetchall()])
        joinedExcludeTitles = excludeTitles | titles;
        for title in titles - excludeTitles:
            pageTitles |= selectPages(title, recursive - 1, joinedExcludeTitles)

    return set(pageTitles)

class DictUseResultCursor(MySQLdb.cursors.CursorUseResultMixIn, \
    MySQLdb.cursors.CursorDictRowsMixIn, \
    MySQLdb.cursors.BaseCursor):
    pass

def allPageTitlesGenerator():
    _conn = openConn()
    with _conn:
        _cur = _conn.cursor(cursorclass=DictUseResultCursor)
        _cur.execute("""
    select p.page_title title from page p 
    where page_namespace = 0 order by page_title asc
    """)
        cnt = 0
        while 1:
            cnt += 1
            if cnt % 1000 == 0:
                print(cnt)
                continue

            rt = _cur.fetchone()
            if rt:
                yield rt['title'].decode('utf-8')
            else:
                break
        _cur.close()


if 1:
    pages = {createPageByTitle(title) for title in allPageTitlesGenerator()}
    infopages = [p for p in pages if p.info]

    for pageList in chunked(infopages, 100):
        cur.execute("""
insert into anadb.page_ex (page_id, name, infotype, infocontent)
values
""" + ','.join(['(%s, %s, %s, %s)'] * len(pageList)), \
tuple(chain.from_iterable([[p.id, p.title, p.info.name, json.dumps(p.info.parts)] for p in pageList])))

    conn.commit()
