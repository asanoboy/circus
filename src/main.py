import MySQLdb
import json
from circus_itertools import lazy_chunked as chunked
from itertools import chain
from Page import Page, PageInfo

def openConn():
    return MySQLdb.connect(host="127.0.0.1", user="root", passwd="", db="wikidb", charset='utf8')

conn = openConn()
cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)

class Category:
    id = False
    name = False

    def __init__(self, id, name):
        self.id = id
        self.name = name

def bracketTextGenerator(text):
    currentPos = 0
    startPos = -1
    depth = 0
    nextStartPos = text.find("{{", currentPos)
    nextEndPos = text.find("}}", currentPos)
    try:
        while 1:
            if nextStartPos == -1 and nextEndPos == -1:
                if depth != 0:
                    raise 'Unclosed info bracket.'
                break
            elif nextEndPos == -1: # exists only {{
                raise 'Unclosed info bracket.'
            elif nextStartPos == -1: # exists only }}
                if depth != 1:
                    raise 'Too many "}}" at last'
                    break
                yield text[startPos+2: nextEndPos]
                break
            else: # exists both {{ and }}
                if nextStartPos < nextEndPos:
                    if depth==0:
                        startPos = nextStartPos
                    depth += 1
                    currentPos = nextStartPos + 2
                    nextStartPos = text.find("{{", currentPos)
                else:
                    if depth==0:
                        raise 'Too many "}}"'
                    elif depth==1:
                        if startPos == -1:
                            raise 'Invalid'
                        yield text[startPos+2: nextEndPos]
                        startPos = -1
                    depth -= 1
                    currentPos = nextEndPos + 2
                    nextEndPos = text.find("}}", currentPos)
    except:
        print("Can't parse wiki text.")
        pass # workaround



def createPageInfoByBracketText(text, allowedNames):
    pos = text.find('|')
    if pos == -1: 
        return False

    name = text[:pos].strip().replace(' ', '_') \
        .replace('　', '_') # multibyte space
    
    if allowedNames != False:
        if name not in allowedNames:
            return False

    text = text[pos+1:]
    keyValue = { elems[0].strip(): elems[1].strip() for elems in \
        [ part.split('=') for part in text.split('|') if part.find('=')>=0 ] \
        if len(elems) == 2 }
    return PageInfo(name, keyValue)
    
def createPageInfoByPageWikiText(text, allowedNames):
    bracketTexts = bracketTextGenerator(text)
    infos = [createPageInfoByBracketText(t, allowedNames) for t in bracketTexts]
    infos = [i for i in infos if i]
    if len(infos) == 0:
        return False
    if len(infos) == 1:
        return infos[0]
    else:
        return infos[0] # Apply first info.

def selectTextByTitle(title, namespace):
    cur.execute("""
        select t.old_text wiki, p.page_id id from page p 
        inner join revision r on r.rev_page = p.page_id
        inner join text t on t.old_id = r.rev_text_id
        where p.page_title = %s and p.page_namespace = %s
        """, (title, namespace))
    res = cur.fetchall()
    if len(res) > 0:
        return res[0]['wiki'].decode('utf-8')
    return False

def createPageByTitle(title, allowedInfoNames=False):
    cur.execute("""
        select t.old_text wiki, p.page_id id from page p 
        inner join revision r on r.rev_page = p.page_id
        inner join text t on t.old_id = r.rev_text_id
        where p.page_title = %s and p.page_namespace = 0
        """, (title,))
    res = cur.fetchall()
    if len(res) > 0:
        text = res[0]['wiki'].decode('utf-8')
        info = createPageInfoByPageWikiText(text, allowedInfoNames)
        if not info:
            return False
        return Page(res[0]['id'], title, len(text), info)
    else:
        return False 

def createFunctionPageByTitle(allowedInfoNames):
    def createPageInternal(title):
        return createPageByTitle(title, allowedInfoNames)
    return createPageInternal

def selectPages(catTitle, recursive=0, excludePageTitles=set(), excludeCategoryTitles=set()):
    cur.execute("""
        select p.page_title title from categorylinks cl 
        inner join page p on cl.cl_from = p.page_id
        where cl.cl_to=%s and cl_type = "page"
        """, (catTitle,))
    res = cur.fetchall()
    pageTitles = set([record['title'].decode('utf-8') for record in res])
    pageTitles = list(filter(lambda title: title not in excludePageTitles, pageTitles))
    pages = [createPageByTitle(title) for title in pageTitles]

    if recursive:
        cur.execute("""
            select p.page_title title from categorylinks cl 
            inner join page p on cl.cl_from = p.page_id
            where cl.cl_to=%s and cl_type = "subcat"
            """, (catTitle,))
        titles = set([record['title'].decode('utf-8') for record in cur.fetchall()])
        joinedExcludeCategoryTitles = excludeCategoryTitles | titles
        joinedExcludePageTitles = excludePageTitles | pageTitles
        for title in titles - excludeCategoryTitles:
            pages += selectPages(title, recursive - 1, joinedExcludePageTitles, joinedExcludeCategoryTitles)

    return pages

class DictUseResultCursor(MySQLdb.cursors.CursorUseResultMixIn, \
    MySQLdb.cursors.CursorDictRowsMixIn, \
    MySQLdb.cursors.BaseCursor):
    pass

class TupleUseResultCursor(MySQLdb.cursors.CursorUseResultMixIn, \
    MySQLdb.cursors.CursorTupleRowsMixIn, \
    MySQLdb.cursors.BaseCursor):
    pass

def selectGenerator(table, cols=[], joins=[], cond='', order=''):
    _conn = openConn()
    with _conn:
        _cur = _conn.cursor(cursorclass=TupleUseResultCursor)
        sql = """
            select %s from %s %s
            """ % (','.join(cols), table, ' '.join(joins))
        if cond:
            sql += " where %s " % (cond,)
        if order:
            sql += " order by %s " % (order,)
        _cur.execute(sql)
        cnt = 0
        while 1:
            cnt += 1
            if cnt % 10 == 0:
                print(cnt)

            rt = _cur.fetchone()
            if rt:
                yield list(map(lambda x: x.decode('utf-8') if hasattr(x, 'decode') else x, rt))
            else:
                break
        _cur.close()

def allCategoryGenerator():
    for cols in selectGenerator('category', cols=['cat_id', 'cat_title'], order='cat_id asc'):
        yield Category(cols[0], cols[1])

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

            rt = _cur.fetchone()
            if rt:
                yield rt['title'].decode('utf-8')
            else:
                break
        _cur.close()

def allInfoDataGenerator():
    _conn = openConn()
    with _conn:
        _cur = _conn.cursor(cursorclass=DictUseResultCursor)
        _cur.execute("""
            select p.page_title title, t.old_text text, t.old_id text_id from page p 
            inner join revision r on r.rev_page = p.page_id
            inner join text t on t.old_id = r.rev_text_id
            where page_namespace = 10 order by p.page_title asc
            """)
        cnt = 0
        while 1:
            rt = _cur.fetchone()
            if not rt:
                break
            
            if not rt['title'].decode('utf-8').lower().startswith('infobox') \
                    and rt['text'].decode('utf-8').lower().find('infobox') == -1:
                continue

            cnt += 1
            if cnt % 1000 == 0:
                print(cnt)

            if rt:
                yield (rt['text_id'], rt['title'].decode('utf-8'))
            else:
                break
        _cur.close()

def queryMultiInsert(table, cols, valuesList):
    cur.execute(("""
        insert into %s (%s)
        values
        """ % (table, ','.join(cols))) \
        + ','.join(['(' + ','.join(['%s'] * len(cols)) + ')'] * len(valuesList)), \
        tuple(chain.from_iterable(valuesList)) \
    )
    pass

def selectAllInfoNames():
    cur.execute("""
        select name from anadb.info_ex
    """)
    return [record['name'] for record in cur.fetchall()]

def buildPageEx():
    allowedInfoNames = selectAllInfoNames()
    pages = map(createFunctionPageByTitle(allowedInfoNames), allPageTitlesGenerator())
    infopages = filter(lambda p: p and p.info, pages)

    for pageList in chunked(infopages, 100):
        queryMultiInsert('anadb.page_ex2', ['page_id', 'name', 'contentlength',  'infotype', 'infocontent'], \
            [[p.id, p.title, p.contentlength, p.info.name, json.dumps(p.info.keyValue)] for p in pageList])

    conn.commit()

def buildInfoEx():
    for datas in chunked(allInfoDataGenerator(), 100):
        queryMultiInsert('anadb.info_ex2', ['text_id', 'name'], datas)

    conn.commit()

def buildCatInfo():
    catIter = allCategoryGenerator()
    for cat in catIter:
        cur.execute("""
            select px.infotype, count(*) page_num from categorylinks cl
            inner join anadb.page_ex px on px.page_id = cl.cl_from
            where cl.cl_type = 'page' and cl.cl_to = %s
            group by px.infotype
            order by px.page_id asc
            """, (cat.name, ))
        valuesList = [[cat.id, record['infotype'], record['page_num']] for record in cur.fetchall()]
        if len(valuesList) > 0:
            queryMultiInsert('anadb.category_info', ['cat_id', 'infotype', 'page_num'], valuesList)

    conn.commit()
