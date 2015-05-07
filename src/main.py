import MySQLdb
import json
from circus_itertools import lazy_chunked as chunked
from Page import Page, PageInfo
from dbutils import *

def openConn():
    return MySQLdb.connect(host="127.0.0.1", user="root", passwd="", db="wikidb", charset='utf8')

conn = openConn()
cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)

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
        info = infos[0]
    else:
        info = infos[0] # Apply first info.

    while 1:
        cur.execute("""
            select ito.name from anadb.info_ex ifrom
            inner join anadb.info_ex ito on ifrom.redirect_to = ito.text_id
            where ifrom.name = %s
        """, (info.name,))
        res = cur.fetchall()
        if len(res) == 1:
            info.name = res[0]['name']
            continue
        else:
            break
    return info

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

def selectAllInfoNames():
    cur.execute("""
        select name from anadb.info_ex
    """)
    return [record['name'] for record in cur.fetchall()]

def buildPageEx():
    allowedInfoNames = selectAllInfoNames()
    pages = map(createFunctionPageByTitle(allowedInfoNames), allPageTitlesGenerator(openConn))
    pages = filter(lambda p: p and p.info, pages)

    for pageList in chunked(pages, 100):
        queryMultiInsert(cur, 'anadb.page_ex', ['page_id', 'name', 'contentlength',  'infotype', 'infocontent'], \
            [[p.id, p.title, p.contentlength, p.info.name, json.dumps(p.info.keyValue)] for p in pageList])

    conn.commit()

def buildInfoEx():
    for datas in chunked(allInfoDataGenerator(openConn), 100):
        queryMultiInsert(cur, 'anadb.info_ex', ['text_id', 'name'], datas)

    conn.commit()

def buildCatInfo():
    catIter = allCategoryGenerator(openConn)
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
            queryMultiInsert(cur, 'anadb.category_info', ['cat_id', 'infotype', 'page_num'], valuesList)

    conn.commit()


