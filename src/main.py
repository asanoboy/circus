import MySQLdb
import json

def openConn():
    return MySQLdb.connect(host="127.0.0.1", user="root", passwd="", db="wikidb", charset='utf8')

conn = openConn()
cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)

class PageInfo:
    name = False
    parts = {}
    def __init__(self, text):
        text = text.strip("{}")
        parts = [ part.strip() for part in text.split('|') ]
        header = parts.pop(0)
        if header.count('Infobox '):
            self.name = ' '.join(header.split(' ')[1:])
        elif header.count('Infobox　'):
            self.name = ' '.join(header.split('　')[1:])
        elif header.count('Infobox_'):
            self.name = '_'.join(header.split('_')[1:])
        elif header == 'Infobox':
            self.name = False
        else:
            print(text)
            raise


        self.parts = { elems[0].strip(): elems[1].strip() for elems in \
            [ part.split('=') for part in parts if part.find('=')>=0 ] \
            if len(elems) == 2 }


class Page:
    id = False
    title = ''
    info = False
    def __init__(self, id, title, rawText):
        self.title = title
        self.id = id
        text = Page._removeComment(rawText)
        infoText = Page._findInfobox(text)
        if( infoText ):
            try:
                self.info = PageInfo(infoText)
            except:
                print(rawText)
                raise


    @staticmethod
    def createByTitle(title):
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
        
    @staticmethod
    def _findInfobox(text):
        startPos = text.find("{{Infobox")
        if( startPos == -1 ):
            return False
        currentPos = startPos + 2;
        depth = 0
        while(1):
            startBracketPos = text.find("{{", currentPos)
            endBracketPos = text.find("}}", currentPos)
            if( endBracketPos == -1 ):
                print(text)
                raise

            if( startBracketPos > 0 and startBracketPos < endBracketPos ):
                depth += 1
                currentPos = startBracketPos + 2
            elif( depth==0 ):
                return text[startPos: endBracketPos+2]
            else:
                depth -= 1
                currentPos = endBracketPos + 2
        raise

    @staticmethod
    def _removeComment(text):
        currentPos = 0
        commentRanges = []
        text = text.replace('<-->', '')
        while 1:
            startPos = text.find("<!--", currentPos)
            endPos = text.find("-->", currentPos)
            if startPos == -1 or endPos == -1:
                break
            elif startPos < endPos:
                commentRanges.append((startPos, endPos + 3))
                currentPos = endPos + 3
            elif startPos > endPos:
                commentRanges.append((endPos, endPos + 3))
                currentPos = endPos + 3
            # elif startPos >= 0:
            #     commentRanges.append((startPos, startPos + 3))
            #     currentPos = startPos + 3
            # elif endPos >= 0:
            #     commentRanges.append((endPos, endPos + 4))
            #     currentPos = endPos + 4
            else:
                raise

        textRanges = []
        for i, r in enumerate(commentRanges):
            if( i==0 ):
                textRanges.append((0, r[0]))
            else:
                textRanges.append((commentRanges[i-1][1], r[0]) )

            if( i==len(commentRanges)-1 ):
                textRanges.append((r[1], len(text)))

        result = ''
        for r in textRanges:
            result += text[r[0]: r[1]]

        return result

def selectPages(catTitle, recursive=0, excludeTitles=set()):
#     cur.execute("""
# select * from category where cat_title=%s
# """, (catTitle,))
#     res = cur.fetchall()
#     cat_pages = res[0]['cat_pages']
#     cat_subcats = res[0]['cat_subcats']
    
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

def allPagesGenerator():
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


def selectAllPages():
    cur.execute("""
select p.page_title title from page p 
where page_namespace = 0
""")
    res = cur.fetchall()
    pageTitles = set([record['title'].decode('utf-8') for record in res])
    return pageTitles

pages = {Page.createByTitle(title) for title in allPagesGenerator()}
infopages = [p for p in pages if p.info]

for p in infopages:
    cur.execute("""
insert into anadb.page_ex (page_id, name, infotype, infocontent)
values(%s, %s, %s, %s)
""", (p.id, p.title, p.info.name, json.dumps(p.info.parts)) )

conn.commit()
