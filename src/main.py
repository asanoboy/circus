import MySQLdb
import json
import re
from circus_itertools import lazy_chunked as chunked
from models import *
from dbutils import *
from consts import valid_infotypes, valid_categories
from numerical import *
from parser import *

def openConn():
    #return MySQLdb.connect(host="127.0.0.1", user="root", passwd="", db="anadb2", charset='utf8')
    return MySQLdb.connect(host="127.0.0.1", user="root", passwd="", db="enwikidb", charset='utf8')

conn = openConn()
cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)

def createPageInfoByBracketText(text, allowedNames=False):
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
    bracketTexts = getBracketTexts(text)
    infos = [createPageInfoByBracketText(t, allowedNames) for t in bracketTexts]
    infos = [i for i in infos if i]
    if len(infos) == 0:
        return False
    if len(infos) == 1:
        info = infos[0]
    else:
        info = infos[0] # Apply first info.

    while 1:
        cur.execute(sqlStr("""
            select ito.name from info_ex ifrom
            inner join info_ex ito on ifrom.redirect_to = ito.text_id
            where ifrom.name = %s
        """), (info.name,))
        res = cur.fetchall()
        if len(res) == 1:
            info.name = res[0]['name']
            continue
        else:
            break
    return info

def selectTextByTitle(title, namespace):
    cur.execute(sqlStr("""
        select t.old_text wiki, p.page_id id from wikidb.page p 
        inner join wikidb.revision r on r.rev_page = p.page_id
        inner join wikidb.text t on t.old_id = r.rev_text_id
        where p.page_title = %s and p.page_namespace = %s
        """), (title, namespace))
    res = cur.fetchall()
    if len(res) > 0:
        return res[0]['wiki'].decode('utf-8')
    return False

def createPageByTitle(title, allowedInfoNames=False):
    cur.execute(sqlStr("""
        select t.old_text wiki, p.page_id id from wikidb.page p 
        inner join wikidb.revision r on r.rev_page = p.page_id
        inner join wikidb.text t on t.old_id = r.rev_text_id
        where p.page_title = %s and p.page_namespace = 0
        """), (title,))
    res = cur.fetchall()
    if len(res) > 0:
        text = res[0]['wiki'].decode('utf-8')
        text = removeComment(text)
        info = createPageInfoByPageWikiText(text, allowedInfoNames)
        if not info:
            return False
        return Page(res[0]['id'], title, len(text), info)
    else:
        return False 

def selectPages(catTitle, recursive=0, excludePageTitles=set(), excludeCategoryTitles=set()):
    cur.execute(sqlStr("""
        select p.page_title title from wikidb.categorylinks cl 
        inner join wikidb.page p on cl.cl_from = p.page_id
        where cl.cl_to=%s and cl_type = "page"
        """), (catTitle,))
    res = cur.fetchall()
    pageTitles = set([record['title'].decode('utf-8') for record in res])
    pageTitles = list(filter(lambda title: title not in excludePageTitles, pageTitles))
    pages = [createPageByTitle(title) for title in pageTitles]

    if recursive:
        cur.execute(sqlStr("""
            select p.page_title title from wikidb.categorylinks cl 
            inner join wikidb.page p on cl.cl_from = p.page_id
            where cl.cl_to=%s and cl_type = "subcat"
            """), (catTitle,))
        titles = set([record['title'].decode('utf-8') for record in cur.fetchall()])
        joinedExcludeCategoryTitles = excludeCategoryTitles | titles
        joinedExcludePageTitles = excludePageTitles | pageTitles
        for title in titles - excludeCategoryTitles:
            pages += selectPages(title, recursive - 1, joinedExcludePageTitles, joinedExcludeCategoryTitles)

    return pages

def selectAllInfoNames():
    cur.execute(sqlStr("""
        select name from info_ex
    """))
    return [record['name'] for record in cur.fetchall()]

def createCategoryWithoutStub(data):
    id = data[0]
    title = data[1]
    text = data[2]
    bracketTexts = getBracketTexts(text)
    infos = [createPageInfoByBracketText(t) for t in bracketTexts]
    infos = [i for i in infos if i]
    if len(infos)>0 and infos[0].name.lower().find('stub') != -1:
        return False
    elif text.find('__HIDDENCAT__') != -1 or text.lower().find('hiddencat}}') != -1:
        return False
    else:
        return Category(id, title)

def buildPageEx():
    def createFunctionPageByTitle(allowedInfoNames):
        def createPageInternal(title):
            return createPageByTitle(title, allowedInfoNames)
        return createPageInternal

    allowedInfoNames = selectAllInfoNames()
    pages = map(createFunctionPageByTitle(allowedInfoNames), allPageTitlesGenerator(openConn))
    pages = filter(lambda p: p and p.info, pages)

    for pageList in chunked(pages, 100):
        queryMultiInsert(cur, 'page_ex', ['page_id', 'name', 'contentlength',  'infotype', 'infocontent'], \
            [[p.id, p.title, p.contentlength, p.info.name, json.dumps(p.info.keyValue)] for p in pageList])

    conn.commit()

def buildInfoEx():
    for datas in chunked(allInfoDataGenerator(openConn), 100):
        queryMultiInsert(cur, 'info_ex', ['text_id', 'name'], datas)

    conn.commit()
    updateInfoFeatured()
    updateInfoRedirect()

def updateInfoFeatured():
    cur.execute(sqlStr("""
        update info_ex
        set featured = case when %s then 1 else 0 end
        """ % (' or '.join(['name = %s'] * len(valid_infotypes)), )), \
        set(valid_infotypes))
    conn.commit()

def updateInfoRedirect():
    infoRecordIter = allInfoRecordGenerator(openConn)
    p = re.compile('#redirect\s*\[\[(template:)?\s*(.+)\]\]', re.IGNORECASE)
    for infoRecord in infoRecordIter:
        infoName = infoRecord['name']
        text = selectTextByTitle(infoName, 10)
        m = p.search(text)
        if m:
            redirectName = m.group(2).replace(' ', '_')
            cur.execute(sqlStr("""
                select text_id from info_ex where name = %s
            """), (redirectName,))
            result = cur.fetchall()

            if len(result) == 0:
                cur.execute(sqlStr("""
                    select text_id from info_ex where lower(name) = lower(%s)
                """), (redirectName,))
                result = cur.fetchall()
                
            if len(result) == 1:
                redirectTo = result[0]['text_id']
                cur.execute(sqlStr("""
                    update info_ex set redirect_to = %s
                    where text_id = %s
                """), (redirectTo, infoRecord['text_id']))
            else:
                print( 'Invalid redirect from %s to %s' % (infoName, redirectName) )

        else:
            if text.lower().startswith('#redirect') :
                msg = 'Missing to parse redirect info "%s".' % (text,)
                print(msg)
                raise Exception(msg)
    conn.commit()

def buildCatInfo(table='category_info'):
    catDataIter = allCategoryDataGenerator(openConn)
    catIter = filter(lambda x: x, map(createCategoryWithoutStub, catDataIter))
    for cat in catIter:
        cur.execute(sqlStr("""
            select px.infotype, count(*) page_num from wikidb.categorylinks cl
            inner join page_ex px on px.page_id = cl.cl_from
            where cl.cl_type = 'page' and cl.cl_to = %s
            group by px.infotype
            order by px.page_id asc
            """), (cat.name, ))
        valuesList = [[cat.id, record['infotype'], record['page_num']] for record in cur.fetchall()]
        if len(valuesList) > 0:
            queryMultiInsert(cur, table, ['cat_id', 'infotype', 'page_num'], valuesList)

    conn.commit()
    updateCatInfoFeatured(table)

def updateCatInfoFeatured(table='category_info'):
    cur.execute(sqlStr("""
        update %s
        set featured = case when %s then 1 else 0 end
        """ % (table, ' or '.join(['infotype = %s'] * len(valid_infotypes)), )), \
        set(valid_infotypes))
    conn.commit()

def updateCategoryRelationsByInfotype(infotype):
    pageIds = [] 
    indexOfPageId = {}
    catIds = []
    indexOfCatId = {}
    for cols in allCategoryPageByInfotype(openConn, infotype):
        catId = cols[0]
        pageId = cols[1]
        if pageId not in pageIds:
            pageIds.append(pageId)
            indexOfPageId[pageId] = len(pageIds) - 1
        if catId not in catIds:
            catIds.append(catId)
            indexOfCatId[catId] = len(catIds) - 1

    def categoryPageRelationGenerator():
        currentCatId = None
        currentRelation = [0] * len(pageIds)
        for cols in allCategoryPageByInfotype(openConn, infotype):
            catId = cols[0]
            pageId = cols[1]
            if currentCatId is not None and currentCatId != catId:
                yield currentRelation
                currentRelation = [0] * len(pageIds)
                currentCatId = catId
            currentRelation[indexOfPageId[pageId]] = 1
        yield currentRelation

    childParent = getCategoryRelationship(categoryPageRelationGenerator(), len(catIds), len(pageIds))
    return [ (catIds[childCatIndex], catIds[parentCatIndex]) for childCatIndex, parentCatIndex in childParent.items()]

def updateAllCategoryRelations():
    return map(updateCategoryRelationsByInfotype, valid_infotypes)

def maxNodeId():
    cur.execute(sqlStr('select max(node_id) max from node'))
    rs = cur.fetchone()
    return rs['max'] if rs['max'] is not None else 0

def _buildNodeInternal(generator, table, idCol):
    nextNodeId = maxNodeId() + 1
    for records in chunked(generator(openConn), 100):
        relationValues = []
        nodeValues = []
        for cols in records:
            pageId = cols[0]
            name = cols[1]
            relationNodeId = cols[2]
            nodeId = cols[3]
            if relationNodeId is None:
                relationValues.append([pageId, nextNodeId])
                nodeValues.append([nextNodeId, name])
                nextNodeId += 1
            elif nodeId is None:
                nodeValues.append([relationNodeId, name])

        if len(relationValues):
            queryMultiInsert(cur, '%s' % (table,), [idCol, 'node_id'], relationValues)
        if len(nodeValues):
            queryMultiInsert(cur, 'node', ['node_id', 'name'], nodeValues)

    conn.commit()

def buildNodeByPage():
    _buildNodeInternal(allFeaturedPageGenerator, 'page_node_relation', 'page_id')

def buildNodeByCategory():
    _buildNodeInternal(allFeaturedCategoryGenerator, 'category_node_relation', 'cat_id')

def pageLinkGeneratorWithSameInfotype(pageIterator):
    for cols in pageIterator:
        infotype = cols['infotype']
        pageId = cols['page_id']
        name = cols['name']
        cur.execute(sqlStr("""
            select pl.pl_title name, pr_from.node_id node_from, pr_to.node_id node_to from wikidb.pagelinks pl
            inner join wikidb.page p on pl.pl_title = p.page_title and p.page_namespace = 0
            inner join page_ex px on px.page_id = p.page_id and px.infotype = %s
            inner join page_node_relation pr_from on pr_from.page_id = pl.pl_from
            inner join page_node_relation pr_to on pr_to.page_id = p.page_id
            where pl.pl_namespace = 0 and pl.pl_from = %s
        """), (infotype, pageId))
        records = cur.fetchall()
        content = selectTextByTitle(name, 0)
        content = removeComment(content)
        for record in records:
            namePos = content.find(record['name'].decode('utf-8'))
            if namePos != -1:
                length = len(content)
                weight = (length - namePos) / length
                yield {'node_id_from': record['node_from'], 'node_id_to': record['node_to'], 'weight': weight}

def buildFeatureNode():
    pageIter = allFeaturedPageGenerator(openConn, dictFormat=True)
    for i, links in enumerate(chunked(pageLinkGeneratorWithSameInfotype(pageIter), 100)):
        queryMultiInsert(cur, 'feature_node_relation', \
            ['feature_node_id', 'node_id', 'weight'], \
            [ [r['node_id_from'], r['node_id_to'], r['weight']] for r in links] )
    conn.commit()
    
def buildTreeNode(clean=False):
    pass # later

if __name__ == '__main__':
    #buildInfoEx()
    #buildCatInfo()
    #buildPageEx()
    #buildNodeByPage()
    #buildNodeByCategory()
    buildFeatureNode()

    pass
