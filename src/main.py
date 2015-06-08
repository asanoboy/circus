import MySQLdb
import json
import re, sys, argparse
import time
from circus_itertools import lazy_chunked as chunked
from models import *
from dbutils import *
from consts import valid_infotypes, valid_categories
#from numerical import *
from parser import *
from builders.pagelinks_filtered import PagelinksFilteredBuilder
from builders.pagelinks_featured import PagelinksFeaturedBuilder
from builders.pagelinks import PagelinksBuilder
from builders.page_sync import PageSyncer

class Lap:
    def __init__(self, tag):
        self.start = None
        self.tag = tag

    def __enter__(self):
        self.start = time.time()
        print('[%s]: start' % (self.tag,))
        
    def __exit__(self, exception_type, exception_value, traceback):
        interval = time.time() - self.start
        print('[%s]: elapsed time = %d' % (self.tag, interval))
 
def selectTextByTitle(wiki_db, title, namespace):
    res = wiki_db.selectAndFetchAll(sqlStr("""
        select t.old_text wiki, p.page_id id from page p 
        inner join revision r on r.rev_page = p.page_id
        inner join text t on t.old_id = r.rev_text_id
        where p.page_title = %s and p.page_namespace = %s
        """), (title, namespace))
    if len(res) > 0:
        return res[0]['wiki'].decode('utf-8')
    return False


#def selectPages(catTitle, recursive=0, excludePageTitles=set(), excludeCategoryTitles=set()):
#    cur.execute(sqlStr("""
#        select p.page_title title from categorylinks cl 
#        inner join page p on cl.cl_from = p.page_id
#        where cl.cl_to=%s and cl_type = "page"
#        """), (catTitle,))
#    res = cur.fetchall()
#    pageTitles = set([record['title'].decode('utf-8') for record in res])
#    pageTitles = list(filter(lambda title: title not in excludePageTitles, pageTitles))
#    pages = [createPageByTitle(title) for title in pageTitles]
#
#    if recursive:
#        cur.execute(sqlStr("""
#            select p.page_title title from categorylinks cl 
#            inner join page p on cl.cl_from = p.page_id
#            where cl.cl_to=%s and cl_type = "subcat"
#            """), (catTitle,))
#        titles = set([record['title'].decode('utf-8') for record in cur.fetchall()])
#        joinedExcludeCategoryTitles = excludeCategoryTitles | titles
#        joinedExcludePageTitles = excludePageTitles | pageTitles
#        for title in titles - excludeCategoryTitles:
#            pages += selectPages(title, recursive - 1, joinedExcludePageTitles, joinedExcludeCategoryTitles)
#
#    return pages


def selectAllInfoNames(wiki_db):
    records = wiki_db.selectAndFetchAll(sqlStr("""
        select name from an_info
    """))
    return [record['name'] for record in records]

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

def buildPageEx(wiki_db):
    allowedInfoNames = selectAllInfoNames(wiki_db)
    def createPageInternal(title):
        return wiki_db.createPageByTitle(title, allowedInfoNames)

    pages = map(createPageInternal, wiki_db.allPageTitlesGenerator())
    pages = filter(lambda p: p and p.info, pages)

    for pageList in chunked(pages, 100):
        wiki_db.multiInsert('an_page', ['page_id', 'name', 'contentlength',  'infotype', 'infocontent'], \
            [[p.id, p.title, p.contentlength, p.info.name, json.dumps(p.info.keyValue)] for p in pageList])

    wiki_db.commit()

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
            result = wiki_db.selectAndFetchAll(sqlStr("""
                select text_id from an_info where name = %s
            """), (redirectName,))

            if len(result) == 0:
                result = wiki_db.selectAndFetchAll(sqlStr("""
                    select text_id from an_info where lower(name) = lower(%s)
                """), (redirectName,))
                
            if len(result) == 1:
                redirectTo = result[0]['text_id']
                wiki_db.updateQuery(sqlStr("""
                    update an_info set redirect_to = %s
                    where text_id = %s
                """), (redirectTo, infoRecord['text_id']))
            else:
                print( 'Invalid redirect from %s to %s' % (infoName, redirectName) )

        else:
            if text.lower().startswith('#redirect') :
                msg = 'Missing to parse redirect info "%s".' % (text,)
                print(msg)
                raise Exception(msg)
    wiki_db.commit()

def buildCatInfo(wiki_db):

    catDataIter = wiki_db.allCategoryDataGenerator()
    catIter = filter(lambda x: x, map(createCategoryWithoutStub, catDataIter))
    for cat in catIter:
        records = wiki_db.selectAndFetchAll(sqlStr("""
            select px.infotype, count(*) page_num from categorylinks cl
            inner join an_page px on px.page_id = cl.cl_from
            where cl.cl_type = 'page' and cl.cl_to = %s
            group by px.infotype
            order by px.page_id asc
            """), (cat.name, ))
        valuesList = [[cat.id, record['infotype'], record['page_num']] for record in records]
        if len(valuesList) > 0:
            wiki_db.multiInsert('an_category_info', ['cat_id', 'infotype', 'page_num'], valuesList)

    wiki_db.commit()

def updateFeatured(wiki_db):
    infotypes = valid_infotypes(wiki_db.lang)
    wiki_db.updateQuery(sqlStr("""
        update an_info
        set featured = case when %s then 1 else 0 end
        """ % (' or '.join(['name = %s'] * len(infotypes)), )), \
        set(infotypes))
    wiki_db.commit()

    wiki_db.updateQuery(sqlStr("""
        update an_category_info 
        set featured = case when %s then 1 else 0 end
        """ % (' or '.join(['infotype = %s'] * len(infotypes)), )), \
        set(infotypes))
    wiki_db.commit()

def gen_feature_page_record(wiki_db):
    infotype_feature_to_target = {}
    root_category_name_to_target_infotype = {}
    if wiki_db.lang == 'ja':
        pass
    elif wiki_db.lang == 'en':
        infotype_feature_to_target['Infobox_music_genre'] = 'Infobox_musical_artist'
        root_category_name_to_target_infotype['Music_genres'] = 'Infobox_musical_artist'

    for feature, target in infotype_feature_to_target.items():
        res = wiki_db.selectAndFetchALL("""
            select page_id from an_page 
            where infotype = %s
            """, (feature,), decode=True)
        yield [res['page_id'], target]


def build_feature_page(wiki_db):
    pass

#def updateCategoryRelationsByInfotype(infotype):
#    pageIds = [] 
#    indexOfPageId = {}
#    catIds = []
#    indexOfCatId = {}
#    for cols in allCategoryPageByInfotype(openConn, infotype):
#        catId = cols[0]
#        pageId = cols[1]
#        if pageId not in pageIds:
#            pageIds.append(pageId)
#            indexOfPageId[pageId] = len(pageIds) - 1
#        if catId not in catIds:
#            catIds.append(catId)
#            indexOfCatId[catId] = len(catIds) - 1
#
#    def categoryPageRelationGenerator():
#        currentCatId = None
#        currentRelation = [0] * len(pageIds)
#        for cols in allCategoryPageByInfotype(openConn, infotype):
#            catId = cols[0]
#            pageId = cols[1]
#            if currentCatId is not None and currentCatId != catId:
#                yield currentRelation
#                currentRelation = [0] * len(pageIds)
#                currentCatId = catId
#            currentRelation[indexOfPageId[pageId]] = 1
#        yield currentRelation
#
#    childParent = getCategoryRelationship(categoryPageRelationGenerator(), len(catIds), len(pageIds))
#    return [ (catIds[childCatIndex], catIds[parentCatIndex]) for childCatIndex, parentCatIndex in childParent.items()]

#def updateAllCategoryRelations():
#    return map(updateCategoryRelationsByInfotype, valid_infotypes)


#def maxNodeId():
#    cur.execute(sqlStr('select max(node_id) max from integrated.node'))
#    rs = cur.fetchone()
#    return rs['max'] if rs['max'] is not None else 0
#
#def _buildNodeInternal(generator, table, idCol):
#    nextNodeId = maxNodeId() + 1
#    for records in chunked(generator(openConn), 100):
#        relationValues = []
#        nodeValues = []
#        for cols in records:
#            pageId = cols[0]
#            name = cols[1]
#            relationNodeId = cols[2]
#            nodeId = cols[3]
#            if relationNodeId is None:
#                relationValues.append([pageId, nextNodeId])
#                nodeValues.append([nextNodeId, name])
#                nextNodeId += 1
#            elif nodeId is None:
#                nodeValues.append([relationNodeId, name])
#
#        if len(relationValues):
#            queryMultiInsert(cur, '%s' % (table,), [idCol, 'node_id'], relationValues)
#        if len(nodeValues):
#            queryMultiInsert(cur, 'integrated.node', ['node_id', 'name'], nodeValues)
#
#    conn.commit()
#
#def buildNodeByPage(wiki_db):
#    _buildNodeInternal(wiki_db.allFeaturedPageGenerator, 'an_page_node_relation', 'page_id')
#
#def buildNodeByCategory(wiki_db):
#    _buildNodeInternal(wiki_db.allFeaturedCategoryGenerator, 'an_category_node_relation', 'cat_id')
#
#def pageLinkGeneratorWithSameInfotype(pageIterator):
#    #conn = openConn()
#    #cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
#    for cols in pageIterator:
#        infotype = cols['infotype']
#        pageId = cols['page_id']
#        name = cols['name']
#        cur.execute(sqlStr("""
#            select pl.pl_title name, pr_from.node_id node_from, pr_to.node_id node_to from pagelinks pl
#            inner join page p on pl.pl_title = p.page_title and p.page_namespace = 0
#            inner join an_page px on px.page_id = p.page_id and px.infotype = %s
#            inner join an_page_node_relation pr_from on pr_from.page_id = pl.pl_from
#            inner join an_page_node_relation pr_to on pr_to.page_id = p.page_id
#            where pl.pl_from = %s and pl.pl_namespace = 0
#        """), (infotype, pageId))
#        records = cur.fetchall()
#        content = selectTextByTitle(name, 0)
#        content = removeComment(content)
#        for record in records:
#            namePos = content.find(record['name'].decode('utf-8'))
#            if namePos != -1:
#                length = len(content)
#                weight = (length - namePos) / length
#                yield {'node_id_from': record['node_from'], 'node_id_to': record['node_to'], 'weight': weight}
#    #cur.close()
#    #conn.close()
#
#def buildFeatureNode(wiki_db):
#    pageIter = wiki_db.allFeaturedPageGenerator(dictFormat=True)
#    for i, links in enumerate(chunked(pageLinkGeneratorWithSameInfotype(pageIter), 100)):
#        if i % 1000 == 0:
#            time.sleep(1)
#        queryMultiInsert(cur, 'integrated.feature_node_relation', \
#            ['feature_node_id', 'node_id', 'weight'], \
#            [ [r['node_id_from'], r['node_id_to'], r['weight']] for r in links] )
#    conn.commit()
#    
#def buildTreeNode(clean=False):
#    pass # later

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--langs') # ja,en
    args = parser.parse_args()
    args = vars(args)

    langs = args['langs'].split(',')
    #imported_langs = ['en', 'ja']

    for lang in langs:
        wiki_db = WikiDB(lang)
        with Lap('buildInfo'):
            #buildInfoEx(wiki_db)
            pass

        with Lap('buildPageEx'):
            #buildPageEx(wiki_db)
            pass

        with Lap('Pagelinks'):
            builder = PagelinksBuilder(wiki_db)
            builder.build()
            pass

        with Lap('PagelinksFilteredBuilder'):
            builder = PagelinksFilteredBuilder(wiki_db)
            #builder.build()
            pass

        with Lap('build_page_links_featured'):
            builder = PagelinksFeaturedBuilder(wiki_db)
            builder.build()
            pass
        
        continue

        with Lap('buildCatInfo'):
            #buildCatInfo(wiki_db)
            pass

        with Lap('updateFeatured'):
            #updateFeatured(wiki_db)
            pass

        master_db = MasterWikiDB('wikimaster')
        with Lap('page_sync'):
            syncer = PageSyncer(master_db, wiki_db, imported_langs)
            syncer.sync()
            #sync_master(lang, imported_langs, wiki_db, master_db)
            pass

        #buildNodeByPage(wiki_db)
        #buildNodeByCategory(wiki_db)
        #buildFeatureNode(wiki_db)

    pass
