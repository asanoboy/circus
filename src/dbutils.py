import MySQLdb.cursors
from itertools import chain
from models import Page, createPageInfoByBracketText
from parser import getBracketTexts, removeComment
from circus_itertools import lazy_chunked as chunked

class TableIndex:
    def __init__(self, name, isUnique):
        self.type = type
        self.name = name
        self.isUnique = isUnique
        self.cols = []
        self.isPrimary = name.lower() == 'primary'

    def addCol(self, col):
        self.cols.append(col)

class TableIndexHolder:
    def __init__(self, openConn, table, indexList):
        self.openConn = openConn 
        self.indexList = indexList
        self.table = table

    @classmethod
    def open(cls, openConn, table):
        conn = openConn()
        cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        cur.execute("""
            show index from %s
        """ % (table, ))
        records = cur.fetchall()
        nameToIndex = {}
        for r in sorted(records, key=lambda x: x['Seq_in_index']):
            name = r['Key_name']
            if name not in nameToIndex:
                nameToIndex[name] = TableIndex(name, r['Non_unique'] == 0)
            nameToIndex[name].addCol(r['Column_name'])
        for index in nameToIndex.values():
            key = None
            if index.isPrimary:
                continue
                cur.execute("""
                    alter table %s drop primary key
                """ % (table))
            else:
                cur.execute("""
                    alter table %s drop index %s
                """ % (table, index.name))

        conn.commit()
        cur.close()
        conn.close()

        return cls(openConn, table, list(nameToIndex.values()))

    def close(self):
        conn = self.openConn()
        cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        for index in self.indexList:
            key = None
            if index.isPrimary:
                continue
                key = 'primary key'
            elif index.isUnique:
                key = 'index' # Because unique key is not necessary.
            else:
                key = 'index'

            cur.execute("""
                alter table %s add %s(%s)
            """ % (self.table, key, ', '.join(index.cols)))
        conn.commit()
        cur.close()
        conn.close()

def sqlStr(text):
    return text

class DictUseResultCursor(MySQLdb.cursors.CursorUseResultMixIn, \
    MySQLdb.cursors.CursorDictRowsMixIn, \
    MySQLdb.cursors.BaseCursor):
    pass

class TupleUseResultCursor(MySQLdb.cursors.CursorUseResultMixIn, \
    MySQLdb.cursors.CursorTupleRowsMixIn, \
    MySQLdb.cursors.BaseCursor):
    pass

def selectGenerator(openConn, table, cols=[], joins=[], cond='', order='', arg=set()):
    conn = openConn()
    cur = conn.cursor(cursorclass=TupleUseResultCursor)
    sql = """
        select %s from %s %s
        """ % (','.join(cols), table, ' '.join(joins))
    if cond:
        sql += " where %s " % (cond,)
    if order:
        sql += " order by %s " % (order,)

    print(sql)
    cur.execute(sqlStr(sql), arg)
    cnt = 0
    while 1:
        cnt += 1
        if cnt % 100 == 0:
            #print(cnt)
            pass

        rt = cur.fetchone()
        if rt:
            yield list(map(lambda x: x.decode('utf-8') if hasattr(x, 'decode') else x, rt))
        else:
            break

    cur.close()
    conn.close()

#def queryMultiInsert(cur, table, cols, valuesList):
#    cur.execute(sqlStr(("""
#        insert into %s (%s)
#        values
#        """ % (table, ','.join(cols))) \
#        + ','.join(['(' + ','.join(['%s'] * len(cols)) + ')'] * len(valuesList))), \
#        tuple(chain.from_iterable(valuesList)) \
#    )
#    pass

class BaseDB:
    def __init__(self, dbname):
        self.dbname = dbname
        self.write_conn = self.openConn()
        self.read_conn = self.openConn()

    def openConn(self):
        return MySQLdb.connect(host="127.0.0.1", user="root", passwd="", db=self.dbname, charset='utf8')

    def multiInsert(self, table, cols, valuesList):
        cur = self.write_conn.cursor()
        cur.execute(sqlStr(("""
            insert into %s (%s)
            values
            """ % (table, ','.join(cols))) \
            + ','.join(['(' + ','.join(['%s'] * len(cols)) + ')'] * len(valuesList))), \
            tuple(chain.from_iterable(valuesList)) \
        )
        cur.close()

    def updateQuery(self, query, args=set()):
        cur = self.write_conn.cursor()
        cur.execute(query, args)
        cur.close()

    def selectAndFetchAll(self, query, args=set(), dictFormat=True):
        cur = None
        if dictFormat:
            cur = self.read_conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        else:
            cur = self.read_conn.cursor(cursorclass=MySQLdb.cursors.Cursor)

        cur.execute(query, args)
        rt = cur.fetchall()
        cur.close()
        return rt

    def commit(self):
        print('commit')
        self.write_conn.commit()
        self.read_conn.close()
        self.read_conn = self.openConn()


class WikiDB(BaseDB):
    def __init__(self, dbname):
        super().__init__(dbname)

    def allCategoryDataGenerator(self):
        for cols in selectGenerator(self.openConn, 'category c', \
                cols=['cat_id', 'cat_title', 't.old_text'], \
                joins=[ \
                    'inner join page p on p.page_title = c.cat_title and p.page_namespace = 14', \
                    'inner join revision r on r.rev_page = p.page_id', \
                    'inner join text t on t.old_id = r.rev_text_id', \
                ], \
                order='cat_id asc'):
            yield (cols[0], cols[1], cols[2])

    def allPageTitlesGenerator(self):
        for cols in selectGenerator(self.openConn, 'page', cols=['page_title'], cond='page_namespace = 0', \
                order='page_title asc'):
            yield cols[0]

    def allInfoDataGenerator(self):
        for cols in selectGenerator(self.openConn, 'page p', \
                joins=[\
                    'inner join revision r on r.rev_page = p.page_id', \
                    'inner join text t on t.old_id = r.rev_text_id' \
                ], \
                cols=['p.page_title', 't.old_text','t.old_id'], \
                cond='page_namespace = 10', \
                order='p.page_title asc'):

            if not cols[0].lower().startswith('infobox') \
                    and cols[1].lower().find('infobox') == -1:
                continue
            yield (cols[2], cols[0])

    def allInfoRecordGenerator(self):
        for cols in selectGenerator(self.openConn, 'an_info', cols=['text_id', 'name'], order='text_id asc'):
            yield {'text_id':cols[0], 'name':cols[1]}

    def allFeaturedPageGenerator(self, dictFormat=False):
        for cols in selectGenerator(self.openConn, 'an_page p', \
                cols=['p.page_id', 'p.name', 'pr.node_id', 'n.node_id', 'p.infotype'], \
                joins=[\
                    'inner join an_info i on i.name = p.infotype', \
                    'left join an_page_node_relation pr on pr.page_id = p.page_id', \
                    'left join integrated.node n on n.node_id = pr.node_id', \
                ], \
                cond='i.featured = 1', \
                order='p.page_id asc'):
            if dictFormat:
                yield dict(zip(['page_id', 'name', 'relation_ndoe_id', 'node_id', 'infotype'], cols))
            else:
                yield cols

    def allFeaturedCategoryGenerator(self):
        for cols in selectGenerator(self.openConn, 'an_category_info ci', \
                cols=['ci.cat_id', 'c.cat_title', 'cr.node_id', 'n.node_id'], \
                joins=[\
                    'inner join category c on c.cat_id = ci.cat_id', \
                    'left join an_category_node_relation cr on cr.cat_id = ci.cat_id', \
                    'left join integrated.node n on n.node_id = cr.node_id', \
                ], \
                cond='ci.featured = 1', \
                order='ci.cat_id asc'):
            yield cols

    def allCategoryPageByInfotype(self, infotype):
        for cols in selectGenerator(self.openConn, 'an_page p', \
                cols=['c.cat_id', 'p.page_id'], \
                joins=[\
                    'inner join categorylinks cl on cl.cl_from = p.page_id', \
                    'inner join category c on c.cat_title = cl.cl_to', \
                    'inner join an_category_info ci on ci.cat_id = c.cat_id and ci.infotype = p.infotype', \
                ],\
                cond='ci.featured = 1 and p.infotype = %s',\
                order='c.cat_id asc, p.page_id asc', arg=(infotype,)):
            yield cols

    def _createPageInfoByPageWikiText(self, text, allowedNames):
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
            res = self.selectAndFetchAll(sqlStr("""
                select ito.name from an_info ifrom
                inner join an_info ito on ifrom.redirect_to = ito.text_id
                where ifrom.name = %s
            """), (info.name,))
            if len(res) == 1:
                info.name = res[0]['name']
                continue
            else:
                break
        return info

    def createPageByTitle(self, title, allowedInfoNames=False):
        res = self.selectAndFetchAll(sqlStr("""
            select t.old_text wiki, p.page_id id from page p 
            inner join revision r on r.rev_page = p.page_id
            inner join text t on t.old_id = r.rev_text_id
            where p.page_title = %s and p.page_namespace = 0
            """), (title,))
        if len(res) > 0:
            text = res[0]['wiki'].decode('utf-8')
            text = removeComment(text)
            info = self._createPageInfoByPageWikiText(text, allowedInfoNames)
            if not info:
                return False
            return Page(res[0]['id'], title, len(text), info)
        else:
            return False 

class MasterWikiDB(BaseDB):
    def __init__(self, dbname):
        super().__init__(dbname)

    def _generate_missing_page_ids(self, lang, page_id_iter):
        for page_ids in chunked(page_id_iter, 10):
            records = self.selectAndFetchAll("""
                select lang_page_id from page_lang_relation
                where lang = %s and lang_page_id in (
                """ + ','.join([str(d) for d in page_ids]) + ')', \
                (lang, ) )
            found_ids = [r['lang_page_id'] for r in records]
            for page_id in page_ids:
                if page_id not in found_ids:
                    yield page_id

    def build_missing_page_relation(self, lang, page_id_iter):
        for missing_page_ids in \
                chunked(self._generate_missing_page_ids(lang, page_id_iter), 100):
            self.multiInsert('page_lang_relation', \
                    ['lang', 'lang_page_id'], \
                    [[lang, page_id] for page_id in missing_page_ids] )




