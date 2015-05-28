import MySQLdb.cursors
import time
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
    cur.execute('set net_read_timeout = 9999')
    cur.execute('set net_write_timeout = 9999')

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
    last_time = time.time()
    while 1:
        cnt += 1
        if cnt % 1000 == 0:
            now_time = time.time()
            print(cnt, ':%s sec' % (now_time - last_time,))
            last_time = now_time
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

def decode_if_binary(x):
    return x.decode('utf-8') if hasattr(x, 'decode') else x

class BaseDB:
    def __init__(self, dbname):
        self.dbname = dbname
        self.write_conn = self.openConn()
        self.read_conn = self.openConn()

    def openConn(self):
        return MySQLdb.connect(host="127.0.0.1", user="root", passwd="", db=self.dbname, charset='utf8')

    def multiInsert(self, table, cols, valuesList, on_duplicate=None):
        cur = self.write_conn.cursor()
        cur.execute(sqlStr(("""
            insert into %s (%s)
            values
            """ % (table, ','.join(cols))) \
            + ','.join(['(' + ','.join(['%s'] * len(cols)) + ')'] * len(valuesList))) \
            + ( (' on duplicate key update ' + on_duplicate) if on_duplicate is not None else ''), \
            tuple(chain.from_iterable(valuesList)) \
        )
        cur.close()

    def updateQuery(self, query, args=set()):
        cur = self.write_conn.cursor()
        cur.execute(query, args)
        cur.close()

    def selectAndFetchAll(self, query, args=set(), dictFormat=True, decode=False):
        cur = None
        if dictFormat:
            cur = self.read_conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        else:
            cur = self.read_conn.cursor(cursorclass=MySQLdb.cursors.Cursor)

        cur.execute(query, args)
        rt = cur.fetchall()
        if decode:
            if dictFormat:
                rt = [ { key: decode_if_binary(value) for key, value in record.items() } for record in rt]
            else:
                raise Exception('Not support')

        cur.close()
        return rt

    def commit(self):
        print('commit')
        self.write_conn.commit()
        self.read_conn.close()
        self.read_conn = self.openConn()


class WikiDB(BaseDB):
    def __init__(self, lang):
        self.lang = lang
        super().__init__('%swiki' % (lang, ))

    def allCategoryDataGenerator(self, dict_format=False):
        for cols in selectGenerator(self.openConn, 'category c', \
                cols=['cat_id', 'cat_title', 't.old_text'], \
                joins=[ \
                    'inner join page p on p.page_title = c.cat_title and p.page_namespace = 14', \
                    'inner join revision r on r.rev_page = p.page_id', \
                    'inner join text t on t.old_id = r.rev_text_id', \
                ], \
                order='cat_id asc'):
            if dict_format:
                yield {'cat_id': cols[0], 'title': cols[1], 'text': cols[2]}
            else:
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

    def allFeaturedPageGenerator(self, dictFormat=False, featured=True):
        for cols in selectGenerator(self.openConn, 'an_page p', \
                cols=['p.page_id', 'p.name', 'p.infotype', 'p.infocontent'], \
                joins=[\
                    'inner join an_info i on i.name = p.infotype', \
                ], \
                cond= 'i.featured = 1' if featured else None, \
                order='p.page_id asc'):
            if dictFormat:
                yield dict(zip(['page_id', 'name', 'infotype', 'infocontent'], cols))
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

    def createPageByTitle(self, title, allowedInfoNames=False, namespace=0, with_info=True):
        res = self.selectAndFetchAll(sqlStr("""
            select t.old_text wiki, p.page_id id from page p 
            left join revision r on r.rev_page = p.page_id
            left join text t on t.old_id = r.rev_text_id
            where p.page_title = %s and p.page_namespace = %s
            """), (title, namespace))
        if len(res) > 0:
            text = res[0]['wiki'].decode('utf-8')
            text = removeComment(text)
            info = None
            if with_info:
                info = self._createPageInfoByPageWikiText(text, allowedInfoNames)
                if not info:
                    return False
            return Page(res[0]['id'], title, text, info)
        else:
            return False 

#    def other_lang_page_infos_generator(self, page_id):
#            rs = self.selectAndFetchAll("""
#                select ll_from orig_id, ll_title title, ll_lang lang from langlinks
#                where ll_from = %s
#            """, (page_id, ))
#            return rs

class MasterWikiDB(BaseDB):
    def __init__(self, dbname):
        super().__init__(dbname)

    def missing_page_generator(self, lang, page_iter):
        for pages in chunked(page_iter, 10):
            records = self.selectAndFetchAll("""
                select lang_page_id from page_lang_relation
                where lang = %s and lang_page_id in (
                """ + ','.join([str(page['page_id']) for page in pages]) + ')', \
                (lang, ) )
            found_ids = [r['lang_page_id'] for r in records]
            for page in pages:
                if page['page_id'] not in found_ids:
                    yield page

    #def build_missing_page_relation(self, lang, page_id_iter):
    #    for missing_page_ids in \
    #            chunked(self.missing_page_ids_generator(lang, page_id_iter), 100):
    #        self.multiInsert('page_lang_relation', \
    #                ['lang', 'lang_page_id'], \
    #                [[lang, page_id] for page_id in missing_page_ids] )




