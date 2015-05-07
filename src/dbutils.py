import MySQLdb.cursors
from itertools import chain
from Page import *

class DictUseResultCursor(MySQLdb.cursors.CursorUseResultMixIn, \
    MySQLdb.cursors.CursorDictRowsMixIn, \
    MySQLdb.cursors.BaseCursor):
    pass

class TupleUseResultCursor(MySQLdb.cursors.CursorUseResultMixIn, \
    MySQLdb.cursors.CursorTupleRowsMixIn, \
    MySQLdb.cursors.BaseCursor):
    pass

def selectGenerator(openConn, table, cols=[], joins=[], cond='', order=''):
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
            if cnt % 100 == 0:
                print(cnt)
                pass

            rt = _cur.fetchone()
            if rt:
                yield list(map(lambda x: x.decode('utf-8') if hasattr(x, 'decode') else x, rt))
            else:
                break

        _cur.close()

def allCategoryGenerator(openConn):
    for cols in selectGenerator(openConn, 'category', cols=['cat_id', 'cat_title'], order='cat_id asc'):
        yield Category(cols[0], cols[1])

def allPageTitlesGenerator(openConn):
    for cols in selectGenerator(openConn, 'page', cols=['page_title'], cond='page_namespace = 0', \
            order='page_title asc'):
        yield cols[0]

def allInfoDataGenerator(openConn):
    for cols in selectGenerator(openConn, 'page p', \
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

def queryMultiInsert(cur, table, cols, valuesList):
    cur.execute(("""
        insert into %s (%s)
        values
        """ % (table, ','.join(cols))) \
        + ','.join(['(' + ','.join(['%s'] * len(cols)) + ')'] * len(valuesList)), \
        tuple(chain.from_iterable(valuesList)) \
    )
    pass
