import MySQLdb.cursors
from itertools import chain

class DictUseResultCursor(MySQLdb.cursors.CursorUseResultMixIn, \
    MySQLdb.cursors.CursorDictRowsMixIn, \
    MySQLdb.cursors.BaseCursor):
    pass

class TupleUseResultCursor(MySQLdb.cursors.CursorUseResultMixIn, \
    MySQLdb.cursors.CursorTupleRowsMixIn, \
    MySQLdb.cursors.BaseCursor):
    pass

def selectGenerator(openConn, table, cols=[], joins=[], cond='', order='', arg=set()):
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

        print(sql)
        _cur.execute(sql, arg)
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

def allCategoryDataGenerator(openConn):
    for cols in selectGenerator(openConn, 'category c', \
            cols=['cat_id', 'cat_title', 't.old_text'], \
            joins=[ \
                'inner join page p on p.page_title = c.cat_title and p.page_namespace = 14', \
                'inner join revision r on r.rev_page = p.page_id', \
                'inner join text t on t.old_id = r.rev_text_id', \
            ], \
            order='cat_id asc'):
        yield (cols[0], cols[1], cols[2])

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

def allInfoRecordGenerator(openConn):
    for cols in selectGenerator(openConn, 'anadb.info_ex', cols=['text_id', 'name'], order='text_id asc'):
        yield {'text_id':cols[0], 'name':cols[1]}

def allFeaturedPageGenerator(openConn, dictFormat=False):
    for cols in selectGenerator(openConn, 'anadb.page_ex p', \
            cols=['p.page_id', 'p.name', 'pr.node_id', 'n.node_id', 'p.infotype'], \
            joins=[\
                'inner join anadb.info_ex i on i.name = p.infotype', \
                'left join anadb.page_node_relation pr on pr.page_id = p.page_id', \
                'left join anadb.node n on n.node_id = pr.node_id', \
            ], \
            cond='i.featured = 1', \
            order='p.page_id asc'):
        if dictFormat:
            yield dict(zip(['page_id', 'name', 'relation_ndoe_id', 'node_id', 'infotype'], cols))
        else:
            yield cols

def allFeaturedCategoryGenerator(openConn):
    for cols in selectGenerator(openConn, 'anadb.category_info ci', \
            cols=['ci.cat_id', 'c.cat_title', 'cr.node_id', 'n.node_id'], \
            joins=[\
                'inner join wikidb.category c on c.cat_id = ci.cat_id', \
                'left join anadb.category_node_relation cr on cr.cat_id = ci.cat_id', \
                'left join anadb.node n on n.node_id = cr.node_id', \
            ], \
            cond='ci.featured = 1', \
            order='ci.cat_id asc'):
        yield cols

def allCategoryPageByInfotype(openConn, infotype):
    for cols in selectGenerator(openConn, 'anadb.page_ex p', \
            cols=['c.cat_id', 'p.page_id'], \
            joins=[\
                'inner join wikidb.categorylinks cl on cl.cl_from = p.page_id', \
                'inner join wikidb.category c on c.cat_title = cl.cl_to', \
                'inner join anadb.category_info ci on ci.cat_id = c.cat_id and ci.infotype = p.infotype', \
            ],\
            cond='ci.featured = 1 and p.infotype = %s',\
            order='c.cat_id asc, p.page_id asc', arg=(infotype,)):
        yield cols

def queryMultiInsert(cur, table, cols, valuesList):
    cur.execute(("""
        insert into %s (%s)
        values
        """ % (table, ','.join(cols))) \
        + ','.join(['(' + ','.join(['%s'] * len(cols)) + ')'] * len(valuesList)), \
        tuple(chain.from_iterable(valuesList)) \
    )
    pass
