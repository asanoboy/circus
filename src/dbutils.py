import MySQLdb.cursors
import time
from itertools import chain
from models import Page, createPageInfoByBracketText
from parser import getBracketTexts, removeComment
from circus_itertools import lazy_chunked as chunked

from contextlib import contextmanager, closing
from sqlalchemy.engine import create_engine
from sqlalchemy.orm.session import sessionmaker


@contextmanager
def master_session(name, base, **kw):
    return open_session('127.0.0.1', 'root', name, base, **kw)


@contextmanager
def open_session(hostname, user, name, base, truncate=False, **kw):
    engine = create_engine(
        'mysql://%s:@%s/%s?charset=utf8' % (user, hostname, name,), **kw)
    if truncate:
        with closing(engine.connect()) as con:
            trans = con.begin()
            for table in reversed(base.metadata.sorted_tables):
                print(table)
                if engine.dialect.has_table(con, str(table)):
                    con.execute(table.delete())
            trans.commit()

    base.metadata.create_all(engine)
    Session = sessionmaker(autocommit=True, autoflush=False)
    Session.configure(bind=engine)
    session = Session()
    yield session
    engine.dispose()


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
    def __init__(self, openConn, table, no_index=False):
        self.openConn = openConn
        self.indexList = []
        self.table = table
        self.no_index = no_index

    def open(self):
        conn = self.openConn()
        cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        cur.execute("""
            show index from %s
        """ % (self.table, ))
        records = cur.fetchall()
        nameToIndex = {}
        for r in sorted(records, key=lambda x: x['Seq_in_index']):
            name = r['Key_name']
            if name not in nameToIndex:
                nameToIndex[name] = TableIndex(name, r['Non_unique'] == 0)
            nameToIndex[name].addCol(r['Column_name'])
        for index in nameToIndex.values():
            if index.isPrimary:
                continue
                cur.execute("""
                    alter table %s drop primary key
                """ % (self.table,))
            else:
                cur.execute("""
                    alter table %s drop index %s
                """ % (self.table, index.name))

        conn.commit()
        cur.close()
        conn.close()

        self.indexList = list(nameToIndex.values())
        # return cls(openConn, table, list(nameToIndex.values()))

    def __enter__(self):
        self.open()

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    def close(self):
        if self.no_index:
            return
        conn = self.openConn()
        cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        for index in self.indexList:
            key = None
            if index.isPrimary:
                continue
                key = 'primary key'
            elif index.isUnique:
                key = 'index'  # Because unique key is not necessary.
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


class DictUseResultCursor(
        MySQLdb.cursors.CursorUseResultMixIn,
        MySQLdb.cursors.CursorDictRowsMixIn,
        MySQLdb.cursors.BaseCursor):
    pass


class TupleUseResultCursor(
        MySQLdb.cursors.CursorUseResultMixIn,
        MySQLdb.cursors.CursorTupleRowsMixIn,
        MySQLdb.cursors.BaseCursor):
    pass


def selectGeneratorFromSql(openConn, sql, arg=set(), dict_format=False):
    conn = openConn()
    cur_class = DictUseResultCursor if dict_format else TupleUseResultCursor
    cur = conn.cursor(cursorclass=cur_class)
    cur.execute('set net_read_timeout = 99999')
    cur.execute('set net_write_timeout = 99999')

    print(sql, arg)
    cur.execute(sqlStr(sql), arg)
    cnt = 0
    last_time = time.time()
    while 1:
        cnt += 1
        if cnt % 10000 == 0:
            now_time = time.time()
            print(cnt, ':%s sec' % (now_time - last_time,))
            last_time = now_time
            pass

        rt = cur.fetchone()
        if rt:
            if dict_format:
                yield {key: decode_if_binary(val) for key, val in rt.items()}
            else:
                yield list(map(lambda x: decode_if_binary(x), rt))
        else:
            break

    cur.close()
    conn.close()


def selectGenerator(
        openConn, table, cols=[], joins=[], cond='',
        order='', arg=set(), dict_format=False):

    sql = """
        select %s from %s %s
        """ % (','.join(cols), table, ' '.join(joins))
    if cond:
        sql += " where %s " % (cond,)
    if order:
        sql += " order by %s " % (order,)

    return selectGeneratorFromSql(openConn, sql, arg, dict_format)


def decode_if_binary(x):
    return x.decode('utf-8') if hasattr(x, 'decode') else x


class BaseDB:
    def __init__(self, dbname):
        self.dbname = dbname
        self.write_conn = self.openConn()
        self.read_conn = self.openConn()
        self.insert_records_num = {}
        self.update_query_num = 0

    def openConn(self):
        return MySQLdb.connect(
            host="127.0.0.1", user="root", passwd="",
            db=self.dbname, charset='utf8')

    def open_conn(self):
        return self.openConn()

    def last_id(self):
        return self.write_conn.insert_id()

    def _multiInsert(self, table, cols, valuesList, on_duplicate):
        if len(valuesList) == 0:
            return
        cur = self.write_conn.cursor()

        if on_duplicate is not None:
            on_duplicate_sql = ' on duplicate key update ' + on_duplicate
        else:
            on_duplicate_sql = ''
        cur.execute(("""
            insert into %s (%s)
            values
            """ % (table, ','.join(cols))) + ','.join(
                ['(' + ','.join(['%s'] * len(cols)) + ')'] * len(valuesList)
                ) + on_duplicate_sql,
            tuple(chain.from_iterable(valuesList))
        )
        cur.close()

        if table not in self.insert_records_num:
            self.insert_records_num[table] = 0
        self.insert_records_num[table] += len(valuesList)

    def multiInsert(
            self, table, cols, valuesList, on_duplicate=None, safe=False):
        if safe:
            try:
                self._multiInsert(table, cols, valuesList, on_duplicate)
            except Exception as e:
                print(e)
                return False
        else:
            self._multiInsert(table, cols, valuesList, on_duplicate)

        return True

    def updateQuery(self, query, args=set()):
        cur = self.write_conn.cursor()
        cur.execute(query, args)
        cur.close()
        self.update_query_num += 1

    def generate_records(
            self, table, cols=[], joins=[], cond='',
            order='', args=set(), dict_format=True):
        return selectGenerator(
            self.openConn, table, cols, joins, cond,
            order, args, dict_format)

    def generate_records_from_sql(self, sql, arg=set()):
        return selectGeneratorFromSql(self.openConn, sql, arg, True)

    def selectOne(self, query, args=set(), decode=True):
        rs = self.selectAndFetchAll(query, args=args, decode=decode)
        if len(rs) == 0:
            return None
        else:
            return rs[0]

    def selectAndFetchAll(
            self, query, args=set(), dictFormat=True, decode=True):
        cur = None
        if dictFormat:
            cur = self.read_conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        else:
            cur = self.read_conn.cursor(cursorclass=MySQLdb.cursors.Cursor)

        cur.execute(query, args)
        rt = cur.fetchall()
        if decode:
            if dictFormat:
                rt = [
                    {key: decode_if_binary(value)
                        for key, value in record.items()}
                    for record in rt]
            else:
                raise Exception('Not support')

        cur.close()
        return rt

    def commit(self):
        print('commit')
        print('Inserted to following tables.')
        print(self.insert_records_num)
        print('Updated by %s queries.' % (self.update_query_num,))

        self.insert_records_num = {}
        self.update_query_num = 0
        self.write_conn.commit()
        self.read_conn.close()
        self.read_conn = self.openConn()


class WikiDB(BaseDB):
    def __init__(self, lang):
        self.lang = lang
        super().__init__('%swiki' % (lang, ))

    def allCategoryDataGenerator(self, dict_format=False):
        for cols in selectGenerator(
                self.openConn, 'category c',
                cols=['cat_id', 'cat_title', 't.old_text'],
                joins=[
                    '''
                    inner join page p
                    on p.page_title = c.cat_title and p.page_namespace = 14
                    ''',
                    'inner join revision r on r.rev_page = p.page_id',
                    'inner join text t on t.old_id = r.rev_text_id',
                ],
                order='cat_id asc'):
            if dict_format:
                yield {'cat_id': cols[0], 'title': cols[1], 'text': cols[2]}
            else:
                yield (cols[0], cols[1], cols[2])

    def allPageTitlesGenerator(self):
        for cols in selectGenerator(
                self.openConn, 'page', cols=['page_title'],
                cond='page_namespace = 0',
                order='page_title asc'):
            yield cols[0]

    def allInfoDataGenerator(self):
        for cols in selectGenerator(
                self.openConn, 'page p',
                joins=[
                    'inner join revision r on r.rev_page = p.page_id',
                    'inner join text t on t.old_id = r.rev_text_id'
                ],
                cols=['p.page_title', 't.old_text', 't.old_id'],
                cond='page_namespace = 10',
                order='p.page_title asc'):

            if not cols[0].lower().startswith('infobox') \
                    and cols[1].lower().find('infobox') == -1:
                continue
            yield (cols[2], cols[0])

    def allInfoRecordGenerator(self):
        for cols in selectGenerator(
                self.openConn, 'an_info',
                cols=['text_id', 'name'], order='text_id asc'):
            yield {'text_id': cols[0], 'name': cols[1]}

    def generate_pagelinks_record(self):
        for cols in selectGenerator(
                self.openConn, 'pagelinks pl',
                cols=['pl.pl_from id_from', 'p.page_id id_to'],
                joins=[
                    '''
                    inner join page p
                    on p.page_title = pl.pl_title and
                    p.page_namespace = pl.pl_namespace
                    ''',
                ]):
            yield cols

    def allFeaturedPageGenerator(
            self, dictFormat=False, featured=True, page_ids=None):
        conds = []
        if featured:
            conds.append('i.featured = 1')
        if page_ids is not None:
            conds.append(
                'p.page_id in (%s)' %
                (','.join([str(pid) for pid in page_ids]), ))

        for cols in selectGenerator(
                self.openConn, 'an_page p',
                cols=['p.page_id', 'p.name', 'p.infotype', 'p.infocontent'],
                joins=[
                    'inner join an_info i on i.name = p.infotype',
                ],
                cond=' and '.join(conds) if len(conds) > 0 else None,
                order='p.page_id asc'):
            if dictFormat:
                yield dict(
                    zip(['page_id', 'name', 'infotype', 'infocontent'], cols))
            else:
                yield cols

    def allFeaturedCategoryGenerator(self):
        for cols in selectGenerator(
                self.openConn, 'an_category_info ci',
                cols=['ci.cat_id', 'c.cat_title', 'cr.node_id', 'n.node_id'],
                joins=[
                    'inner join category c on c.cat_id = ci.cat_id',
                    '''
                    left join an_category_node_relation
                    cr on cr.cat_id = ci.cat_id
                    ''',
                    'left join integrated.node n on n.node_id = cr.node_id',
                ],
                cond='ci.featured = 1',
                order='ci.cat_id asc'):
            yield cols

    def allCategoryPageByInfotype(self, infotype):
        for cols in selectGenerator(
                self.openConn, 'an_page p',
                cols=['c.cat_id', 'p.page_id'],
                joins=[
                    'inner join categorylinks cl on cl.cl_from = p.page_id',
                    'inner join category c on c.cat_title = cl.cl_to',
                    '''
                    inner join an_category_info ci on ci.cat_id = c.cat_id
                    and ci.infotype = p.infotype
                    ''',
                ],
                cond='ci.featured = 1 and p.infotype = %s',
                order='c.cat_id asc, p.page_id asc', arg=(infotype,)):
            yield cols

    def _createPageInfoByPageWikiText(self, text, allowedNames):
        bracketTexts = getBracketTexts(text)
        infos = [
            createPageInfoByBracketText(t, allowedNames)
            for t in bracketTexts]
        infos = [i for i in infos if i]
        if len(infos) == 0:
            return False
        if len(infos) == 1:
            info = infos[0]
        else:
            info = infos[0]  # Apply first info.

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

    def createPageByTitle(
            self, title, allowedInfoNames=False, namespace=0, with_info=True):
        title = '_'.join(title.split(' '))
        res = self.selectAndFetchAll(sqlStr("""
            select t.old_text wiki, p.page_id id from page p
            left join revision r on r.rev_page = p.page_id
            left join text t on t.old_id = r.rev_text_id
            where p.page_title = %s and p.page_namespace = %s
            """), (title, namespace))
        if len(res) > 0:
            text = res[0]['wiki']
            text = removeComment(text)
            info = None
            if with_info:
                info = self._createPageInfoByPageWikiText(
                    text, allowedInfoNames)
            return Page(res[0]['id'], title, text, info)
        else:
            return False


class MasterWikiDB(BaseDB):
    def __init__(self, dbname):
        super().__init__(dbname)

    def missing_page_generator(self, lang, page_iter):
        for pages in chunked(page_iter, 10):
            records = self.selectAndFetchAll("""
                select lang_page_id from page_lang_relation
                where lang = %s and lang_page_id in (
                """ + ','.join([str(page['page_id']) for page in pages]) + ')',
                (lang, ))
            found_ids = [r['lang_page_id'] for r in records]
            for page in pages:
                if page['page_id'] not in found_ids:
                    yield page
