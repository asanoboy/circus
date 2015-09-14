from envutils import get_amz_db, init_logger
from debug import get_logger
from create_model import generate_pages
import os
import igraph
import pickle


def memoize(f, filepath=None):
    memo = {}
    if filepath and os.path.isfile(filepath):
        with open(filepath, 'rb') as file:
            memo = pickle.load(file)

    def helper(*args, **kw):
        if len(kw) > 0:
            raise Exception('Can\'t memoize.')
        key = str(tuple(args))
        if key not in memo:
            memo[key] = f(*args)
            if filepath:
                with open(filepath, 'wb') as file:
                    pickle.dump(memo, file)
        return memo[key]
    return helper


def _get_near_pairs(db):
    sql = '''
        select li1.id_from, li1.id_to
        from v_pagelinks li1
        inner join pagelinks li2
            on li2.id_to = li1.id_from and li2.id_from = li1.id_to
        where li1.id_from < li1.id_to
    '''
    return [(r['id_from'], r['id_to']) for r in db.select_all(sql)]


class Graph:
    def __init__(self):
        self.g = igraph.Graph()
        self.names = []

    def add_vertex(self, name, ignore_duplicate=True):
        if name in self.names:
            if ignore_duplicate:
                return
            raise Exception('Invalid vertex.')
        self.names.append(name)
        self.g.add_vertex(name=name)

    def add_edges(self, edges):
        g_edges = [
            [self.names.index(v_from), self.names.index(v_to)]
            for v_from, v_to in edges
            if v_from in self.names and v_to in self.names]
        if len(g_edges) != len(edges):
            raise Exception('Invalid edges.')
        self.g.add_edges(g_edges)

    def delete_vertices(self, names):
        vertices = [
            self.names.index(n) for n in names
            if n in self.names]
        if len(vertices) != len(names):
            print(vertices)
            print(names)
            raise Exception('Invalid vertex.')
        self.g.delete_vertices(vertices)
        for n in names:
            self.names.remove(n)

    def farthest_points(self, *args, **kw):
        src, dst, distance = self.g.farthest_points(*args, **kw)
        return self.names[src], self.names[dst], distance


def get_graph(db):
    valids = [p['id'] for p in generate_pages(db)]
    sql = '''
        select id_from, id_to
        from pagelinks
    '''
    g = Graph()
    for r in db.select_all(sql):
        if r['id_from'] in valids and r['id_to'] in valids:
            g.add_vertex(r['id_from'])
            g.add_vertex(r['id_to'])
            g.add_edges([[r['id_from'], r['id_to']]])
    return g


def _get_far_pairs(db):
    num = 1000
    g = get_graph(db)
    rs = []
    for i in range(num):
        src, dst, distance = g.farthest_points()
        if i % 2 == 0:
            g.delete_vertices([min(src, dst)])
        else:
            g.delete_vertices([max(src, dst)])

        rs.append((src, dst))
    return rs


get_near_pairs = memoize(_get_near_pairs)
get_far_pairs = memoize(_get_far_pairs, 'als_far_pairs')


def basename_to_norm_table(name):
    return 'mf_' + name + '_norm'


def basename_to_table(name):
    return 'mf_' + name


def get_rank(db, table):
    sql = '''
        show columns from %s
        where Field like 'rank%%%%'
    ''' % (table,)
    res = db.select_all(sql)
    rank = len(res)
    return rank


def _get_formula(rank, method):
    if method == 'cos':
        return ' + '.join([
            'org.rank_%03d * dst.rank_%03d' % (i, i)
            for i in range(rank)])
    else:
        return 'sqrt(' + \
            ' + '.join([
                '''
                power(org.rank_%03d -
                    dst.rank_%03d, 2)
                ''' % (i, i)
                for i in range(rank)]) \
            + ')'


def distance(db, table, page_id_from, page_id_to, method='cos', rank=None):
    if rank is None:
        rank = get_rank(db, table)

    sql = '''
        select ( %s ) distance
        from %s org
        inner join %s dst
        where org.page_id = %s and dst.page_id = %s
    ''' % (
        _get_formula(rank, method),
        table,
        table,
        page_id_from,
        page_id_to)
    rs = db.select_one(sql)
    if not rs:
        return None
    return rs['distance']


def nears(db, basename, page_id, method, num=20):
    if method == 'cos':
        table = basename_to_norm_table(basename)
        order = 'desc'
    else:
        table = basename_to_table(basename)
        order = 'asc'

    rank = get_rank(db, table)

    sql = '''
        select ( %s ) distance, p.*
        from %s org
        inner join %s dst
        inner join page p on p.id = dst.page_id
        where org.page_id = %s
        order by distance %s
        limit %s
    ''' % (
        _get_formula(rank, method),
        table,
        table,
        page_id,
        order,
        num)
    return db.select_all(sql)


def calc_validation(db, basename, method):
    table = basename_to_norm_table(basename)
    rank = get_rank(db, table)

    near_dis_list = filter(lambda d: d, [
        distance(db, table, id_from, id_to, method, rank=rank)
        for id_from, id_to in get_near_pairs(db)])
    near_dis_list = list(near_dis_list)

    far_dis_list = filter(lambda d: d, [
        distance(db, table, id_from, id_to, method, rank=rank)
        for id_from, id_to in get_far_pairs(db)])
    far_dis_list = list(far_dis_list)

    near_ave = sum(near_dis_list) / len(near_dis_list)
    far_ave = sum(far_dis_list) / len(far_dis_list)
    return near_ave, far_ave


if __name__ == '__main__':
    init_logger()
    logger = get_logger(__name__)

    lang = 'ja'
    bases = [
        path for path in os.listdir('.')
        if os.path.isdir(path) and path.startswith('als')]
    db = get_amz_db(lang)

    result = {}
    for i, base in enumerate(bases):
        logger.debug('>> %s / %s' % (i, len(bases)))

        with logger.lap('[%s cos]' % (base,)):
            near, far = calc_validation(db, base, 'cos')
            cos_validations = {'near': near, 'far': far}
        with logger.lap('[%s euc' % (base,)):
            near, far = calc_validation(db, base, 'euc')
            euc_validations = {'near': near, 'far': far}

        vals = {
            'cos': cos_validations,
            'euc': euc_validations,
        }
        with open('%s.vals' % (base,), 'wb') as f:
            pickle.dump(vals, f)
        result[base] = vals

    with open('result.vals', 'wb') as f:
        pickle.dump(result, f)
