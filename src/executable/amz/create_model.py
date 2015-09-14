from multiprocessing import Pool
import os
import numpy as np
import scipy.sparse as sp
from numerical import RelationMatrix, calc_mf
from envutils import get_amz_db, init_logger
from debug import get_logger


def create(**kw):
    R = np.array([
        [5, 3, 0, 1],
        [4, 0, 0, 1],
        [1, 1, 0, 5],
        [1, 0, 0, 4],
        [0, 1, 5, 4],
    ])
    M = sp.coo_matrix(R, dtype=np.float64)
    mat = RelationMatrix()
    nonzero = M.nonzero()
    for i, j in zip(nonzero[0], nonzero[1]):
        mat.append(i, j, -R[i, j])
    mat.build()
    return calc_mf(mat, rank=2, max_iter=100, **kw)


def generate_pages(db, min_worksnum=5, min_linkednum=10):
    for page in db.generate_records_from_sql('''
            select p.id, p.name, p.worksnum, count(*) cnt
            from page p
            inner join pagelinks pl on pl.id_to = p.id
            group by p.id
            '''):
        if page['worksnum'] >= min_worksnum and \
                page['cnt'] >= min_linkednum:
            yield page


def get_pagelinks_by_from_id(db, id_from, depth):
    result = {}
    for each_depth in range(depth + 1):
        joins = ' '.join([
            '''
                inner join pagelinks p%s on p%s.id_to = p%s.id_from
            ''' % (i+1, i, i+1)
            for i in range(each_depth)])

        sql = '''
            select distinct(p%s.id_to) id
            from pagelinks p0
            %s
            where p0.id_to = %s;
        ''' % (
            each_depth,
            joins,
            id_from,
            )
        rs = db.select_all(sql)
        rs = [r['id'] for r in rs]

        for id in rs:
            if id not in result:
                result[id] = 0
            result[id] += 1
    return result


def generate_pagelinks(db, valid_ids=None, min_linknum=3, depth=1):
    for id_from_link in db.generate_records_from_sql('''
            select distinct(id_from)
            from pagelinks
            '''):
        id_from = id_from_link['id_from']
        id2weight = get_pagelinks_by_from_id(db, id_from, depth)

        id2weight = {
            id: weight
            for id, weight in id2weight.items()
            if valid_ids is None or id in valid_ids}

        if len(id2weight) > min_linknum:
            for id, weight in id2weight.items():
                yield id_from, id, weight


def build_matrix(db, search_depth):
    id2page = {}
    for p in generate_pages(db):
        id2page[p['id']] = p

    mx = RelationMatrix()
    for id_from, id_to, weight in generate_pagelinks(
            db, id2page.keys(), depth=search_depth):
        mx.append(
            id_from,
            id_to,
            weight)
    mx.build()

    return mx


def calc(arg):
    rank, depth, lambda_var = arg
    init_logger()
    logger = get_logger('subprocess: %s' % (os.getpid(),))

    name = 'als_d%s_l%s_r%s' % (depth, lambda_var, rank)

    with logger.lap('build_%s' % (depth,)):
        db = get_amz_db('ja')
        mx = build_matrix(db, depth)

    with logger.lap(name):
        fit = calc_mf(
            mx,
            rank=rank,
            lmabda_var=lambda_var,
            min_residuals=0.1,
            max_iter=30)
        fit.save(name)


if __name__ == '__main__':
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--lang', required=True)  # ja,us
    args = parser.parse_args()
    args = vars(args)
    lang = args['lang']
    '''

    '''
    Configure
    '''
    lambda_vars = [0.01, 0.5]
    depths = [2]
    ranks = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

    '''
    Init and prepara
    '''
    args_grid = [
        (rank, depth, lambda_var)
        for depth in depths
        for rank in ranks
        for lambda_var in lambda_vars]

    '''
    Execute calculations
    '''
    with Pool(processes=4) as pool:
        results = pool.map(calc, args_grid)

    print('Finished')
