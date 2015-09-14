from numerical import MfFit
from envutils import get_amz_db, init_logger
from debug import get_logger
import os.path
import argparse
from circus_itertools import lazy_chunked as chunked
from functools import reduce
import math


def create_table(db, basename, fit, func=lambda x: x):
    table = 'mf_' + basename
    view = 'mf_v_' + basename
    cols = ['rank_%03d' % (i,) for i in range(fit.rank())]
    sql = '''
        create table %s (
            page_id int not null,
            %s,
            primary key (page_id)
        )''' % (
            table,
            ','.join([
                '%s float not null' % (c,)
                for c in cols])
                )
    db.updateQuery('''
        drop table if exists %s
    ''' % (table,))
    db.updateQuery('''
        drop view if exists %s
    ''' % (view,))

    db.updateQuery(sql)
    for id_and_values_list in chunked(fit.w_rows_iter(), 100):
        db.multiInsert(
            table,
            ['page_id'] + cols,
            [[id] + func(values) for id, values in id_and_values_list])
    db.commit()

    db.updateQuery('''
        create view %s as
        select p.name, tb.* from %s tb
        inner join page p on p.id = tb.page_id
    ''' % (view, table))
    db.commit()


if __name__ == '__main__':
    init_logger()
    logger = get_logger(__name__)

    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--lang', required=True)  # ja,us
    args = parser.parse_args()
    args = vars(args)
    lang = args['lang']

    for base in [
            path for path in os.listdir('.')
            if os.path.isdir(path) and path.startswith('als')]:
        fit = MfFit.load(base)
        if not fit:
            raise Exception('Can\'t load: %s' % (base,))

        db = get_amz_db(lang)

        def normalize(values):
            norm = math.sqrt(reduce(lambda summ, x: summ + x*x, values, 0))
            return [v / norm for v in values]

        create_table(db, base, fit)
        create_table(db, base + '_norm', fit, func=normalize)
