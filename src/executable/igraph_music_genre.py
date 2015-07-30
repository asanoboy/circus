import argparse
from itertools import chain
from igraph import Graph
from clustering.utils import get_method_to_comm, as_clustering_if_not, save_cluster
from config import log_path, dbhost, user, pw
from debug import get_logger, set_config
from dbutils import WikiDB, open_session
from model.master import Base, Page

        
if True or __name__ == '__main__':
    set_config(log_path)
    logger = get_logger(__name__)

    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--lang')  # ja|en
    args = parser.parse_args()
    args = vars(args)

    lang = 'en' # args['lang']

    with open_session(dbhost, user, 'master', Base, pw=pw, truncate=False) as session:
        wiki_db = WikiDB(lang, user, dbhost, pw=pw)
        pages = session.query(Page).filter(Page.lang==lang)
        pid_to_page = {p.page_id: p for p in pages}

        table = 'an_pagelinks_music_genre'
        links = wiki_db.selectAndFetchAll('''
            select id_from, id_to from %s
            ''' % (table,))

        pids = chain.from_iterable([[r['id_from'], r['id_to']] for r in links])
        pids = list(set(pids))
        pid_to_page = {pid: page for pid, page in pid_to_page.items() if pid in pids}

        g = Graph()
        logger.debug('page_num=%s' % (len(pid_to_page),))
        id_to_page = {}
        pid_to_id = {}
        names = ''
        for index, (pid, page) in enumerate(pid_to_page.items()):
            id_to_page[index] = page
            pid_to_id[pid] = index
            # g.add_vertex()
            label = page.name
            pos = label.find('(')
            if pos >= 0:
                label = label[:pos]
            g.add_vertex(name=label)
            names += ',' + label

        logger.debug('link_num=%s' % (len(links),))
        g.add_edges(
            [[
                pid_to_id[r['id_from']],
                pid_to_id[r['id_to']]
            ] for r in links])
        
        g.vs['hands'] = [len(v.neighbors()) for v in g.vs]
        method_to_comm = get_method_to_comm(g)
        # for method, c in method_to_comm.items():
        #     save_cluster(c, method, box=(10000, 10000))
        #     cl = as_clustering_if_not(c)
        #     logger.debug(
        #         'len = %s, mod = %s %s' % (
        #             len(cl.subgraphs()),
        #             cl.q,
        #             method))
