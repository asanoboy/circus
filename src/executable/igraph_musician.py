import argparse
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
        pages = session.query(Page).filter(
            Page.lang==lang, Page.linknum>5)
        pid_to_page = {p.page_id: p for p in pages}

        pid_joined = ','.join([str(i) for i in pid_to_page.keys()])
        links = wiki_db.selectAndFetchAll('''
            select id_from, id_to from an_pagelinks_picked
            where id_from in (%s) and id_to in (%s)
            ''' % (pid_joined, pid_joined))

        g = Graph()
        for pid, page in pid_to_page.items():
            g.add_vertex(str(pid), label=page.name)
            print(str(pid), page.name)

        for link in links:
            id_from = link['id_from']
            id_to = link['id_to']
            if id_from > id_to:
                continue
            g.add_edge(str(id_from), str(id_to))
        
        method_to_comm = get_method_to_comm(g)
        for method, c in method_to_comm.items():
            save_cluster(c, method, box=(4000, 4000))
            cl = as_clustering_if_not(c)
            logger.debug(
                'len = %s, mod = %s %s' % (
                    len(cl.subgraphs()),
                    cl.q,
                    method))
