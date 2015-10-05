import argparse
from igraph import Graph
from clustering.utils import get_method_to_comm, as_clustering_if_not, save_cluster
from config import log_path, dbhost, user, pw
from debug import get_logger, set_config
from dbutils import WikiDB, open_session
from model.master import Base, Page


class PairHolder:
    def __init__(self):
        self.pair_to_cnt = {}

    def add(self, id_from, id_to):
        if id_from < id_to:
            key = (id_from, id_to)
        else:
            key = (id_to, id_from)

        if key not in self.pair_to_cnt:
            self.pair_to_cnt[key] = 1
        else:
            self.pair_to_cnt[key] += 1

    def get_multi_pairs(self):
        return [
            pair for pair, cnt in self.pair_to_cnt.items()
            if cnt >= 2]
            

        
if True or __name__ == '__main__':
    set_config(log_path)
    logger = get_logger(__name__)

    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--lang')  # ja|en
    args = parser.parse_args()
    args = vars(args)

    lang = 'en' # args['lang']

    with open_session(
            dbhost,
            user,
            'master',
            Base,
            pw=pw,
            truncate=False) as session:
        wiki_db = WikiDB(lang, user, dbhost, pw=pw)
        pages = session.query(Page).filter(
            Page.lang==lang, Page.linknum>5)
        pid_to_page = {p.page_id: p for p in pages}

        pid_joined = ','.join([str(i) for i in pid_to_page.keys()])
        links = wiki_db.selectAndFetchAll('''
            select id_from, id_to from an_pagelinks_music_genre
            where id_from in (%s) and id_to in (%s)
            ''' % (pid_joined, pid_joined))

        
        holder = PairHolder()
        for link in links:
            holder.add(link['id_from'], link['id_to'])

        g = Graph()
        logger.debug('page_num=%s' % (len(pid_to_page),))
        index_to_page = {}
        pid_to_index = {}
        names = ''
        for index, (pid, page) in enumerate(pid_to_page.items()):
            index_to_page[index] = page
            pid_to_index[pid] = index
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
                pid_to_index[pair[0]],
                pid_to_index[pair[1]]
            ] for pair in holder.get_multi_pairs()])
        g.vs['hands'] = [
            index_to_page[i].linknum
            for i, v in enumerate(g.vs)]
        method_to_comm = get_method_to_comm(g)
        for method, c in method_to_comm.items():
            save_cluster(c, method, box=(10000, 10000))
            cl = as_clustering_if_not(c)
            logger.debug(
                'len = %s, mod = %s %s' % (
                    len(cl.subgraphs()),
                    cl.q,
                    method))
