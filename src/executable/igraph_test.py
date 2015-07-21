from igraph import Graph, plot
from igraph.clustering import VertexDendrogram, VertexClustering
from debug import get_logger, set_config
from config import log_path
from clustering.utils import as_clustering_if_not, save_cluster


if __name__ == '__main__':
    set_config(log_path)
    logger = get_logger(__name__)

    # g = Graph.GRG(1000, 0.06)
    g = Graph.GRG(10000, 0.02)
    print('vertices num: ', len(g.vs))
    print('edges num   : ', len(g.es))
    print('es/vs num   : ', len(g.es) / len(g.vs))
    method_to_comm = {}

    # Vertices number is over 1000, calculation is too heavy.
    # method = 'edge_betweenness'
    # with logger.lap(method):
    #     method_to_comm[method] = g.community_edge_betweenness()

    method = 'fastgreedy'
    with logger.lap(method):
        method_to_comm[method] = g.community_fastgreedy()

    method = 'infomap'
    with logger.lap(method):
        method_to_comm[method] = g.community_infomap()

    method = 'label_propagation'
    with logger.lap(method):
        method_to_comm[method] = g.community_label_propagation()

    method = 'leading_eigenvector'
    with logger.lap(method):
        method_to_comm[method] = g.community_leading_eigenvector()

    # with logger.lap('leading_eigenvector_naive'):
    #     method_to_comm[method] = g.community_leading_eigenvector_naive()

    method = 'multilevel'
    with logger.lap(method):
        method_to_comm[method] = g.community_multilevel()

    # method = 'optimal_modularity'
    # with logger.lap(method):
    #     method_to_comm[method] = g.community_optimal_modularity()

    method = 'spinglass'
    with logger.lap(method):
        method_to_comm[method] = g.community_spinglass()

    method = 'walktrap'
    with logger.lap(method):
        method_to_comm[method] = g.community_walktrap()

    for method, c in method_to_comm.items():
        save_cluster(c, method)
        cl = as_clustering_if_not(c)
        logger.debug('len = %s, mod = %s %s' % (len(cl.subgraphs()), cl.q, method))
