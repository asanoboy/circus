from igraph import Graph, plot
from igraph.clustering import VertexDendrogram, VertexClustering
from debug import get_logger, set_config
from config import log_path

def as_clustering_if_not(obj):
    if isinstance(obj, VertexDendrogram):
        return obj.as_clustering()
    elif isinstance(obj, VertexClustering):
        return obj
    return None


def show(obj, save=None):
    p = None
    if isinstance(obj, Graph):
        p = plot(obj)
    elif isinstance(obj, VertexDendrogram):
        p = plot(obj.as_clustering())
    elif isinstance(obj, VertexClustering):
        p = plot(obj)
    else:
        return None

    if save:
        p.save('%s.png' % (save,))
    else:
        p.show()


if __name__ == '__main__':
    set_config(log_path)
    logger = get_logger(__name__)

    g = Graph.GRG(1000, 0.06)
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
        # show(c, save=method)
        cl = as_clustering_if_not(c)
        logger.debug('len = %s, %s' % (len(cl.subgraphs()), method))
