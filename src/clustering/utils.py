from igraph.clustering import VertexDendrogram, VertexClustering
from igraph import Graph, plot
from debug import get_logger


def save_cluster(obj, filename, box=(2000, 2000)):
    p = None
    if isinstance(obj, Graph):
        p = plot(obj, bbox=box)
    elif isinstance(obj, VertexDendrogram) or isinstance(obj, VertexClustering):
        cl = as_clustering_if_not(obj)
        p = plot(cl, bbox=box)
    else:
        return None

    p.save('%s.png' % (filename,))


def as_clustering_if_not(obj):
    if isinstance(obj, VertexDendrogram):
        return obj.as_clustering()
    elif isinstance(obj, VertexClustering):
        return obj
    return None


def get_method_to_comm(g):
    logger = get_logger(__name__)
    logger.debug('vertices num: %d' % (len(g.vs),))
    logger.debug('edges num   : %d' % (len(g.es), ))
    logger.debug('es/vs num   : %d' % (len(g.es) / len(g.vs), ))
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

    # method = 'leading_eigenvector'
    # with logger.lap(method):
    #     method_to_comm[method] = g.community_leading_eigenvector()

    # with logger.lap('leading_eigenvector_naive'):
    #     method_to_comm[method] = g.community_leading_eigenvector_naive()

    method = 'multilevel'
    with logger.lap(method):
        method_to_comm[method] = g.community_multilevel()

    # method = 'optimal_modularity'
    # with logger.lap(method):
    #     method_to_comm[method] = g.community_optimal_modularity()

    # method = 'spinglass'
    # with logger.lap(method):
    #     method_to_comm[method] = g.community_spinglass()

    method = 'walktrap'
    with logger.lap(method):
        method_to_comm[method] = g.community_walktrap()

    return method_to_comm
