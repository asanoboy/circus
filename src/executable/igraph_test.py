from igraph import Graph, plot
from igraph.clustering import VertexDendrogram, VertexClustering
from debug import get_logger, set_config
from config import log_path
from clustering.utils import as_clustering_if_not, save_cluster, get_method_to_comm


if __name__ == '__main__':
    set_config(log_path)
    logger = get_logger(__name__)

    # g = Graph.GRG(1000, 0.06)
    g = Graph.GRG(10000, 0.02)

    method_to_comm = get_method_to_comm(g)
    for method, c in method_to_comm.items():
        save_cluster(c, method)
        cl = as_clustering_if_not(c)
        logger.debug('len = %s, mod = %s %s' % (len(cl.subgraphs()), cl.q, method))
