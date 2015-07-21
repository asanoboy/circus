from igraph.clustering import VertexDendrogram, VertexClustering


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
