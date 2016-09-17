import networkx as nx
from ndex.networkn import NdexGraph


def generate_graph(graph_size, prefix=''):
    prefix = str(prefix)
    G = NdexGraph(networkx_G=nx.complete_graph(graph_size))
    G.set_name(prefix + '-' + str(graph_size))
    return G