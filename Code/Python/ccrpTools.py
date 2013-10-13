import heapq
import networkx as nx
from networkx.utils import generate_unique_node

def party_dijkstra(G,source,target=None,cutoff=None,weight='weight'):
    """Compute shortest paths and lengths in a weighted graph G.

    Uses Dijkstra's algorithm for shortest paths.

    Parameters
    ----------
    G : NetworkX graph

    source : node label
       Starting node for path

    target : node label, optional
       Ending node for path

    cutoff : integer or float, optional
       Depth to stop the search. Only paths of length <= cutoff are returned.

    Returns
    -------
    distance,path : dictionaries
       Returns a tuple of two dictionaries keyed by node.
       The first dictionary stores distance from the source.
       The second stores the path from the source to that node.
    """
    if source==target:
        return ({source:0}, {source:[source]})
    dist = {}  # dictionary of final distances
    paths = {source:[source]}  # dictionary of paths
    seen = {source:0}
    fringe=[] # use heapq with (distance,label) tuples
    heapq.heappush(fringe,(0,source))
    while fringe:
        (d,v)=heapq.heappop(fringe)
        #print 'd,v: ',d,',',v
        if v in dist:
            continue # already searched this node.
        dist[v] = d
        if v == target:
            break
        
        edata=iter(G[v].items())
        for w,edgedata in edata:
            #if v == 2:
                #print 'bottle neck'
                #print 'w,edgedata: ', w, ',', edgedata
            vw_dist = dist[v] + edgedata.get(weight,1)
                
            if cutoff is not None:
                if vw_dist>cutoff:
                    continue
            if w in dist:
                if vw_dist < dist[w]:
                    raise ValueError('Contradictory paths found:',
                                     'negative weights?')
            elif w not in seen or vw_dist < seen[w]:
                if (str(w)[0] == 'D'):
                    S = paths[v][1].lstrip('S')
                    D = str(w).lstrip('D')
                    #print 'S,D: ', S, D
                    if S == D:
                        #print 'Yha!!'
                        seen[w] = vw_dist
                        heapq.heappush(fringe,(vw_dist,w))
                        paths[w] = paths[v]+[w]
                else:
                    seen[w] = vw_dist
                    heapq.heappush(fringe,(vw_dist,w))
                    paths[w] = paths[v]+[w]
        #print 'fringe: ', fringe
        
    try:
        return paths[target]
    except KeyError:
        nx.draw_spectral(G)
        raise nx.NetworkXNoPath("node %s not reachable from %s"%(source,target))
