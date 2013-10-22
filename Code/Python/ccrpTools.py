import random
import heapq
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from matplotlib import animation

def party_dijkstra(G, source, target=None, cutoff=None, weight='weight'):
    """Compute shortest paths and lengths in a weighted graph G of multiple parties

    Uses aa modification of Dijkstra's algorithm for shortest paths.

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
    use = source.lstrip ('S')
    seen['SS'] = 0
    for x in G.edges('SS'):
        if x[1].lstrip('S')!=use:
            seen[x[1]] = 0
    while fringe:
        (d,v)=heapq.heappop(fringe)
        if v in dist:
            continue # already searched this node.
        dist[v] = d
        if v == target:
            break
        
        edata=iter(G[v].items())
        for w,edgedata in edata:
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
                    S = paths[v][0].lstrip('S')
                    D = str(w).lstrip('D')
                    if S == D:
                        seen[w] = vw_dist
                        heapq.heappush(fringe,(vw_dist,w))
                        paths[w] = paths[v]+[w]
                else:
                    seen[w] = vw_dist
                    heapq.heappush(fringe,(vw_dist,w))
                    paths[w] = paths[v]+[w]
    try:
        return paths[target], dist [target]
    except KeyError:
        raise nx.NetworkXNoPath("node %s not reachable from %s"%(source,target))

class MPCCRP(object):
    def __init__(self, popN, capN, capE, timeE, graph_type='grid_2d', graph_shape=[5,5], seed=42):
        """Initialize Capacity Constrained Route Planner"""
        self.graph_type = graph_type
        self.graph_shape = graph_shape
        self.popN = popN
        self.capN = capN
        self.capE = capE
        self.timeE = timeE
        self.seed = seed
        self.makeCCGraph()
        
    def makeCCGraph(self):
        """Make the specified Capacity Constrained Graph"""
        graph_types = {
            'grid_2d': self.makeCCGraph_grid2d
        }
        if self.graph_type in graph_types:
            graph_types[self.graph_type]()
        else:
            graph_types['grid_2d']()
        

    def makeCCGraph_grid2d(self):
        """Make 2D grid Capacity Constrained Graph"""
        if (self.seed != None):
            random.seed(self.seed)
        self.G = nx.grid_2d_graph(self.graph_shape[0],self.graph_shape[1])
        self.makeCCNodes()
        self.makeCCEdges()
        
    def makeCCNodes(self):
        """Make Capacity Constrained Nodes"""
        for node in self.G.nodes_iter():
            cap = random.randint(self.capN['min'],self.capN['max'])
            self.G.node[node]['cap'] = cap
            self.G.node[node]['res'] = dict()
            self.G.node[node]['pop'] = {}
            pop = 0
            for party, partyInfo in self.popN.items():
                partyMax = min(cap-pop, partyInfo['max'])
                partyMin = min(partyMax, partyInfo['min'])
                partyPop = random.randint(partyMin,partyMax)
                self.G.node[node]['pop'][party] = {0:partyPop}
                pop += partyPop
    
    def makeCCEdges(self):
        """Make Capacity Constrained Edges"""
        for edge in self.G.edges_iter():
            cap  = random.randint(self.capE['min'],self.capE['max'])
            time = random.randint(self.timeE['min'],self.timeE['max'])
            self.G.edge[edge[0]][edge[1]]['cap'] = cap
            self.G.edge[edge[0]][edge[1]]['time'] = time
            self.G.edge[edge[0]][edge[1]]['res'] = dict()
    
    def getPartyN(self, party, t = 0):
        """Get Node Locations for a Party and Time t"""
        nodes = []
        for node in self.G.nodes_iter():
            if (self.getNodePop(node, party, t) > 0):
                nodes.append(node)
        return nodes
    
    def addPartySuper(self, party, partyInfo):
        """Add Party Super to Graph"""
        S = partyInfo['S']
        D = partyInfo['D']
        PS = 'S' + party
        PD = 'D' + party
        self.G.add_edges_from([(PS,'SS')], time = 0, cap = float('inf'))
        self.G.add_edges_from([(PD, 'SD')], time = 0, cap = float('inf'))
        PS = [PS] * len(S)
        PD = [PD] * len(D)
        self.G.add_edges_from(zip(PS, S), time = 0, cap = float('inf'))
        self.G.add_edges_from(zip(PD, D), time = 0, cap = float('inf'))
        
    def removePartySuper(self, party):
        """Remove Party Super from Graph"""
        PS = 'S' + party
        PD = 'D' + party
        self.G.remove_node(PS)
        self.G.remove_node(PD)
    
    def addSuper(self, SD):
        """Add Super Nodes to Graph"""
        self.SD = SD
        self.G.add_node('SS')
        self.G.add_node('SD')
        for party, partyInfo in self.SD.iteritems():
            self.addPartySuper(party, partyInfo)
    
    def removeSuper(self):
        """Remove Super Nodes from Graph"""
        self.G.remove_node('SS')
        self.G.remove_node('SD')
        for party, partyInfo in self.SD.iteritems():
            self.removePartySuper(party)
    
    def getPop(self, t = 0):
        """Get Total Population"""
        pop = 0
        for node in self.G.nodes_iter():
            for party, partyInfo in self.G.node[node]['pop'].items():
                pop += self.getNodePop(node, party, t)
        return pop
    
    def getNodePop(self, node, party, t):
        """Get Node Population of Party at Time t"""
        if t in self.G.node[node]['pop'][party]:
             pop = self.G.node[node]['pop'][party][t]
        else:
            pop = self.G.node[node]['pop'][party][min(self.G.node[node]['pop'][party].keys(), key=lambda T: abs(t-T) if (T<=t) else T)]
        return pop
    
    def setNodePop(self, node, party, t, pop):
        """Set Node Population of Party at Time t"""
        pop = self.G.node[node]['pop'][party][t] = pop
    
    def getR(self, t = 0):
        """Get the best path R"""
        Routes = []
        for x in self.G.edges('SS'):
            R1,d1 = party_dijkstra(self.G, source=x[1], target='SD', weight='time')
            heapq.heappush(Routes, (d1,R1))
        d,R = heapq.heappop(Routes)
        party = R[0].lstrip('S')
        R = R[1:-2]
        return R, party
    
    def getEdgeTime(self, R, i):
        """Get an Edge's Travel Time"""
        try:
            t = self.G.edge[R[i]][R[i+1]]['time']
        except:
            t = 0
        return t
    
    def getResE(self, R, t, i):
        """Get an Edge's Reservation"""
        try:
            res = self.G.edge[R[i]][R[i+1]]['res'][t]
        except:
            res = self.G.edge[R[i]][R[i+1]]['cap']
        return res
    
    def getResN(self, node, t):
        """Get a Node's Reservation"""
        try:
            res = self.G.node[node]['res'][t]
        except:
            res = self.G.node[node]['cap']
        return res
    
    def getEdgeRes(self, R, t=0):
        """Get Edge Reservations"""
        capER = 0
        for i in range(len(R)-1):
            res = self.getResE(R, t, i)
            capER += res
            
            t += self.getEdgeTime(R, i)
        return capER
    
    def getNodeRes(self, R, t=0):
        """Get Node Reservations"""
        capNR = 0
        for i in range(len(R)):
            res = self.getResN(R[i], t)
            capNR += res
            
            t += self.getEdgeTime(R, i)
        return capNR
    
    def getPathFlow(self, R, party, t=0):
        """Get Path Flow"""
        popS    = self.getNodePop(R[0], party, t)
        edgeRes = self.getEdgeRes(R, t)
        nodeRes = self.getNodeRes(R, t)
        flow = min(popS, edgeRes, nodeRes)
        return flow
    
    def setStage(self, R, party, flow, t=0):
        """Set Path Reservations"""
        t0 = t
        for i in range(len(R)-1):
            pop = self.getNodePop(R[i], party, t)
            self.setNodePop(R[i], party, t, pop - flow)
            
            resN = self.getResN(R[i], t)
            self.G.node[R[i]]['res'][t] = resN + flow
            
            resN = self.getResE(R, t, i)
            self.G.edge[R[i]][R[i+1]]['res'][t] = resN - flow
            
            t += self.getEdgeTime(R, i)
            
            pop = self.getNodePop(R[i+1], party, t)
            self.setNodePop(R[i+1], party, t, pop + flow)
            
            resN = self.getResN(R[i], t)
            self.G.node[R[i]]['res'][t] = resN - flow
            
        if ((self.getNodePop(R[0], party, t0) <= 0) or (len(R) < 2)):
            PS = 'S' + party
            self.SD[party]['S'].remove(R[0])
            self.G.remove_edge(R[0],PS)
            if self.G.degree(PS) == 1:
                self.G.remove_edge(PS,'SS')
    
    def genSD(self, source_shape=[[1,4],[1,4]], source_type='grid_2d'):
        """Generate Sources and Destination"""
        source_types = {
            'grid_2d': self.genS_grid2d
        }
        if source_type in source_types:
            S = source_types[source_type](source_shape)
        else:
            S = source_types['grid_2d'](source_shape)
        D = self.genD(S)
        return S, D
        
    def genS_grid2d(self, source_shape):
        """Generate 2D grid of Sources given shape"""
        S = []
        Sx = range(source_shape[0][0],source_shape[0][1])
        Sy = range(source_shape[1][0],source_shape[1][1])
        for x in Sx:
            S += (zip([x]*len(Sy),Sy))
        return S 
    
    def genD(self, S):
        """Generate Destination given Sources"""
        D = self.G.nodes()
        for s in S:
            D.remove(s)
        return D
    
    def getStage(self, R, party, flow, t):
        """Get the stage of the plan given R and t"""
        stage = dict()
        stage['Party'] = party
        stage['S'] = R[0]
        stage['S_data'] = self.G.node[R[0]]
        stage['D'] = R[-1]
        stage['D_data'] = self.G.node[R[-1]]
        stage['R'] = R
        stage['Flow'] = flow
        stage['Start'] = t
        return stage
    
    def isPlanning(self, t):
        """Check if Planning is Done"""
        S = 0
        for party in self.SD:
            S += len(self.SD[party]['S'])
        return (S != 0)
    
    def getTotalTime(self, plan):
        totalTime = 0
        for stage in plan:
            for party, partyInfo in stage['D_data']['pop'].items():
                maxT = max(partyInfo.keys())
                if totalTime < maxT:
                    totalTime = maxT
        return totalTime
    
    def applyCCRP(self, SD, t=0):
        """Apply Capacity Constrained Route Planner to Graph"""
        plan = []
        self.addSuper(SD)
        while self.isPlanning(t):
            R, party = self.getR(t)
            flow = self.getPathFlow(R, party, t)
            if (flow <= 0):
                t += 1
            else:
                stage = self.getStage(R, party, flow, t)
                plan.append(stage)
                self.setStage(R, party, flow, t)
        self.removeSuper()
        toatalTime = self.getTotalTime(plan)
        return plan, toatalTime
    
    def drawGraph(self, SD, figsize=(20,15), params='ne', saveFig=False, fname=None, flabel=None, t=0,):
        if saveFig:
            fig = plt.figure(figsize=figsize)
            
        edgewidth = []
        nodelabels = {}
        edgelabels = {}
        
        for (u,d) in self.G.nodes(data=True):
            nodelabels[u] = str(d['cap'])
            for party in self.popN:
                nodelabels[u] += ',' + str(self.getNodePop(u, party, t))
        
        for (u,v,d) in self.G.edges(data=True):
            edgewidth.append(d['cap']*3)
            edgelabels[(u,v)] = str('\n\n\n') + str(d['cap']) + ',' + str(d['time'])
        
        #pos = nx.spring_layout(self.G, weight='time'*-1, iterations=50)
        #pos = nx.spectral_layout(self.G)
        pos = dict((node,node) for node in self.G.nodes())        
        edge_colors = list(edge[2]['time'] for edge in self.G.edges(data=True))
        edge_cmap = plt.cm.summer
        bbox = dict(alpha=0.0)        
        nodecap=[self.G.node[node]['cap']*500 for node in self.G]
        nx.draw_networkx_edges (self.G, pos, width = edgewidth, edge_color=edge_colors, edge_cmap=edge_cmap)
        nx.draw_networkx_nodes (self.G, pos, node_size=nodecap, node_color='blue', alpha=.6)
        
        
        #evacColor = dict((node,'r') for node in self.G.nodes())
        evacColor = list('purple' for node in self.G.nodes(data=True))
        
        evacColor = []
        for node in self.G.nodes():
            if node in SD['evac']['S']:
                evacColor.append('r')
            else:
                evacColor.append('g')
        
        partyColor = {
                      'evac':evacColor,
                      'resp':'orange'
                      }
        for party in self.popN:
            nodepop=[]
            for node in self.G.nodes():
                pop = self.getNodePop(node, party, t)
                nodepop.append(pop*500)
            nx.draw_networkx_nodes (self.G, pos, node_size=nodepop, node_color=partyColor[party], alpha=.9)
            
        if 'n' in params:
            nx.draw_networkx_labels(self.G, pos, labels=nodelabels, font_size=10, font_color='white', font_weight='bold')
        if 'e' in params:
            nx.draw_networkx_edge_labels(self.G, pos, edge_labels = edgelabels, font_size=10, font_color='black', bbox=bbox)
        if saveFig:
            if ~(fname==None):
                fname = 'CCRP_'\
                + str(ccrp.graph_type) + '_'\
                + str(ccrp.graph_shape[0]) +'x'\
                + str(ccrp.graph_shape[1])\
                + flabel +'.pdf'
            fig.savefig(fname)
        #return fig