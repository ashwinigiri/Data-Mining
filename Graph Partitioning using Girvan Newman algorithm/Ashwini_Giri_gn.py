import operator
import itertools
import collections
from itertools import combinations
from collections import defaultdict
import sys
import json

input_file = sys.argv[1]
output_file = sys.argv[2]

def loadfile(input_file):
    file_pointer = open(input_file, "r")
    filecontent = file_pointer.read() 
    inputdata = [json.loads(str(item)) for item in filecontent.strip().split('\n')]
    file_pointer.close
    return inputdata

inputdata = loadfile(input_file)
inputgraph = [tuple(l) for l in inputdata]

def adjacencylist(inputgraph, node):
    graph = defaultdict(list)
    for ver in node:
        neighbor = []
        for i in range(len(inputgraph)):
            if ver in inputgraph[i]:
                edge_middle = inputgraph[i]
                neigh = set(edge_middle) - set(ver)
                neighbor.append(neigh.pop())
        for i in neighbor:
            graph[ver].append(i)
    return graph

node = sorted(list(set([i for i in inputgraph for i in i])))
graph = adjacencylist(inputgraph,node)

def devide(calculated_edge_betweeness):
    for key,val in calculated_edge_betweeness.items():
        calculated_edge_betweeness[key] = float(val)/2
    return calculated_edge_betweeness

def levels(vertices, graph):
    queue = list()
    directedacyclicgraph = dict()
    directedacyclicgraph[vertices] = 0
    for v in graph[vertices]:
        directedacyclicgraph[v] = directedacyclicgraph[vertices] + 1
        queue.append(v)
    while len(queue) > 0:
        u = queue.pop(0)
        for k in graph[u]:
            if k not in directedacyclicgraph.keys():
                queue.append(k)
                directedacyclicgraph[k] = directedacyclicgraph[u] + 1
    return directedacyclicgraph

def node_val(directedacyclicgraph):
    node_values = dict()
    for k,v in directedacyclicgraph.items():
        if v != 0:
            node_values[k] = v/v
        else:
            node_values[k] = v
    return node_values

def get_edge_bet(node,graph):
    calculated_edge_betweeness = dict()
    for ver in node:
        directedacyclicgraph = levels(ver, graph)
        node_values = node_val(directedacyclicgraph)
        counter = 0
        level_before_leaf = list()
        edge_middle = dict()
        while max(directedacyclicgraph.items(), key=operator.itemgetter(1))[1]  - counter > 0:
            for k,v in directedacyclicgraph.items():
                if directedacyclicgraph[k] == max(directedacyclicgraph.items(), key=operator.itemgetter(1))[1]  - counter:
                    level_before_leaf.append(k)
            while len(level_before_leaf) > 0:
                i = level_before_leaf.pop(0)
                previous_level = list()

                for node in graph[i]:
                    if directedacyclicgraph[node] == directedacyclicgraph[i] - 1:
                        previous_level.append(node)

                if len(previous_level) > 1:        
                    for q in previous_level:
                        edge_middle[(q,i)] = float(node_values[i])/len(previous_level)
                        node_values[q] += float(node_values[i])/len(previous_level)

                elif len(previous_level) == 1:        
                    for q in previous_level:
                        node_values[q] += float(node_values[i])
                        edge_middle[(q,i)] = float(node_values[i])
            counter+=1
        for k,v in edge_middle.items():
            if tuple(sorted(k)) not in calculated_edge_betweeness.keys():
                calculated_edge_betweeness[tuple(sorted(k))] = v
            else:
                calculated_edge_betweeness[tuple(sorted(k))] += v
    calculated_edge_betweeness = devide(calculated_edge_betweeness)
    return calculated_edge_betweeness

calculated_edge_betweeness = get_edge_bet(node,graph)
# print(calculated_edge_betweeness)
with open(output_file, 'w') as fp:
    for k in sorted(calculated_edge_betweeness):
        # l=k.split(',')
        fp.write("({}, {}), {}\n".format(k[0],k[1], calculated_edge_betweeness[k]))