import heapq
import sys
import itertools
import math
import os

class formHierarchicalClusters:
    def __init__(self, input_file, number_of_clusters):
        self.heap = []
        self.clusters = []
        self.acutal_standard_clusters = {}
        self.fileName = input_file
        self.len_of_complete_dataset = 0
        self.shape = 0
        self.k = number_of_clusters
        self.complete_dataset = None
        
    def checkFileExists(self):
        if not os.path.isfile(self.fileName):
            self.quit("The input file given does not exist.")
        self.complete_dataset, self.clusters, self.acutal_standard_clusters = self.load_data(self.fileName)
        self.len_of_complete_dataset = len(self.complete_dataset) 
        if self.len_of_complete_dataset == 0:
            self.quit("The input file is empty it does not have any data.")
        if self.k == 0:
            self.quit("Please enter correct value for number of clusters")
        if self.k > self.len_of_complete_dataset:
            self.quit("The value of k enter is larger than the points in the dataset")
        self.shape = len(self.complete_dataset[0]["data"])
        if self.shape == 0:
            self.quit("The dimensions of the dataset is zero which cannot happen")

    def calculate_euclidean_distance(self, data_point_one, data_point_two):
        answer = 0.0
        i=0
        while i<len(data_point_one):
            answer += pow(float(data_point_one[i])-float(data_point_two[i]), 2)
            i+=1
        answer = math.sqrt(answer)
        return answer

    def function_queue(self, distance_list):
        heapq.heapify(distance_list)
        self.heap = distance_list
        return self.heap

    def compute_pairwise_distance(self, complete_dataset):
        answer = []
        p=0
        while p<(len(complete_dataset)-1):
            q=p+1
            while q <  len(complete_dataset):
                distance = self.calculate_euclidean_distance(complete_dataset[p]["data"],complete_dataset[q]["data"])
                answer.append((distance,[distance,[[p],[q]]]))
                q+=1
            p+=1
        return answer

    def compute_centroid_two_clusters(self, current_clusters, data_points_index):
        new_centroid = [0.0]*self.shape
        for index in data_points_index:
            dim_data = current_clusters[str(index)]["centroid"]
            i=0
            while i < self.shape:
                new_centroid[i] += float(dim_data[i])
                i+=1
        j=0
        while j < self.shape:
            new_centroid[j] /= len(data_points_index)
            j+=1
        return new_centroid

    def compute_centroid(self, complete_dataset, data_points_index):
        centroid = [0.0]*self.shape
        for idx in data_points_index:
            dim_data = complete_dataset[idx]["data"]
            i=0
            while i < self.shape:
                centroid[i] += float(dim_data[i])
                i+=1
        j=0
        while j < self.shape:
            centroid[j] /= len(data_points_index)
            j+=1
        return centroid

    def make_clusters_method(self):
        complete_dataset = self.complete_dataset
        current_clusters = self.clusters
        old_clusters = []
        heap = clusterObject.compute_pairwise_distance(complete_dataset)
        heap = clusterObject.function_queue(heap)
        while len(current_clusters) > self.k:
            dist, min_item = heapq.heappop(heap)
            pair_data = min_item[1]
            if not self.valid_heap_node(min_item, old_clusters):
                continue
            new_cluster = {}
            new_cluster_elements = sum(pair_data, [])
            new_cluster_cendroid = self.compute_centroid(complete_dataset, new_cluster_elements)
            new_cluster_elements.sort()
            new_cluster.setdefault("centroid", new_cluster_cendroid)
            new_cluster.setdefault("elements", new_cluster_elements)
            for pair_item in pair_data:
                old_clusters.append(pair_item)
                del current_clusters[str(pair_item)]
            self.add_heap_entry(heap, new_cluster, current_clusters)
            current_clusters[str(new_cluster_elements)] = new_cluster
        sorted(current_clusters)
        return current_clusters
            
    def valid_heap_node(self, heap_node, old_clusters):
        data = heap_node[1]
        for o in old_clusters:
            if o in data:
                return False
        return True
            
    def add_heap_entry(self, heap, new_cluster, current_clusters):
        values = current_clusters.values()
        for v in values:
            lst = []
            dist = self.calculate_euclidean_distance(v["centroid"], new_cluster["centroid"])
            lst.append(dist)
            lst.append([new_cluster["elements"], v["elements"]])
            heapq.heappush(heap, (dist, lst))

    def find_precision_recall(self, current_clusters):
        acutal_standard_clusters = self.acutal_standard_clusters
        current_clustes_pairs = []
        for (k, v) in current_clusters.items():
            tmp = list(itertools.combinations(v["elements"], 2))
            current_clustes_pairs.extend(tmp)
        tp_fp = len(current_clustes_pairs)
        acutal_standard_clusters_pairs = []
        for (acutal_standard_clusters_key, acutal_standard_clusters_value) in acutal_standard_clusters.items():
            tmp = list(itertools.combinations(acutal_standard_clusters_value, 2))
            acutal_standard_clusters_pairs.extend(tmp)
        tp_fn = len(acutal_standard_clusters_pairs)
        tp = 0.0
        for ccp in current_clustes_pairs:
            if ccp in acutal_standard_clusters_pairs:
                tp += 1
        if tp_fp == 0:
            precision = 0.0
        else:
            precision = tp/tp_fp
        if tp_fn == 0:
            precision = 0.0
        else:
            recall = tp/tp_fn
        return precision, recall

    def load_data(self, fileName):
        input_file = open(fileName, 'rU')
        complete_dataset = []
        clusters = {}
        acutal_standard_clusters = {}
        id = 0
        for line in input_file:
            line = line.strip('\n')
            row = str(line)
            row = row.split(",")
            iris_class = row[-1]
            data = {}
            data.setdefault("id", id)
            data.setdefault("data", row[:-1])
            data.setdefault("class", row[-1])
            complete_dataset.append(data)
            clusters_key = str([id])
            clusters.setdefault(clusters_key, {})
            clusters[clusters_key].setdefault("centroid", row[:-1])
            clusters[clusters_key].setdefault("elements", [id])
            acutal_standard_clusters.setdefault(iris_class, [])
            acutal_standard_clusters[iris_class].append(id)
            id += 1
        return complete_dataset, clusters, acutal_standard_clusters

    def quit(self, err_desc):
        raise SystemExit('\n'+ "PROGRAM EXIT: " + err_desc + ', please check your input' + '\n')

    def loaded_dataset(self):
        return self.complete_dataset

    def print_results(self, current_clusters, precision, recall):
        clusters = current_clusters.values()
        for i in range(len(clusters)):
            cluster=clusters[i]['elements']
            cluster=sorted(cluster)
            print "Cluster " +str(i+1)+ ": "+"["+','.join([str(x) for x in cluster])+"]"
        print "Precision = ",precision 
        print "Recall = ",recall 

if __name__ == '__main__':
    input_file = sys.argv[1]     
    number_of_clusters = int(sys.argv[2])
    clusterObject = formHierarchicalClusters(input_file, number_of_clusters)
    clusterObject.checkFileExists()
    current_clusters = clusterObject.make_clusters_method()
    precision, recall = clusterObject.find_precision_recall(current_clusters)
    clusterObject.print_results(current_clusters, precision, recall)
