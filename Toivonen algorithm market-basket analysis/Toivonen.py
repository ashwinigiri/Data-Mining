import os
import random
import math
import time
import itertools
import sys
from itertools import combinations

current_directory = os.getcwd()
final_directory = os.path.join(current_directory, r'output')
if not os.path.exists(final_directory):
   os.makedirs(final_directory)

filename = sys.argv[1]

with open(filename, "r") as fp:
    dataset = fp.readlines()    

def createTuple(key):
    if isinstance(key, tuple):
        return key
    else:
        return key,

def calculateSupportForSample(dataset):
    number_of_baskets = len(dataset)                            #number of baskets
    sample_size = int(math.ceil(0.1 * number_of_baskets))       #sample size                                         
    support = int(math.ceil(sample_size*(4/15.0)))              #support
    n_samples = number_of_baskets/sample_size                   #number of samples
    return number_of_baskets, sample_size, support, n_samples

def calculateSupportForSample1(dataset):
    number_of_baskets = len(dataset)                                                        
    support = int(math.ceil(number_of_baskets*(4/15.0)))    
    return number_of_baskets, support

def createSample(dataset):
    baskets = [eval(dataset[i]) for i in range(len(dataset))]
    return baskets

def makeRandomSample(number_of_baskets, sample_size, noOfItr):
    random.seed(noOfItr)
    randnum = random.randint(0, number_of_baskets - sample_size)
    sample_gen = [i for i in range(randnum, randnum + sample_size)]
    return sample_gen

def loadSample(dataset, x):
    sample = [eval(dataset[i]) for i in x]
    return sample

def makeSingletons(baskets, support):
    candidate_items = {}
    frequent_items = []
    negative_border = []
    for basket in baskets:
        basket = createTuple(basket)    
        for j in basket:
            if j in candidate_items:
                candidate_items[j] +=1
            else:
                candidate_items[j] = 1
    for k,v in candidate_items.items():
        if v >= support:
            frequent_items.append(k)
        else:
            negative_border.append(k)
            
    return frequent_items, negative_border

def itemsetGenerator(frequent_items, frequent_itemset, baskets, p):
    candidate_set = []
    if p == 2:
        for basket in baskets:
            basket = createTuple(basket)    
            for pairs in itertools.combinations(frequent_items, 2):
                if len(pairs) == p and pairs not in candidate_set:
                    candidate_set.append(pairs)
    else:
        for i, f1 in enumerate(frequent_itemset):
            for j, f2 in enumerate(frequent_itemset):
                if j > i:
                    if len(set(f1).intersection(set(f2))) == p - 2:
                        pair =  set(f1)|set(f2)
                        if pair not in candidate_set:
                            pairs = list(itertools.combinations(pair, p-1))
                            b = 0
                            for each in pairs:
                                if b == p - 2:
                                    break
                                if (set(f1).intersection(set(f2))).issubset(each) == False:
                                    if each in frequent_itemset:
                                        b += 1
                            if b == p - 2:
                                candidate_set.append(pair)

    return candidate_set

def counterCandidates(baskets, candidate_set):
    candidate_ = {}
    for candidate in candidate_set:
        
        for basket in baskets:
            basket = createTuple(basket)    
            if len(candidate) > 2:
                candidate_.setdefault(tuple(candidate),0)
                if candidate.issubset(basket):
                    candidate_[tuple(candidate)] += 1
            else:    
                candidate_.setdefault(candidate,0)
                if set(candidate).issubset(basket):
                    candidate_[candidate] += 1
    
    return candidate_

def findFrequentItemset(candidates, support, frequent_itemset, negative_border):
    frequent = []
    for k,v in candidates.items():
            if v >= support:
                frequent.append(k)
                frequent_itemset.append(k)
            else:
                negative_border.append(k)
    return frequent

def aprioriFullData(dataset):
    p = 2
    frequent_itemset = []
    n, support = calculateSupportForSample1(dataset)
    baskets = createSample(dataset)
    frequent_items, negative_border = makeSingletons(baskets, support)
    while 1:
        candidate_set = itemsetGenerator(frequent_items, frequent_itemset, baskets, p)
        candidates = counterCandidates(baskets, candidate_set)
        frequent = findFrequentItemset(candidates, support, frequent_itemset, negative_border)
        p = p+1
        if not frequent:
            break
    return baskets, frequent_items + frequent_itemset, negative_border

def apriori(dataset,seeder,sp):
    p = 2
    frequent_itemset = []
    n, sample_size, support, n_samples = calculateSupportForSample(dataset)
    support = int(math.ceil(sp * support))
    x = makeRandomSample(n, sample_size, seeder)
    baskets = loadSample(dataset, x)
    frequent_items, negative_border = makeSingletons(baskets, support)
    while 1:
        candidate_set = itemsetGenerator(frequent_items, frequent_itemset, baskets, p)
        candidates = counterCandidates(baskets, candidate_set)
        frequent = findFrequentItemset(candidates, support, frequent_itemset, negative_border)
        p = p+1
        if not frequent:
            break
     
    return baskets, frequent_items + frequent_itemset, negative_border

def Toivenen_Algorithm(dataset):
    start = time.time()
    seeder = 1
    sp = 1
    checker = 1
    while checker:
        sample_basket, sample_frequent_itemset, sample_negative_border = apriori(dataset, seeder, sp)
        fname = "output/OutputForIteration_"+str(seeder)+".txt"
        f = open(fname,"a+")
        f.write("Sample Created:\n")
        f.write(str(sample_basket)+"\n")
        f.write("frequent itemsets:\n")
        f.write(str(sample_frequent_itemset)+"\n")
        f.write("negative border: \n")
        f.write(str(sample_negative_border)+"\n")
        f.close()
        full_basket, full_frequent_itemset, full_negative_border = aprioriFullData(dataset)
        false_negative = sorted(set(full_frequent_itemset) - set(sample_frequent_itemset))
        print("False Negative: " + str(false_negative))
        sp = 1
        seeder +=1
        if not false_negative:
            checker = 0
    print( "--- "+str(seeder - 1) + " times iteration ---")
    print("--- %s seconds ---" % (time.time() - start))

Toivenen_Algorithm(dataset)

