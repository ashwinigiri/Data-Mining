from pyspark import SparkContext
from itertools import combinations
import operator
import sys

sc = SparkContext(appName ="ashwini553")
inputfilename = sys.argv[1]
outputfilename = sys.argv[2]
input_file_rdd = sc.textFile(inputfilename)
input_file_rdd2= input_file_rdd

input_file_rdd = input_file_rdd.map(lambda u:u.split(',')).flatMap(lambda x:[(x[0],y) for y in x[1:]])

def minHash(x):
    hashbuckets =[]
    for i in range(20):
        h = ((3*int(x[1]))+(13*i))%100
        hashbuckets.append(((x[0],i),h))
    return hashbuckets

def LSH_Bands(h):
    return [((h[0],0),h[1][0:4]),((h[0],1),h[1][4:8]),((h[0],2),h[1][8:12]),((h[0],3),h[1][12:16]),((h[0],4),h[1][16:20])]


def modify(input):
    key = input[0]
    value = input[1]
    final = [key[0],value]
    return (key[1],final)


def findCandidates(input):
    keys = input[0]
    values = input[1]
    dict = {}
    for x in values:
        dict[str(x[1])] = []
    for x in values:
        key = str(x[0])
        value = str(x[1])
        if value in dict:
            dict[value].append(key)
    similar_users = []
    for k,v in dict.items():
        if len(v) > 1:
            similar_users.append(v)
    lsh_pair = []
    for x in similar_users:
        y = (list(combinations(x,2)))
        lsh_pair += y
    return lsh_pair

#Generating the 20 hash functions
input_file_rdd = input_file_rdd.flatMap(minHash).groupByKey().map(lambda x:(x[0],min(list(x[1]))))
input_file_rdd = input_file_rdd.map(lambda x:(x[0][0],(x[0][1],x[1]))).groupByKey().map(lambda x:(x[0],sorted(list(x[1]))))

#Sorting the hash functions into bands. four(4) hash functions per band and there are 5 bands
input_file_rdd = input_file_rdd.flatMap(LSH_Bands)

#Transforming the input_file_rdd to have band number as key and then group by band
input_file_rdd = input_file_rdd.map(modify).groupByKey().map(lambda x:(x[0],list(x[1])))

#Finding candidate pair based on similarity of signature within each band
input_file_rdd = input_file_rdd.flatMap(findCandidates).distinct()

#Transforming original input_file_rdd
input_complete_data = input_file_rdd2.map(lambda x:x.split(',')).flatMap(lambda x:[(x[0],int(y)) for y in x[1:]]).groupByKey().map(lambda x:(x[0],list(x[1])))
input_complete_data_list = sc.broadcast(input_complete_data.collect())

input_complete_data_dict = {}
def makeDict():
    dict = {}
    k = input_complete_data_list.value
    for x in k:
        dict[x[0]] = list(x[1])
    global input_complete_data_dict
    input_complete_data_dict = dict


makeDict()


f = input_file_rdd.collect()

#finding the jaccard similarity of pairs with identical signature
def getRecommendation(x):
    import operator
    dict_pair = {}
    lis = {}
    for i in x:
        intx = float(len(set(input_complete_data_dict[i[0]]).intersection(set(input_complete_data_dict[i[1]]))))
        uni = len(set(input_complete_data_dict[i[0]]).union(set(input_complete_data_dict[i[1]])))     
        jx = intx / uni
        if i[0] not in dict_pair.keys():
            dict_pair[i[0]] = {i[1]:jx}
        else:
            dict_pair[i[0]][i[1]] = jx
        if i[1] not in dict_pair.keys():
            dict_pair[i[1]] = {i[0]:jx}
        else:
            dict_pair[i[1]][i[0]] = jx  
    for key,v in dict_pair.items():
        l = sorted(v.items(), key=operator.itemgetter(1), reverse = True)[:5]
        lis[key] = [i[0] for i in l]
    return lis

def top3_recommendation(x):
    top3_recommendation = []
    for k,v in x.items():
        c = {}
        for i in v:
            w = input_complete_data_dict[i]
            for p in w:
                if p not in c:
                    c[p] = 1
                else:
                    c[p] +=1
        f = sorted(c.items(), key=operator.itemgetter(0))
        f = sorted(f, key=operator.itemgetter(1), reverse = True)[:3]
        key = sorted([i[0] for i in f])
        top3_recommendation.append([k,key])
    return top3_recommendation

b = getRecommendation(f)
output = top3_recommendation(b)
finaloutput = []
for x in output:
    finaloutput.append([int(x[0][1:]),x[1]])
finaloutput = sorted(finaloutput)
f = open(outputfilename,"a+")
for x in finaloutput:
    f.write(str('U')+str(x[0])+", "+str(x[1])[1:-1]+"\n")

