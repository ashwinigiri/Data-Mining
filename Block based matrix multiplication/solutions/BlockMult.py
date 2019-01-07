import sys
from pyspark import SparkContext
import numpy as np

sc=SparkContext(appName="ashwiniinf553")

input_fileA=sys.argv[1]
input_fileB=sys.argv[2]
output_file=sys.argv[3]
output_data = []
file_pointer = open(output_file,'a')

matrixA=sc.textFile(input_fileA)
matrixB=sc.textFile(input_fileB)

def funA(input):
    l=[]
    for i in range(1,4):
        l.append(((input[1],str(i)),('A',input[3],input[6:])))
    return l

def funB(input):
    l=[]
    for i in range(1,4):
        l.append(((str(i),input[3]),('B',input[1],input[6:])))
    return l

def makelist(s):
    l =[]
    for i in range(len(s)-7):
        if s[i]=='(':
            tup = (int(s[i+1]),int(s[i+3]),int(s[i+5]))
            l.append(tup)
    return l

def multiply(dx):
    multiresult = []
    for key,val in dx.items():
        A=[[0,0],[0,0]]
        B=[[0,0],[0,0]]
        if len(val)==2:
            one = val[0]
            two = val[1]
            if one[0]=='A':
                for tup in one[1]:
                    A[tup[0]-1][tup[1]-1] = tup[2]
            if one[0]=='B':
                for tup in one[1]:
                    B[tup[0]-1][tup[1]-1] = tup[2]
            if two[0]=='A':
                for tup in two[1]:
                    A[tup[0]-1][tup[1]-1] = tup[2]
            if two[0]=='B':
                for tup in two[1]:
                    B[tup[0]-1][tup[1]-1] = tup[2]

        multiresult.append(np.matmul(A, B))
    return multiresult

def writeoutput(first):
    f=[]
    file_pointer = open(output_file,'a')
    dx = {1:[],2:[],3:[]}
    value = list(first[1])
    for tup in value:
        dx[int(tup[1])].append([str(tup[0]),tup[2]])
    for x in dx.values():
        for val in x:
            val[1] = makelist(val[1])

    multiresult = multiply(dx)
    finalresult = [[0,0],[0,0]]
    for i in range(0,2):
        for j in range(0,2):
            finalresult[i][j] = (multiresult[0].tolist())[i][j]+(multiresult[1].tolist())[i][j]+(multiresult[2].tolist())[i][j]  
    res = []
    if finalresult!=[[0,0],[0,0]]:
        for i in range(2):
            for j in range(2):
                if finalresult[i][j]!=0:
                    res.append((i+1,j+1,finalresult[i][j]))
        f.append(first[0])
        f.append(res)
        file_pointer = open(output_file,'a')
        file_pointer.write("("+str(f)[3:-1]+'\n')

def reducefunction(first,second):
    if first is not None:
        writeoutput(first)
    if second is not None:
        writeoutput(second)

# Spark map and reduce functions
mapoutput = matrixA.flatMap(funA).union(matrixB.flatMap(funB))
mapoutput=mapoutput.groupByKey()
mapoutput.reduce(reducefunction)
