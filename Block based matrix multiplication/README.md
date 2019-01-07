# Block-Based Matrix Multiplication using Hadoop MapReduce & Apache Spark

we consider using Hadoop MapReduce and Spark to implement the block-based matrix multiplication. Each matrix is stored as a text file. Each line of the file corresponds to a block of the matrix in the format of index-content. 

This repository contains:

1. Hadoop MapReduce program, BlockMult.java, to multiply matrix A and B. Using one-phase approach and we use only one reducer here

Sample invocation:
hadoop jar bm.jar BlockMult file-A file-B output_path
Where dir-A stores the content of matrix A

2. Python Spark program, BlockMult.py, to multiply matrix A and B. 

Sample invocation:
spark-submit BlockMult.py file-A file-B (output_path)
