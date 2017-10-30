# Frequent-Item-Sets---Apriori-Algorithm
An implementation of the Apriori algorithm to find frequent item sets. Written in scala.

This code has been tested with Scala ver **2.10.3** and JDK ver **1.8**

**Running the code:**

The code can be run using an IDE like IntelliJ IDEA or by building the jar and running it on the command line spark-submit tool. Creation of the jar has been tested with the open source sbt build tool ver **1.0.3**

The program uses data from the movie and rating files of the datasets found at https://grouplens.org/datasets/movielens/

**The arguments to the program are:**

1. 1 or 2: Case 1 calculates all frequent movies rated by **male** users. Case 2 calculates frequent female users who rated movies.
2. Users file. Fields are separated by a ':' delimieter
3. Ratings file. Fields are separated by a ':' delimieter
4. The minimum support threshold to qualify as a frequent itemset (specified as an integer)

Output:

The program writes the frequent pairs to the file **output.txt** with lexicographic sorting. 









