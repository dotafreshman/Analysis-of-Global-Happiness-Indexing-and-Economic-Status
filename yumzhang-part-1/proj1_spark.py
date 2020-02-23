#spark-submit --master yarn --num-executors 16 --executor-memory 1g --executor-cores 2 proj1_spark.py

import json
from pyspark import SparkContext
sc = SparkContext(appName="proj1")

h2016_raw = sc.textFile("2016.csv")
ef_raw=sc.textFile("efw_cc.csv")

ef=ef_raw.map(lambda line:tuple([line.split(',')[i] for i in [0,2,3]]))
ef2016=ef.filter(lambda x:x[0]=='2016')
ef2016=ef2016.map(lambda x:(x[1],x[2]))


h2016=h2016_raw.map(lambda line:tuple([line.split(',')[i] for i in [0,3]]))


all=h2016.join(ef2016)
res=all.map(lambda x:(x[0],float(x[1][0]),float(x[1][1])))\
       .sortBy(lambda x:x[1],ascending=False)\
       .map(lambda x:(str(x[0]),str(x[1]),str(x[2])))\
       .map(lambda x : ("\t".join(x)).strip("\""))

res.saveAsTextFile("Task2")



sc.stop()
