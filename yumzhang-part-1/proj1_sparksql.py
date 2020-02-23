conf = SparkConf().setAppName('Hw')
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
sqlContext= SQLContext(sc)

h2015 = spark.read.format("csv").option("header", "true").load("2015.csv")
h2015.registerTempTable('h2015')
q1=sqlContext.sql('select Country,Happiness_Score from h2015')
q1.registerTempTable('q1')

h2016 = spark.read.format("csv").option("header", "true").load("2016.csv")
h2016.registerTempTable('h2016')
q2=sqlContext.sql('select Country,Happiness_Score from h2016')
q2.registerTempTable('q2')

h2017 = spark.read.format("csv").option("header", "true").load("2017.csv")
h2017.registerTempTable('h2017')
q3=sqlContext.sql('select Country,Happiness_Score from h2017')
q3.registerTempTable('q3')

all=sqlContext.sql('select q1.Country as Country,(q2.Happiness_Score-q1.Happiness_Score) as 1stRise,(q3.Happiness_Score-q2.Happiness_Score) as 2ndRise from q1 join q2 join q3 on (q1.Country=q2.Country and q3.Country=q2.Country) order by (1stRise+2ndRise)/2 desc')
all.registerTempTable('all')

ef2016=spark.read.format("csv").option("header", "true").load("efw_cc.csv")
ef2016.registerTempTable('ef2016')
t1=sqlContext.sql('select countries,1_size_government as Government_Size,2e_integrity_legal_system as Legal_System,5_regulation as Regulation from ef2016 where year=2016')
t1.registerTempTable('t1')

all2=sqlContext.sql('select all.Country,all.1stRise,all.2ndRise,t1.Government_Size,t1.Legal_System,t1.Regulation from all join t1 on all.Country=t1.countries')
all2.write.csv('Task3')
