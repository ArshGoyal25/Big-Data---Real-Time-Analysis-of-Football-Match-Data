#import findspark
import json
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import lit
from pyspark.sql import SQLContext,SparkSession
import sys
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col,array_contains,struct,when
from pyspark.sql.types import *
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
count = 0
####################################################################################################################
########################################### Spark Initialisation ###################################################
####################################################################################################################

conf = SparkConf()
conf.setAppName("FPL")
sc = SparkContext(conf=conf)
sqlContext = SparkSession.builder.getOrCreate()
ssc = StreamingContext(sc, 5)
ssc.checkpoint("checkpoint_FPL")


# Reading players and teams csv files
player_count=[]
query = sqlContext.read.json("count_matches.json")
for i in query.collect():
	player_id=i.metric._1
	count=i.metric._2
	player_count.append([float(player_id),float(count)])
player_count = sc.parallelize(player_count).toDF(("a", "b"))
player_count.show()


#df=sqlContext.read.json("profile.json")

with open('data2.json') as json_file: 
     chemistry= json.load(json_file)

player_Profile = sqlContext.read.json("profile.json")
#player_Profile = sqlContext.read.json("/user/arshgoyal/Player_Profile.json/part-00000-f331403b-22c4-4855-9796-39ebae661158-c000.json")
#player_Profile.show()
df_list=[]
for i in player_Profile.collect():
	if (i.metric._1!=None):
		col1=float(i.metric._1)
	else:
		col1=float(0)

	if (i.metric._2!=None):
		col2=float(i.metric._2)
	else:
		col2=float(0)
	if (i.metric._3!=None):
		col3=float(i.metric._3)
	else:
		col3=float(0)
	if (i.metric._4!=None):
		col4=float(i.metric._4)
	else:
		col4=float(0)
	if (i.metric._5!=None):
		col5=float(i.metric._5)
	else:
		col5=float(0)
	if (i.metric._6!=None):
		col6=float(i.metric._6)
	else:
		col6=float(0)

	df_list.append([float(i.id),col1,col2,col3,col4,col5,col6])
df1 = sc.parallelize(df_list).toDF(("a", "b", "c","d","e","f",'g'))


vecAssembler = VectorAssembler(inputCols=["b", "c","d","e","f","g"], outputCol="features")
new_df = vecAssembler.transform(df1)
kmeans = KMeans(k=5, seed=1) 
model = kmeans.fit(new_df.select('features'))
transformed = model.transform(new_df)
transformed.show()    
cluster_1=transformed.filter(transformed.prediction==0).select("a")
cluster_2=transformed.filter(transformed.prediction==1).select("a")
cluster_3=transformed.filter(transformed.prediction==2).select("a")
cluster_4=transformed.filter(transformed.prediction==3).select("a")
cluster_5=transformed.filter(transformed.prediction==4).select("a")

cluster_1=cluster_1.filter(cluster_1.a==transformed.a)
cluster_1=cluster_1.join(player_count,cluster_1.a==player_count.a)
cluster_1.show()

cluster_2=cluster_2.filter(cluster_2.a==transformed.a)
cluster_2=cluster_2.join(player_count,cluster_2.a==player_count.a)

cluster_3=cluster_3.filter(cluster_3.a==transformed.a)
cluster_3=cluster_3.join(player_count,cluster_3.a==player_count.a)

cluster_4=cluster_4.filter(cluster_4.a==transformed.a)
cluster_4=cluster_4.join(player_count,cluster_4.a==player_count.a)

cluster_5=cluster_5.filter(cluster_5.a==transformed.a)
cluster_5=cluster_5.join(player_count,cluster_5.a==player_count.a)

final_values=[]

unique_players_1=[]
for i in cluster_1.collect():
	unique_players_1.append(str(int(i.a)))
average_cluster_1=0
for i in unique_players_1:
	for j in unique_players_1:
		if(i!=j):
			try:
				average_cluster_1+=float(chemistry[str(j+" "+i)])
			except:
				average_cluster_1+=float(chemistry[str(i+" "+j)])
final_values.append(average_cluster_1/len(cluster_1.collect()))



unique_players_2=[]
for i in cluster_2.collect():
	unique_players_1.append(str(int(i.a)))
average_cluster_2=0
for i in unique_players_2:
	for j in unique_players_2:
		if(i!=j):
			try:
				average_cluster_2+=float(chemistry[str(j+" "+i)])
			except:
				average_cluster_2+=float(chemistry[str(i+" "+j)])
final_values.append(average_cluster_2/len(cluster_2.collect()))


unique_players_3=[]
for i in cluster_3.collect():
	unique_players_3.append(str(int(i.a)))
average_cluster_3=0
for i in unique_players_3:
	for j in unique_players_3:
		if(i!=j):
			try:
				average_cluster_3+=float(chemistry[str(j+" "+i)])
			except:
				average_cluster_3+=float(chemistry[str(i+" "+j)])
final_values.append(average_cluster_3/len(cluster_3.collect()))


unique_players_4=[]
for i in cluster_4.collect():
	unique_players_4.append(str(int(i.a)))
average_cluster_4=0
for i in unique_players_4:
	for j in unique_players_4:
		if(i!=j):
			try:
				average_cluster_4+=float(chemistry[str(j+" "+i)])
			except:
				average_cluster_4+=float(chemistry[str(i+" "+j)])
final_values.append(average_cluster_4/len(cluster_4.collect()))


unique_players_5=[]
for i in cluster_5.collect():
	unique_players_5.append(str(int(i.a)))
average_cluster_5=0
for i in unique_players_5:
	for j in unique_players_5:
		if(i!=j):
			try:
				average_cluster_5+=float(chemistry[str(j+" "+i)])
			except:
				average_cluster_5+=float(chemistry[str(i+" "+j)])
final_values.append(average_cluster_5/len(cluster_5.collect()))


print(final_values)

#for i in cluster_1.collect():

# cluster_1.show()
# cluster_2.show()
# cluster_3.show()
# cluster_4.show()
# cluster_5.show()

