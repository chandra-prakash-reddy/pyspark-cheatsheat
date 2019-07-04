#!/usr/bin/env python
# coding: utf-8

# In[3]:


from pyspark.sql import SparkSession


# In[4]:


spark = SparkSession.builder                    .appName("Analyzing soccer players")                    .getOrCreate()


# In[5]:


players = spark.read              .format("csv")              .option("header","true")              .load("/home/cnk45874/demo/player.csv")


# In[6]:


players.printSchema()


# In[7]:


players.show(10)


# In[51]:


players_attributes=spark.read                        .format("csv")                        .option("header","true")                         .load("/home/cnk45874/demo/player_attributes.csv")


# In[52]:


players_attributes.printSchema()


# In[10]:


players.count(),players_attributes.count()


# In[11]:


players_attributes.select("player_api_id").distinct().count()


# In[12]:


players.columns


# In[13]:


players=players.drop('id','player_fifa_api_id')
players.columns


# In[14]:


players=players.dropna()
players_attributes=players_attributes.dropna()


# In[15]:


players.count(),players_attributes.count()


# In[17]:


from pyspark.sql.functions import udf


# In[18]:


players_attributes.select("date").show(10)


# In[54]:


year_extract_udf=udf(lambda date:date.split('-')[0])
players_attributes=players_attributes.withColumn("year",year_extract_udf(players_attributes.date))


# In[55]:


players_attributes.columns


# In[56]:


players_attributes=players_attributes.drop('date')


# In[57]:


players_attributes.select("year").show(10)


# In[58]:


pa_2016=players_attributes.filter(players_attributes.year==2016)


# In[59]:


pa_2016.count()


# In[25]:


pa_2016.select(pa_2016.player_api_id).distinct().count()


# In[26]:


pa_striker_2016=pa_2016.groupBy('player_api_id').agg({
    'finishing':'avg',
    'shot_power':'avg',
    'acceleration':'avg'
})


# In[27]:


pa_striker_2016.count()


# In[28]:


pa_striker_2016.show(10)


# In[29]:


pa_striker_2016.collect()


# In[30]:


pa_striker_2016=pa_striker_2016.withColumnRenamed("avg(finishing)","finishing")                               .withColumnRenamed("avg(shot_power)","shot_power")                               .withColumnRenamed("avg(acceleration)","acceleration")


# In[31]:


pa_striker_2016.show(10)


# In[32]:


weight_finishing=1
weight_shot_power=2
weight_acceleration=1

total_weight=weight_finishing+weight_shot_power+weight_acceleration


# In[33]:


strikers=pa_striker_2016.withColumn("stiker_grade",(pa_striker_2016.finishing* weight_finishing+                                                    pa_striker_2016.shot_power* weight_shot_power+                                                     pa_striker_2016.acceleration* weight_acceleration
                                                   )/total_weight)


# In[34]:


strikers=strikers.drop('finishing','acceleration','shot_power')
strikers.columns


# In[35]:


strikers=strikers.filter(strikers.stiker_grade > 70 )                 .sort(strikers.stiker_grade.desc())

strikers.show()


# In[36]:


strikers.count(),players.count()


# In[37]:


striker_details=players.join(strikers,players.player_api_id==strikers.player_api_id)


# In[38]:


striker_details.columns


# In[39]:


striker_details.count()


# In[40]:


striker_details=players.join(strikers,['player_api_id'])


# In[41]:


striker_details.show(5)


# In[42]:


from pyspark.sql.functions import broadcast


# In[46]:


striker_details=players.select("player_api_id","player_name").join(broadcast(strikers),['player_api_id'],'inner')


# In[48]:


striker_details=striker_details.sort(striker_details.stiker_grade.desc())


# In[49]:


striker_details.show(5)


# In[60]:


players.count(),players_attributes.count()


# In[63]:


players_heading_acc=players_attributes.select('player_api_id','heading_accuracy')                                     .join(broadcast(players),players_attributes.player_api_id == players.player_api_id)


# In[64]:


players_heading_acc.columns


# In[65]:


short_count=spark.sparkContext.accumulator(0)
medium_low_count=spark.sparkContext.accumulator(0)
medium_high_count=spark.sparkContext.accumulator(0)
tail_count=spark.sparkContext.accumulator(0)


# In[66]:


def count_players_by_height(row) :
    height = float (row.height)
    
    if (height <= 175):
        short_count.add(1)
    elif(height <=183 and height > 175):
        medium_low_count.add(1)
    elif(height <=195 and height > 183):
        medium_high_count.add(1)
    elif(height > 195):
        tail_count.add(1)


# In[68]:


players_heading_acc.foreach( lambda x : count_players_by_height(x))


# In[70]:


all_players=[short_count,medium_low_count,medium_high_count,tail_count]
all_players


# In[71]:


pa_2016.show(2)


# In[72]:


pa_2016.select("player_api_id","overall_rating")       .coalesce(1)\#creates single file  from all partitions
       .write       .option("header","true")       .csv("/home/cnk45874/output/player_overall.csv")


# In[73]:


pa_2016.select("player_api_id","overall_rating")       .write       .json("/home/cnk45874/output/player_overall.json")


# In[ ]:




