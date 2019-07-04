#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession


# In[2]:


spark=SparkSession.builder                  .appName("analyzing airline data")                  .getOrCreate()


# In[3]:


from pyspark.sql.types import Row
from datetime import datetime


# In[5]:


record = sc.parallelize([Row(id=1,
                             name="Jill",
                             active=True,
                             clubs=['chees','hockey'],
                             subjects={"math":80,"english":56},
                             enrolled=datetime(2014,8,1,14,1,5)),
                         Row(id=2,
                             name="George",
                             active=False,
                             clubs=['chees','soccor'],
                             subjects={"math":60,"english":96},
                             enrolled=datetime(2015,3,21,8,2,5))
                        ])


# In[6]:


record_df=record.toDF()
record_df.show()


# In[7]:


record_df.createOrReplaceTempView("records")


# In[8]:


all_records_df=sqlContext.sql("SELECT * FROM records")
all_records_df.show()


# In[9]:


sqlContext.sql("select id,clubs[1],subjects['english'] from records").show()


# In[10]:


sqlContext.sql("SELECT id, NOT active from records").show()


# In[11]:


sqlContext.sql("SELECT *  from records where active").show()


# In[12]:


sqlContext.sql('SELECT *  from records where subjects["english"] > 90 ').show()


# In[13]:


record_df.createGlobalTempView("global_records")


# In[16]:


sqlContext.sql("SELECT * FROM global_temp.global_records").show()


# In[3]:


airlinesPath="/home/cnk45874/demo/airlines.csv"
flightsPath="/home/cnk45874/demo/flights.csv"
airportsPath="/home/cnk45874/demo/airports.csv"


# In[4]:


airlines = spark.read                .format("csv")                .option("header","true")                .load(airlinesPath)


# In[5]:


airlines.createOrReplaceTempView("airlines")


# In[6]:


sqlContext.sql("SELECT * FROM airlines").show()


# In[7]:


flights = spark.read               .format("csv")               .option("header","true")               .load(flightsPath)


# In[8]:


flights.createOrReplaceTempView("flights")
flights.columns


# In[9]:


flights.show(5)


# In[10]:


flights.count(),airlines.count()


# In[11]:


flights_count=spark.sql("SELECT COUNT(*) FROM flights")
airlines_count=spark.sql("SELECT COUNT(*) FROM airlines")


# In[12]:


flights_count,airlines_count


# In[13]:


flights_count.collect()[0][0],airlines_count.collect()[0][0]


# In[14]:


total_distance_df=spark.sql("SELECT distance FROM flights")                       .agg({"distance":"sum"})                       .withColumnRenamed("sum(distance)","total_distance")


# In[15]:


total_distance_df.show()


# In[16]:


all_delays_2012 = spark.sql( "SELECT date , airlines , flight_number ,departure_delay FROM flights where departure_delay > 0 and year(date) =2012")


# In[17]:


all_delays_2012.show()


# In[18]:


all_delays_2014 = spark.sql( "SELECT date , airlines , flight_number ,departure_delay FROM flights where departure_delay > 0 and year(date) =2014")


# In[19]:


all_delays_2014.show(5)


# In[20]:


all_delays_2014.createOrReplaceTempView("all_delays")


# In[21]:


all_delays_2014.orderBy(all_delays_2014.departure_delay.desc()).show(5)


# In[22]:


delay_count = spark.sql("SELECT COUNT(departure_delay) from all_delays")


# In[23]:


delay_count.show()


# In[24]:


delay_count.collect()[0][0]


# In[25]:


delay_percent = (delay_count.collect()[0][0]/flights_count.collect()[0][0])*100
delay_percent


# In[26]:


delay_per_airline = spark.sql(" SELECT airlines, departure_delay FROM flights")                         .groupBy("airlines")                         .agg({"departure_delay":"avg"})                         .withColumnRenamed("avg(departure_delay)","departure_delay")


# In[27]:


delay_per_airline.orderBy(delay_per_airline.departure_delay.desc()).show(5)


# In[28]:


delay_per_airline.createOrReplaceTempView("delay_per_airline")


# In[29]:


delay_per_airline = spark.sql("SELECT * FROM delay_per_airline ORDER BY departure_delay DESC")


# In[30]:


delay_per_airline.show(5)


# In[31]:


delay_per_airline = spark.sql ( "SELECT * FROM delay_per_airline "+
                               "JOIN airlines ON delay_per_airline.airlines = airlines.code "+
                               "ORDER BY departure_delay DESC")


# In[32]:


delay_per_airline.show(5)


# In[33]:


products = spark.read                .format("csv")                .option("header","true")                .load("/home/cnk45874/demo/products.csv")


# In[34]:


products.show(5)


# In[36]:


import sys
from pyspark.sql.window import Window
import pyspark.sql.functions as func


# In[37]:


windowSpecl = Window.partitionBy(products['category'])                    .orderBy(products['price'].desc())


# In[40]:


price_rank = (func.rank().over(windowSpecl))


# In[41]:


product_rank = products.select ( 
    products['product'],
    products['category'],
    products['price']
).withColumn('rank',func.rank().over(windowSpecl))
product_rank.show()


# In[50]:


windowSpecl2 = Window.partitionBy(products['category'])                    .orderBy(products['price'].desc())                    .rowsBetween(-1,0)


# In[51]:


price_max = (func.max(products['price']).over(windowSpecl2))


# In[52]:


products.select ( 
   products['product'],
   products['category'],
   products['price'],
   price_max.alias("price_max")).show()


# In[47]:


windowSpecl3 = Window.partitionBy(products['category'])                    .orderBy(products['price'].desc())                    .rangeBetween(-sys.maxsize,sys.maxsize)


# In[49]:


price_difference = (func.max(products['price']).over(windowSpecl3) - products['price'])


# In[53]:


products.select ( 
    products['product'],
    products['category'],
    products['price'],
    price_difference.alias("price_difference")).show()


# In[ ]:




