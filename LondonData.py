#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession


# In[4]:


spark=SparkSession.builder                  .appName("Analyzing London crime data")                  .getOrCreate()


# In[5]:


data=spark.read          .format("csv")          .option("header","true")          .load("/home/cnk45874/demo/airlines.csv")


# In[6]:


data.printSchema()


# In[7]:


data.count()


# In[8]:


data.limit(5).show()


# In[9]:


distinct_desc=data.select("Description").distinct()


# In[10]:


distinct_desc.show()


# In[11]:


distinct_desc.count()


# In[ ]:




