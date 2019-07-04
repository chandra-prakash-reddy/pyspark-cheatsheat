#!/usr/bin/env python
# coding: utf-8

# In[1]:


sc


# In[2]:


from pyspark.sql.types import Row
from datetime import datetime


# In[4]:


simple_data=sc.parallelize([1,"Alice",50])
simple_data


# In[5]:


simple_data.count()


# In[6]:


simple_data.first()


# In[7]:


simple_data.take(2)


# In[8]:


simple_data.collect()


# In[11]:


records=sc.parallelize([[1,"Alice",50],[1,"Bob",50]])
records


# In[12]:


records.count()


# In[13]:


records.first()


# In[15]:


records.take(2)


# In[16]:


records.collect()


# In[17]:


df=records.toDF()


# In[18]:


df


# In[19]:


df.show()


# In[20]:


my_df=sc.parallelize(Row(id=1,name="chandra"))


# In[21]:


my_df


# In[26]:


my_df.collect()


# In[25]:


my_df=sc.parallelize([Row(id=1,name="chandra"),Row(id=2,name="prakash")])


# In[27]:


data_frame=my_df.toDF()


# In[28]:


data_frame.show()


# In[ ]:




