#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.accumulators import AccumulatorParam


# In[12]:


class VectorAccumulatorParam(AccumulatorParam):
    
    def zero(self,value):
        return [0.0]*len(value)
    
    def addInPlace(self,v1,v2):
        for i in range(len(v1)):
            v1[i]*=v2[i]
        return v1            


# In[13]:


vector_accum = sc.accumulator([10.0,20.0,30.0],VectorAccumulatorParam())
vector_accum.value


# In[14]:


vector_accum += [1,2,3]
vector_accum.value


# In[22]:


valuesA =[('John',100000),('James',150000),('Emily',650000),('Nina',200000)]
tableA =spark.createDataFrame(valuesA , ['name','salary'])


# In[23]:


tableA.show()


# In[26]:


valuesB =[('Darth Vader',5),('James',2),('Emily',3),('Princess Leia',6)]
tableB =spark.createDataFrame(valuesB , ['name','employee_id'])


# In[27]:


tableB.show()


# In[28]:


inner_join = tableA.join(tableB , tableA.name == tableB.name)
inner_join.show()


# In[29]:


right_join = tableA.join(tableB , tableA.name == tableB.name, how= 'right')
right_join.show()


# In[30]:


full_join = tableA.join(tableB , tableA.name == tableB.name, how= 'full')
full_join.show()


# In[ ]:





# In[ ]:




