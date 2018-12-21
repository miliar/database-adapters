#!/usr/bin/env python
# coding: utf-8

# In[1]:


import mysql.connector
from mysql_config import *


# In[2]:


cnx = mysql.connector.connect(user=USER, password=PASSWORD, database=DATABASE)
cursor = cnx.cursor()


# In[4]:


query = ('select * from watchlists limit 1')


# In[5]:


cursor.execute(query)


# In[6]:


print(dict(zip(cursor.column_names, cursor.fetchone())))


# In[7]:


cursor.close()
cnx.close()

