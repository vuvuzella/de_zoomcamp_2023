#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd


# In[3]:


pd.__version__


# In[12]:


df = pd.read_csv("../yellow_tripdata_2021-01.csv", nrows=100)


# In[13]:


# print DDL to create the table with the schema based on the csv data
print(pd.io.sql.get_schema(df, "yellow_trip_data"))


# In[23]:


df


# In[24]:


# display only n rows:
df.head(n=5) # n=0 displays only the header


# In[15]:


# convert timestamp fields (tpep_pickup_datetime and tpep_dropoff_datetime) into datetime fields from test
df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


# In[16]:


print(pd.io.sql.get_schema(df, "yellow_trip_data"))


# In[17]:


# Create a POSTGRES specific DDL:
# Create connection with POSTGRES
from sqlalchemy import create_engine


# In[19]:


# https://docs.sqlalchemy.org/en/20/core/engines.html
engine = create_engine("postgresql://de_zoomcamp:de_zoomcamp@localhost:5433/de_zoomcamp")


# In[21]:


# POSTGRES-specific DDL for the CSV
print(pd.io.sql.get_schema(df, "yellow_trip_data", con=engine))


# In[29]:


# chunkify the csv, because reading the whole data into memory is costly
df_iter = pd.read_csv("../yellow_tripdata_2021-01.csv", iterator=True, chunksize=100000)


# In[30]:


# We want to separate the creation of the table and the insertion of the rows
# To do this, we first get the first chunk and get the first row,
# generate the DDL SQL to create the table and execute it to propagate
# the changes to the database
header_chunk = next(df_iter)
header_chunk.head(n=0)


# In[31]:


# Create the table using the first row data
header_chunk.head(n=0).to_sql(name="yellow_taxi_data", con=engine, if_exists="replace")


# In[35]:


from time import time


# In[36]:


# insert data
df_iter = pd.read_csv("../yellow_tripdata_2021-01.csv", iterator=True, chunksize=100000) # reset iterator
for chunk in df_iter:
    start_time = time()
    
    # convert timestamp to proper timestamp format
    chunk.tpep_pickup_datetime = pd.to_datetime(chunk.tpep_pickup_datetime)
    chunk.tpep_dropoff_datetime = pd.to_datetime(chunk.tpep_dropoff_datetime)
    
    chunk.to_sql(name="yellow_taxi_data", con=engine, if_exists="append")
    
    end_time = time()
    
    print(f"inserted chunk, took {(end_time - start_time):.3f} seconds")


# In[ ]:




