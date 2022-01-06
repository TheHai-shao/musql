#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import openpyxl
import pymysql
import datetime,time


# In[2]:


con = pymysql.connect(host="139.198.189.25",port=44000,user="cnereader",passwd="read51096677",charset="utf8",autocommit=True,database='logisticscore')
cur = con.cursor()


# In[3]:


def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns=[column[i][0] for i in range(len(column))]
    df=pd.DataFrame([list(i)for i in data],columns=columns)
    return df


# In[4]:


S1="""
SELECT pre_order_id,channel_id,des,standard_event_code_list
FROM platform_track_match_monitor 
where 
platform='WISH_ONLINE'
AND order_time between '2021-12-05 16:00:00' and '2021-12-06 16:00:00'
and is_deleted='n'
"""


# In[5]:


d1=execude_sql(S1)


# In[6]:


S2="""
SELECT pre_order_id,channel_id,des,standard_event_code_list
FROM platform_track_match_monitor 
where 
platform='WISH_ONLINE'
AND order_time between '2021-12-06 16:00:00' and '2021-12-07 16:00:00'
and is_deleted='n'
"""


# In[7]:


d2=execude_sql(S2)


# In[8]:


S3="""
SELECT pre_order_id,channel_id,des,standard_event_code_list
FROM platform_track_match_monitor 
where 
platform='WISH_ONLINE'
AND order_time between '2021-12-07 16:00:00' and '2021-12-08 16:00:00'
and is_deleted='n'
"""


# In[9]:


d3=execude_sql(S3)


# In[10]:


S4="""
SELECT pre_order_id,channel_id,des,standard_event_code_list
FROM platform_track_match_monitor 
where 
platform='WISH_ONLINE'
AND order_time between '2021-12-08 16:00:00' and '2021-12-09 16:00:00'
and is_deleted='n'
"""


# In[11]:


d4=execude_sql(S4)


# In[12]:


S5="""
SELECT pre_order_id,channel_id,des,standard_event_code_list
FROM platform_track_match_monitor 
where 
platform='WISH_ONLINE'
AND order_time between '2021-12-09 16:00:00' and '2021-12-10 16:00:00'
and is_deleted='n'
"""


# In[13]:


d5=execude_sql(S5)


# In[14]:


S6="""
SELECT pre_order_id,channel_id,des,standard_event_code_list
FROM platform_track_match_monitor 
where 
platform='WISH_ONLINE'
AND order_time between '2021-12-10 16:00:00' and '2021-12-11 16:00:00'
and is_deleted='n'
"""


# In[15]:


d6=execude_sql(S6)


# In[16]:


S7="""
SELECT pre_order_id,channel_id,des,standard_event_code_list
FROM platform_track_match_monitor 
where 
platform='WISH_ONLINE'
AND order_time between '2021-12-11 16:00:00' and '2021-12-12 16:00:00'
and is_deleted='n'
"""


# In[17]:


d7=execude_sql(S7)


# In[19]:


d1.to_csv(r'C:\Users\hp\Desktop\标准事件推送备份\125-1212\事件推送\2122.csv',index=False)


# In[20]:


d2.to_csv(r'C:\Users\hp\Desktop\标准事件推送备份\125-1212\事件推送\2223.csv',index=False)


# In[21]:


d3.to_csv(r'C:\Users\hp\Desktop\标准事件推送备份\125-1212\事件推送\2324.csv',index=False)


# In[22]:


d4.to_csv(r'C:\Users\hp\Desktop\标准事件推送备份\125-1212\事件推送\2425.csv',index=False)


# In[23]:


d5.to_csv(r'C:\Users\hp\Desktop\标准事件推送备份\125-1212\事件推送\2526.csv',index=False)


# In[24]:


d6.to_csv(r'C:\Users\hp\Desktop\标准事件推送备份\125-1212\事件推送\2627.csv',index=False)


# In[25]:


d7.to_csv(r'C:\Users\hp\Desktop\标准事件推送备份\125-1212\事件推送\2728.csv',index=False)


# In[ ]:




