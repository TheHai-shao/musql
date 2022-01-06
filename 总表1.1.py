#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import openpyxl
import pymysql
import datetime,time
nows=datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')


print(datetime.date.today())
import numpy as np
# #数仓连接
con = pymysql.connect(host="139.198.189.25",port=44000,user="cnereader",passwd="read51096677",charset="utf8",autocommit=True,database='logisticscore')
cur = con.cursor()

# days="BETWEEN '2021-03-31 16:00:00' and '"+time_yes+"'"1
# days="BETWEEN '2021-03-31 16:00:00' and "+"'"+nows+"'"
days="BETWEEN '2021-11-28 16:00:00' and "+"'"+nows+"'"


end_day = datetime.date.today()
start_day = datetime.date.today()+ datetime.timedelta(-60)
print(end_day)
print(start_day)
print(days)
# days="BETWEEN '2020-12-17 16:00:00' and '2021-02-15 15:59:59'"

# list
s1="""SELECT 
channel_code "渠道名称",
des "目的地",
date_format(date_add(gmt_create,interval 8 hour),'%Y-%m-%d') as "业务日期" ,
 customer_id,
 round(timestampdiff(hour,lgo.gmt_create,lgo.delivery_date)/24,1) as delivery_interval,
 order_status "追踪状态" ,
 ISNULL( mawb_id ),
 standard_track_event_id id,
 count(1) c ,
 sum(weight) 总重,
 avg(weight) 单重
from lg_order lgo 
where gmt_create {}
and platform='WISH_ONLINE' 
AND customer_id in (1151368,1151370,1181372,1181374) 
and is_deleted='n'   
group by
1,2,3,4,5,6,7,8
order by c desc
""".format(days)

s2="""SELECT 
channel_code "渠道名称",
des "目的地",
date_format(date_add(gmt_create,interval 8 hour),'%Y-%m-%d') as "业务日期" ,
 customer_id,
 round(timestampdiff(hour,lgo.gmt_create,lgo.delivery_date)/24,1) as delivery_interval,
 order_status "追踪状态" ,
 standard_track_event_id id,
 ISNULL( mawb_id ),
 count(1) c ,
 sum(weight) 总重,
 avg(weight) 单重
from lg_order lgo 
where gmt_create {}
and lgo.customer_id='3282094'
and is_deleted='n'   
group by
1,2,3,4,5,6,7,8
order by c desc
""".format(days)

#敦煌
s3="""SELECT 
channel_code "渠道名称",
des "目的地",
date_format(date_add(gmt_create,interval 8 hour),'%Y-%m-%d') as "业务日期" ,
 customer_id,
 round(timestampdiff(hour,lgo.gmt_create,lgo.delivery_date)/24,1) as delivery_interval,
 order_status "追踪状态" ,
 ISNULL( mawb_id ),
 standard_track_event_id id,
 count(1) c ,
 sum(weight) 总重,
 avg(weight) 单重
from lg_order lgo 
where gmt_create {}
AND lgo.platform="DHLINK"   
AND lgo.channel_code IN ("CNE经济专线DH","CNE特惠专线DH","CNE优先专线DH")
and is_deleted='n'   
group by
1,2,3,4,5,6,7,8
order by c desc
""".format(days)


#兰亭
s4="""SELECT 
channel_code "渠道名称",
des "目的地",
date_format(date_add(gmt_create,interval 8 hour),'%Y-%m-%d') as "业务日期" ,
 customer_id,
 round(timestampdiff(hour,lgo.gmt_create,lgo.delivery_date)/24,1) as delivery_interval,
 order_status "追踪状态" ,
 ISNULL( mawb_id ),
 standard_track_event_id id,
 count(1) c ,
 sum(weight) 总重,
 avg(weight) 单重
from lg_order lgo 
where gmt_create {}
and customer_id='3161297'
and is_deleted='n'   
group by
1,2,3,4,5,6,7,8
order by c desc
""".format(days)



def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns=[column[i][0] for i in range(len(column))]
    df=pd.DataFrame([list(i)for i in data],columns=columns)
    return df
# 主表
d1=execude_sql(s1)

d2=execude_sql(s2)
d3=execude_sql(s3)
d4=execude_sql(s4)


# print(d1['业务日期'].dtypes)

d1['项目名称'] = 'WISH'
d2['项目名称'] = '促佳'
d3['项目名称'] = '敦煌'
d4['项目名称'] = '兰亭'


df1 = pd.concat([d1,d2,d3,d4])

df1['当日']=datetime.date.today()
df1['业务日期']=pd.to_datetime(df1['业务日期'])
# print()
df1['距离当天']=(pd.to_datetime(df1['当日'])- df1['业务日期']).dt.days

# 追踪状态 的中文解释
o_s={0:'未发送',1 :'已发送',2 :'转运中', 3:'送达',4:'超时',5:'扣关',6:'地址错误',7:'快件丢失', 8:'退件',9:'其他异常'}
df1['状态']=df1['追踪状态'].map(o_s)
df1['状态']=df1['状态'].fillna('销毁')

# 间隔日
# d1.info()
df1['间隔日']=pd.cut(df1['距离当天'],[-999,-2,3, 5, 7, 12, 20, 30, 100000],
                 labels=['已发送','03天未发出', '05天未发出', '07天未发出', '12天未发出', '20天未发出','30天未发出','30天以上未发出',
                         ])
df1.loc[df1['状态']!='未发送',['间隔日']]='已发送'
# 淡旺季
# df1['淡旺季']='20210'#先默认淡季
# print(d1['业务日期'].dt.month)
# print(d1.loc[1,'业务日期'].year)
# for i in df1.index:
#     if df1.loc[i,'业务日期'].year==2020:
#         df1.loc[i,'淡旺季']='20201'
#     elif df1.loc[i,'业务日期'].month in [1,9,10,11,12]:
#         df1.loc[i, '淡旺季'] = '20211'
# print(d1)



s5 = """
select
id,
zh_name
from
standard_track_event
where 
is_deleted='n'
"""
d5=execude_sql(s5)

df2 = pd.merge(df1,d5,on=["id"],how='left')

name_l=[['总list',df2]]

print(name_l)

def file_xlsx(name,df):
    bf =r'F:\PBI临时文件\wish总表监控\{}.xlsx'.format(name)
    writer = pd.ExcelWriter(bf)
    df.to_excel(writer,'sheet1',index=False)
    writer.save()
for n in name_l:
    file_xlsx(n[0],n[1])
