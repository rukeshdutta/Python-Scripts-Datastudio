from odps import ODPS
import pandas as pd
from odps import options
from odps.df import DataFrame
import csv
import arrow
import numpy as np
# import seaborn as sns
# import matplotlib.pyplot as plt
lineList = [line.rstrip('\n') for line in open('Credentials.txt')]
AccessId=str(lineList[0])
AccessKey=str(lineList[1])
Role=str(lineList[2])

o = ODPS(AccessId,AccessKey,Role, endpoint='http://service-all.ali-sg-lazada.odps.aliyun-inc.com/api')
options.tunnel.endpoint = 'http://dt-all.ali-sg-lazada.odps.aliyun-inc.com'
query="""SELECT  T.venture_category1_name_en
        ,T.venture_category2_name_en
        ,T.venture_category3_name_en
        ,TO_CHAR(T.fulfillment_create_date,'mm') AS date_time
        ,SUM(net_order) AS orders
        ,SUM(net_item) AS Net_Items
        ,SUM(nmv * exchange_rate_current) AS NMV
        ,SUM((T.gmv)*(T.exchange_rate_current)) AS GMV
FROM    daraz_cdm.dwd_drz_trd_core_df_bd T
WHERE   T.ds = TO_CHAR(DATEADD(GETDATE(), - 1, 'dd'), 'yyyymmdd')
AND     TO_CHAR(T.fulfillment_create_date,'yyyymmdd') BETWEEN TO_CHAR(DATEADD(GETDATE(), - 120, 'dd'), 'yyyymmdd')
AND     TO_CHAR(DATEADD(GETDATE(), - 1, 'dd'), 'yyyymmdd')
AND     T.payment_method IS NOT NULL
AND     T.venture_category1_name_en IS NOT NULL
AND     T.venture_category1_name_en NOT LIKE '%Test%'
AND     T.is_fulfilled = 1
GROUP BY T.venture_category1_name_en
         ,T.venture_category2_name_en
         ,T.venture_category3_name_en
         ,TO_CHAR(T.fulfillment_create_date,'mm')"""
sql=o.execute_sql(query).open_reader()
Filename='Target'
df=pd.DataFrame.from_records(sql)
#Write
df.to_csv(Filename+'.csv',index=False)
# Read
df=pd.read_csv(Filename+'.csv')
#drop first duplicate row
df.drop(df.index[:1], inplace=True)
column_name=['venture_category1_name_en',
 'venture_category2_name_en',
 'venture_category3_name_en',
 'date_time',
 'orders',
 'net_items',
 'nmv',
 'gmv']
num_of_col=len(column_name)
df.columns=column_name
#remove common parts
remove_first_brace="("
remove_end_brace=")"
remove_first_quotes="'"
remove_last_quotes="'"
remove_comma=","
for i in range(0,num_of_col):
    df_1=df.iloc[:,i].astype(str).str.replace(remove_first_brace,"")
    df_1=df_1.str.replace(str(column_name[i]),"")
    df_1=df_1.str.replace(remove_end_brace,"")
    df_1=df_1.str.replace(remove_first_quotes,"")
    df_1=df_1.str.replace(remove_last_quotes,"")
    df_1=df_1.str.replace(remove_comma,"")
    df_1=df_1.str.lstrip()
    df.iloc[:,i]=df_1
df.to_csv(Filename+'.csv',index=False)
df=pd.read_csv('Target.csv')
df=df.rename(columns={'date_time':'month'})
monthmap={1:'Jan',2:'Feb',3:'Mar',4:'Apr',5:'May',6:'Jun',7:'Jul',8:'Aug',9:'Sep',10:'Oct',11:'Nov',12:'Dec'}
df['month']=df['month'].map(monthmap)
cat1=pd.pivot_table(df,values=['gmv','nmv','orders','net_items'],index=['venture_category1_name_en','month'],\
                    aggfunc=np.sum,fill_value=0).unstack()
cat1=cat1.fillna(0)
total1=cat1.stack().sum(level='venture_category1_name_en')
cat1=cat1.join(total1,how='inner')
cat2=pd.pivot_table(df,values=['gmv','nmv','orders','net_items'],\
                    index=['venture_category1_name_en','venture_category2_name_en','month'],aggfunc=np.sum,).unstack()
cat2=cat2.fillna(0)
total2=cat2.stack().sum(level='venture_category2_name_en')
cat2=cat2.join(total2,how='inner')
cat3=pd.pivot_table(df,values=['gmv','nmv','orders','net_items'],\
                    index=['venture_category1_name_en','venture_category2_name_en','venture_category3_name_en','month'],\
                    aggfunc=np.sum).unstack()
cat3=cat3.fillna(0)
total3=cat3.stack().sum(level='venture_category3_name_en')
cat3=cat3.join(total3,how='inner')
for cat in (cat1,cat2,cat3):
    cat['gmv_catmix']=cat['gmv']/cat['gmv'].sum()
    cat['nmv_catmix']=cat['nmv']/cat['nmv'].sum()
    cat['g2n']=cat['nmv']/cat['gmv']
    cat['nis_catmix']=cat['net_items']/cat['net_items'].sum()
    cat['aiv']=cat['nmv']/cat['net_items']
    cat['aov']=cat['nmv']/cat['orders']
    cat['gmv_catmix']=cat['gmv_catmix'].map('{:.2%}'.format)
    cat['nmv_catmix']=cat['nmv_catmix'].map('{:.2%}'.format)
    cat['g2n']=cat['g2n'].map('{:.2%}'.format)
    cat['nis_catmix']=cat['nis_catmix'].map('{:.2%}'.format)
    cat['aiv']=cat['aiv'].map('{:,.2f}'.format)
    cat['aov']=cat['aov'].map('{:,.2f}'.format)
    cat['gmv']=cat['gmv'].map('{:,.2f}'.format)
    cat['nmv']=cat['nmv'].map('{:,.2f}'.format)
    cat.iloc[:,:20]=cat.iloc[:,:20].applymap('{:,.2f}'.format)
# function
def dfs_tabs(df_list, sheet_list, file_name):
    writer = pd.ExcelWriter(file_name,engine='xlsxwriter')   
    for dataframe, sheet in zip(df_list, sheet_list):
        dataframe.to_excel(writer, sheet_name=sheet, startrow=0 , startcol=0)   
    writer.save()

# list of dataframes and sheet names
dfs = [cat1, cat2, cat3]
sheets = ['cat1','cat2','cat3']    
daterun= arrow.now().format('YYYYMMDD')+'_'
# run function
dfs_tabs(dfs, sheets,str(daterun)+'TargetFile.xlsx')