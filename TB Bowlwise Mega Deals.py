from odps import ODPS
import pandas as pd
from odps import options
from odps.df import DataFrame
import csv
import arrow
import numpy as np
lineList = [line.rstrip('\n') for line in open('Credentials.txt')]
AccessId=str(lineList[0])
AccessKey=str(lineList[1])
Role=str(lineList[2])

o = ODPS(AccessId,AccessKey,Role, endpoint='http://service-all.ali-sg-lazada.odps.aliyun-inc.com/api')
options.tunnel.endpoint = 'http://dt-all.ali-sg-lazada.odps.aliyun-inc.com'

query="""SELECT  CASE    WHEN TB.child_campaign_name IS NULL THEN 'NOT IN BOWL' 
                ELSE TB.child_campaign_name 
        END AS child_campaign_name
        ,CASE    WHEN TB.campaign_price IS NULL THEN 'NOT IN BOWL' 
                 ELSE TB.campaign_price 
         END AS campaign_price
        ,T1.venture_category1_name_en
        ,T1.venture_category2_name_en
        ,T1.venture_category3_name_en
        ,T1.venture_category4_name_en
        ,T1.daraz_sku
        ,T1.product_id
        ,T1.product_name
        ,T1.business_type
        ,T1.Orders
        ,T1.Items_Sold
        ,T1.NMV
        ,T1.GMV
        ,T1.Cart
        ,T1.Coupon
        ,T1.pageview
        ,T1.cr
FROM    (
            SELECT  T.venture_category1_name_en
                    ,T.venture_category2_name_en
                    ,T.venture_category3_name_en
                    ,T.venture_category4_name_en
                    ,T.daraz_sku
                    ,T.product_id
                    ,T.product_name
                    ,T.business_type
                    ,ROUND(SUM(net_order),0) AS Orders
                    ,SUM(net_item) AS Items_Sold
                    ,ROUND((SUM((T.nmv)*(T.exchange_rate_current))),2) AS NMV
                    ,ROUND((SUM((T.gmv)*(T.exchange_rate_current))),2) AS GMV
                    ,ROUND((SUM((T.cart_rule_discount_amount)*(T.exchange_rate_current))),2) AS Cart
                    ,ROUND((SUM((T.voucher_discount_amount)*(T.exchange_rate_current))),2) AS Coupon
                    ,page.pageview
                    ,SUM(net_item)/page.pageview AS cr
            FROM    daraz_cdm.dwd_drz_trd_core_df_bd AS T INNER
            JOIN    (
                        SELECT  PA.sku
                                ,SUM(PA.ipv_1d_001) AS pageview
                        FROM    daraz_cdm.dws_drz_log_sku_pv_uv_all_1d PA
                        WHERE   PA.ds >= TO_CHAR(DATEADD(GETDATE(), - 7, 'dd'), 'yyyymmdd')
                        GROUP BY PA.sku
                    ) AS page
            ON      T.daraz_sku = page.sku
            INNER JOIN (
                           SELECT  P.daraz_sku
                           FROM    daraz_cdm.dim_drz_prd_sku_core_bd P
                           WHERE   P.is_visible = 1
                           AND     P.is_purchasable = 1
                           AND     stock_available > 0
                       ) AS prod
            ON      T.daraz_sku = prod.daraz_sku
            WHERE   T.ds = TO_CHAR(DATEADD(GETDATE(), - 1, 'dd'), 'yyyymmdd')
            AND     TO_CHAR(T.fulfillment_create_date,'yyyymmdd') >= TO_CHAR(DATEADD(GETDATE(), - 60, 'dd'), 'yyyymmdd')
            AND     T.is_fulfilled = 1
            AND     T.b2b = 0
            GROUP BY T.venture_category1_name_en
                     ,T.venture_category2_name_en
                     ,T.venture_category3_name_en
                     ,T.venture_category4_name_en
                     ,T.daraz_sku
                     ,T.product_id
                     ,T.product_name
                     ,T.business_type
                     ,page.pageview
            HAVING  Items_Sold > 1
            AND     Orders > 1
        ) AS T1 LEFT
JOIN    (
            SELECT  simple_sku
                    ,child_campaign_id
                    ,child_campaign_name
                    ,(campaign_promotion_price/100) AS campaign_price
            FROM    daraz_cdm.dim_drz_pro_treasurebowl_sku_bd
            WHERE   master_campaign_id = 10215
            AND     campaign_sku_status = 2
            GROUP BY simple_sku
                     ,child_campaign_id
                     ,child_campaign_name
                     ,campaign_promotion_price
        ) AS TB
ON      T1.daraz_sku = TB.simple_sku
GROUP BY TB.child_campaign_name
         ,TB.campaign_price
         ,T1.venture_category1_name_en
         ,T1.venture_category2_name_en
         ,T1.venture_category3_name_en
         ,T1.venture_category4_name_en
         ,T1.daraz_sku
         ,T1.product_id
         ,T1.product_name
         ,T1.business_type
         ,T1.Orders
         ,T1.Items_Sold
         ,T1.NMV
         ,T1.GMV
         ,T1.Cart
         ,T1.Coupon
         ,T1.pageview
         ,T1.cr
;"""
sql=o.execute_sql(query).open_reader()
Filename='MegaL3_special'
df=pd.DataFrame.from_records(sql)
#Write
df.to_csv(Filename+'.csv',index=False)
# Read
df=pd.read_csv(Filename+'.csv')
#drop first duplicate row
df.drop(df.index[:1], inplace=True)
column_name=['child_campaign_name','campaign_price','venture_category1_name_en','venture_category2_name_en','venture_category3_name_en',\
             'venture_category4_name_en','daraz_sku','product_id','product_name','business_type','orders','items_sold','nmv','gmv','cart','coupon',\
             'pageview','cr']
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
Mega=pd.read_csv(Filename+'.csv')
Mega['AIV']=Mega['nmv']/Mega['items_sold']
Mega['AOV']=Mega['nmv']/Mega['orders']
Mega['G2N']=Mega['nmv']/Mega['gmv']
Mega=Mega.fillna(0)

#drop any cat 
Mega=Mega.drop(Mega[(Mega['venture_category1_name_en']=='Groceries')\
|(Mega['venture_category1_name_en']=='Digital Goods')|(Mega['venture_category1_name_en']=='Stationery & Craft')|(Mega['venture_category1_name_en']=='Motors')].index)

STD = {}
MEAN = {}
WEIGHTS = {'orders_weight':.25,'items_sold_weight':.15,'gmv_weight':.05,'nmv_weight':.25,'AIV_weight':.02,\
           'AOV_weight':.03,'cr_weight':.05,'G2N_weight':.05,'cart_weight':.05,'coupon_weight':.05,\
          'pageview_weight':.05}
for columns in Mega.loc[:,'orders':'G2N']:
        STD[format(columns) + "_std"] =  Mega[columns].std()
        MEAN[format(columns) + "_mean"] =  Mega[columns].mean()
        Mega[format(columns) + "_Score"] = ((Mega[columns] - MEAN[format(columns) + "_mean"])/STD[format(columns) + "_std"] 
                                            * WEIGHTS[format(columns) + "_weight"])
Mega['Score'] = np.sum(Mega.loc[:,'orders_Score':'G2N_Score'], axis=1)
Mega = Mega.sort_values(['Score'], ascending = False)

Mega=Mega.fillna(0)
date_string= arrow.now().format('YYYYMMDD')+'_'
writer = pd.ExcelWriter(str(date_string)+Filename+'.xlsx')
Mega.to_excel(writer,'Output',index=False)
writer.save()