from odps import ODPS
import pandas as pd
from odps import options
from odps.df import DataFrame
import csv
import arrow

lineList = [line.rstrip('\n') for line in open('Credentials.txt')]
AccessId=str(lineList[0])
AccessKey=str(lineList[1])
Role=str(lineList[2])

o = ODPS(AccessId,AccessKey,Role, endpoint='http://service-all.ali-sg-lazada.odps.aliyun-inc.com/api')
options.tunnel.endpoint = 'http://dt-all.ali-sg-lazada.odps.aliyun-inc.com'

query="""SELECT  daraz_sku
        ,product_name
        ,venture_category1_name_en
        ,venture_category2_name_en
        ,venture_category3_name_en
        ,promotion_amount
        ,unit_price
        ,paid_price
        ,list_price
        ,voucher_discount_amount
        ,discount_amount_by_platform
        ,collectible_discount_amount_seller
        ,collectible_discount_amount_platform
        ,cart_rule_discount_amount
        ,sales_order_id
        ,bundle_discount_amount
        ,shop_account_name
        ,is_fulfilled
        ,voucher_code
FROM    daraz_cdm.dwd_drz_trd_core_hh_bd
WHERE   TO_CHAR(order_create_date,'yyyymmdd') = TO_CHAR(DATEADD(GETDATE(), 0, 'dd'), 'yyyymmdd')
AND     paid_price / unit_price < 0.4
AND     venture_category3_name_en NOT IN ('Daraz Packaging Material')
AND voucher_code NOT LIKE ('REF%')
AND     b2b = 0
GROUP BY daraz_sku
         ,product_name
         ,venture_category1_name_en
         ,venture_category2_name_en
         ,venture_category3_name_en
         ,promotion_amount
         ,unit_price
         ,paid_price
         ,list_price
         ,voucher_discount_amount
         ,cart_rule_discount_amount
         ,sales_order_id
         ,bundle_discount_amount
         ,shop_account_name
         ,is_fulfilled
         ,discount_amount_by_platform
         ,collectible_discount_amount_seller
         ,collectible_discount_amount_platform
         ,voucher_code
;
"""
sql=o.execute_sql(query).open_reader()
Filename='Alert'
df=pd.DataFrame.from_records(sql)
#Write
df.to_csv(Filename+'.csv',index=False)
# Read
df=pd.read_csv(Filename+'.csv')
#drop first duplicate row
df.drop(df.index[:1], inplace=True)
column_name=['daraz_sku','product_name','venture_category1_name_en','venture_category2_name_en','venture_category3_name_en'\
             ,'promotion_amount','unit_price','paid_price','list_price','voucher_discount_amount',\
             'discount_amount_by_platform','collectible_discount_amount_seller','collectible_discount_amount_platform'\
             ,'cart_rule_discount_amount','sales_order_id','bundle_discount_amount','shop_account_name','is_fulfilled'\
             ,'voucher_code']
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

date_string= arrow.now().format('YYYYMMDD')+'_'
writer = pd.ExcelWriter(str(date_string)+Filename+'.xlsx')
df.to_excel(writer,'Output',index=False)
writer.save()

